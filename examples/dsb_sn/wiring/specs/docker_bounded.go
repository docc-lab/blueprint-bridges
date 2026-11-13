package specs

// Tomislav-RetCtx: snbounded variants: the real-work Social Network wiring of docker.go / docker_nt.go with the
// user and post caches on memcached, as upstream DeathStarBench deploys them (user-memcached, post-storage-memcached;
// social-graph, user-timeline and home-timeline caches stay on Redis, as upstream's), otherwise unchanged, backed by
// the `snbounded` workflow package (examples/dsb_sn/workflow/snbounded), which reimplements upstream DeathStarBench's
// handlers operation for operation (see the package comment in common.go). Pick with `-w docker_<kind>_es_bd`; the
// bridge kind is the prefix before `_es_bd`.

import (
	"fmt"
	"os"
	"strings"

	"github.com/blueprint-uservices/blueprint/blueprint/pkg/wiring"
	"github.com/blueprint-uservices/blueprint/examples/dsb_sn/workflow/snbounded"
	"github.com/blueprint-uservices/blueprint/plugins/clientpool"
	"github.com/blueprint-uservices/blueprint/plugins/cmdbuilder"
	"github.com/blueprint-uservices/blueprint/plugins/goproc"
	"github.com/blueprint-uservices/blueprint/plugins/gotests"
	"github.com/blueprint-uservices/blueprint/plugins/grpc"
	"github.com/blueprint-uservices/blueprint/plugins/http"
	"github.com/blueprint-uservices/blueprint/plugins/jaeger"
	"github.com/blueprint-uservices/blueprint/plugins/linuxcontainer"
	"github.com/blueprint-uservices/blueprint/plugins/memcached"
	"github.com/blueprint-uservices/blueprint/plugins/mongodb"
	"github.com/blueprint-uservices/blueprint/plugins/opentelemetry"
	"github.com/blueprint-uservices/blueprint/plugins/otelcol"
	"github.com/blueprint-uservices/blueprint/plugins/redis"
	"github.com/blueprint-uservices/blueprint/plugins/retries"
	"github.com/blueprint-uservices/blueprint/plugins/workflow"
)

var (
	DockerVESBD    = makeVariantBD("v_es_bd", vanillaConfig, true)
	DockerPBESBD   = makeVariantBD("pb_es_bd", bridgesConfig, true)
	DockerCGPBESBD = makeVariantBD("cgpb_es_bd", bridgesConfig, true)
	DockerSBESBD   = makeVariantBD("sb_es_bd", bridgesConfig, true)
	DockerNTESBD   = makeVariantNTBD("nt_es_bd", vanillaConfig, true)
)

func makeVariantBD(kind, configPath string, useES bool) cmdbuilder.SpecOption {
	return cmdbuilder.SpecOption{
		Name:        "docker_" + kind,
		Description: fmt.Sprintf("BOUNDED DSB SocialNetwork (snbounded), bridge kind %q, collector config %s", kind, configPath),
		Build: func(spec wiring.WiringSpec) ([]string, error) {
			bridgeKind := strings.TrimSuffix(strings.TrimSuffix(kind, "_bd"), "_es")
			if _, set := os.LookupEnv("BLUEPRINT_BRIDGE_KIND"); !set {
				os.Setenv("BLUEPRINT_BRIDGE_KIND", bridgeKind)
			}
			return makeDockerSpecBD(spec, kind+*extraSuffix, configPath, useES)
		},
	}
}

func makeVariantNTBD(kind, configPath string, useES bool) cmdbuilder.SpecOption {
	return cmdbuilder.SpecOption{
		Name:        "docker_" + kind,
		Description: fmt.Sprintf("NO-TRACING BOUNDED DSB SocialNetwork (snbounded, OTel SDK stripped), config %s", configPath),
		Build: func(spec wiring.WiringSpec) ([]string, error) {
			return makeDockerSpecNTBD(spec, kind+*extraSuffix, configPath, useES)
		},
	}
}

// makeDockerSpecBD: makeDockerSpec (docker.go) with snbounded services.
func makeDockerSpecBD(spec wiring.WiringSpec, suffix, configPath string, useES bool) ([]string, error) {
	sn := func(name string) string { return name + "_" + suffix }

	var containers []string
	var allServices []string

	var jaeger_collector string
	if useES {
		// Single-node ES container co-located with jaeger as storage
		// backend — jaeger gets SPAN_STORAGE_TYPE=elasticsearch +
		// ES_SERVER_URLS env vars wired automatically.
		jaeger_collector = jaeger.CollectorWithElasticsearch(spec, sn("jaeger"), sn("elasticsearch"))
	} else {
		jaeger_collector = jaeger.Collector(spec, sn("jaeger"))
	}
	trace_collector := otelcol.CollectorWithConfig(
		spec, sn("otelcol"),
		jaeger_collector,
		configPath,
		otelImage,
		8080, "jaeger")

	applyDockerDefaults := func(serviceName string) string {
		retries.AddRetries(spec, serviceName, 3)
		// Tomislav-RetCtx: gRPC services are traced with gRPC interceptors; build with
		// OT_GRPC_MODE=wrapper to get the old wrapper services instead.
		opentelemetry.InstrumentGRPC(spec, serviceName, trace_collector)
		grpc.Deploy(spec, serviceName)
		goproc.Deploy(spec, serviceName)
		return linuxcontainer.Deploy(spec, serviceName)
	}

	applyHTTPDefaults := func(serviceName string, collector string) string {
		retries.AddRetries(spec, serviceName, 3)
		clientpool.Create(spec, serviceName, 100)
		// Tomislav-RetCtx: HTTP services are traced with HTTP middleware; OT_GRPC_MODE=wrapper for the old wrappers.
		opentelemetry.InstrumentHTTP(spec, serviceName, collector)
		http.Deploy(spec, serviceName)
		goproc.Deploy(spec, serviceName)
		return linuxcontainer.Deploy(spec, serviceName)
	}

	// Backends
	user_cache := memcached.Container(spec, sn("user_cache")) // Tomislav-RetCtx: upstream user-memcached (DeathStarBench socialNetwork/docker-compose.yml)
	user_db := mongodb.Container(spec, sn("user_db"))
	post_cache := memcached.Container(spec, sn("post_cache")) // Tomislav-RetCtx: upstream post-storage-memcached (DeathStarBench socialNetwork/docker-compose.yml)
	post_db := mongodb.Container(spec, sn("post_db"))
	social_cache := redis.Container(spec, sn("social_cache"))
	social_db := mongodb.Container(spec, sn("social_db"))
	urlshorten_db := mongodb.Container(spec, sn("urlshorten_db"))
	usertimeline_cache := redis.Container(spec, sn("usertimeline_cache"))
	usertimeline_db := mongodb.Container(spec, sn("usertimeline_db"))
	hometimeline_cache := redis.Container(spec, sn("hometimeline_cache"))

	allServices = append(allServices,
		user_cache, user_db,
		post_cache, post_db,
		social_cache, social_db,
		usertimeline_cache, usertimeline_db,
		hometimeline_cache,
	)

	urlshorten_service := workflow.Service[snbounded.UrlShortenService](spec, sn("urlshorten_service"), urlshorten_db)
	containers = append(containers, applyDockerDefaults(urlshorten_service))
	allServices = append(allServices, urlshorten_service)

	usermention_service := workflow.Service[snbounded.UserMentionService](spec, sn("usermention_service"), user_cache, user_db)
	containers = append(containers, applyDockerDefaults(usermention_service))
	allServices = append(allServices, usermention_service)

	post_storage_service := workflow.Service[snbounded.PostStorageService](spec, sn("post_storage_service"), post_cache, post_db)
	containers = append(containers, applyDockerDefaults(post_storage_service))
	allServices = append(allServices, post_storage_service)

	media_service := workflow.Service[snbounded.MediaService](spec, sn("media_service"))
	containers = append(containers, applyDockerDefaults(media_service))
	allServices = append(allServices, media_service)

	uniqueId_service := workflow.Service[snbounded.UniqueIdService](spec, sn("uniqueid_service"))
	containers = append(containers, applyDockerDefaults(uniqueId_service))
	allServices = append(allServices, uniqueId_service)

	// Tomislav-RetCtx: the social graph calls the user service (GetUserId) and the user service calls the social graph
	// (InsertUser), as upstream's; the user service is named here before it is defined (a cycle over gRPC).
	socialgraph_service := workflow.Service[snbounded.SocialGraphService](spec, sn("socialgraph_service"), social_cache, social_db, sn("user_service"))
	containers = append(containers, applyDockerDefaults(socialgraph_service))
	allServices = append(allServices, socialgraph_service)

	hometimeline_service := workflow.Service[snbounded.HomeTimelineService](spec, sn("hometimeline_service"), hometimeline_cache, post_storage_service, socialgraph_service)
	containers = append(containers, applyDockerDefaults(hometimeline_service))
	allServices = append(allServices, hometimeline_service)

	user_service := workflow.Service[snbounded.UserService](spec, sn("user_service"), user_cache, user_db, socialgraph_service, "secret")
	containers = append(containers, applyDockerDefaults(user_service))
	allServices = append(allServices, user_service)

	text_service := workflow.Service[snbounded.TextService](spec, sn("text_service"), urlshorten_service, usermention_service)
	containers = append(containers, applyDockerDefaults(text_service))
	allServices = append(allServices, text_service)

	usertimeline_service := workflow.Service[snbounded.UserTimelineService](spec, sn("usertimeline_service"), usertimeline_cache, usertimeline_db, post_storage_service)
	containers = append(containers, applyDockerDefaults(usertimeline_service))
	allServices = append(allServices, usertimeline_service)

	composepost_service := workflow.Service[snbounded.ComposePostService](spec, sn("composepost_service"), post_storage_service, usertimeline_service, user_service, uniqueId_service, media_service, text_service, hometimeline_service)
	containers = append(containers, applyDockerDefaults(composepost_service))
	allServices = append(allServices, composepost_service)

	wrk2api_service := workflow.Service[snbounded.Wrk2APIService](spec, sn("wrk2api_service"), user_service, composepost_service, usertimeline_service, hometimeline_service, socialgraph_service)
	containers = append(containers, applyHTTPDefaults(wrk2api_service, trace_collector))
	allServices = append(allServices, wrk2api_service)

	// Synthetic span-pressure pump. Same HTTP-deploy pattern as wrk2api so
	// an external wrk can pace request rate. Each request emits N forced-LP
	// child spans via the TracePressureService implementation. Becomes a
	// DaemonSet with non-local traffic policy at d2k8s time.
	tracepressure_service := workflow.Service[snbounded.TracePressureService](spec, sn("tracepressure_service"))
	containers = append(containers, applyHTTPDefaults(tracepressure_service, trace_collector))
	allServices = append(allServices, tracepressure_service)

	tests := gotests.Test(spec, allServices...)
	containers = append(containers, tests, sn("otelcol"), sn("jaeger"))
	if useES {
		// ES has no pointer wrapper (jaeger talks to it over plain
		// TCP), so we add the .ctr IR node name directly.
		containers = append(containers, sn("elasticsearch")+".ctr")
	}

	return containers, nil
}

// makeDockerSpecNTBD: makeDockerSpecNT (docker_nt.go) with snbounded services.
func makeDockerSpecNTBD(spec wiring.WiringSpec, suffix, configPath string, useES bool) ([]string, error) {
	sn := func(name string) string { return name + "_" + suffix }

	var containers []string
	var allServices []string

	var jaeger_collector string
	if useES {
		jaeger_collector = jaeger.CollectorWithElasticsearch(spec, sn("jaeger"), sn("elasticsearch"))
	} else {
		jaeger_collector = jaeger.Collector(spec, sn("jaeger"))
	}
	trace_collector := otelcol.CollectorWithConfig(
		spec, sn("otelcol"),
		jaeger_collector,
		configPath,
		otelImage,
		8080, "jaeger")

	// NO opentelemetry.Instrument — services are not traced.
	applyDockerDefaults := func(serviceName string) string {
		retries.AddRetries(spec, serviceName, 3)
		grpc.Deploy(spec, serviceName)
		goproc.Deploy(spec, serviceName)
		return linuxcontainer.Deploy(spec, serviceName)
	}

	applyHTTPDefaults := func(serviceName string, collector string) string {
		retries.AddRetries(spec, serviceName, 3)
		clientpool.Create(spec, serviceName, 100)
		http.Deploy(spec, serviceName)
		goproc.Deploy(spec, serviceName)
		return linuxcontainer.Deploy(spec, serviceName)
	}

	// Backends
	user_cache := memcached.Container(spec, sn("user_cache")) // Tomislav-RetCtx: upstream user-memcached (DeathStarBench socialNetwork/docker-compose.yml)
	user_db := mongodb.Container(spec, sn("user_db"))
	post_cache := memcached.Container(spec, sn("post_cache")) // Tomislav-RetCtx: upstream post-storage-memcached (DeathStarBench socialNetwork/docker-compose.yml)
	post_db := mongodb.Container(spec, sn("post_db"))
	social_cache := redis.Container(spec, sn("social_cache"))
	social_db := mongodb.Container(spec, sn("social_db"))
	urlshorten_db := mongodb.Container(spec, sn("urlshorten_db"))
	usertimeline_cache := redis.Container(spec, sn("usertimeline_cache"))
	usertimeline_db := mongodb.Container(spec, sn("usertimeline_db"))
	hometimeline_cache := redis.Container(spec, sn("hometimeline_cache"))

	allServices = append(allServices,
		user_cache, user_db,
		post_cache, post_db,
		social_cache, social_db,
		usertimeline_cache, usertimeline_db,
		hometimeline_cache,
	)

	urlshorten_service := workflow.Service[snbounded.UrlShortenService](spec, sn("urlshorten_service"), urlshorten_db)
	containers = append(containers, applyDockerDefaults(urlshorten_service))
	allServices = append(allServices, urlshorten_service)

	usermention_service := workflow.Service[snbounded.UserMentionService](spec, sn("usermention_service"), user_cache, user_db)
	containers = append(containers, applyDockerDefaults(usermention_service))
	allServices = append(allServices, usermention_service)

	post_storage_service := workflow.Service[snbounded.PostStorageService](spec, sn("post_storage_service"), post_cache, post_db)
	containers = append(containers, applyDockerDefaults(post_storage_service))
	allServices = append(allServices, post_storage_service)

	media_service := workflow.Service[snbounded.MediaService](spec, sn("media_service"))
	containers = append(containers, applyDockerDefaults(media_service))
	allServices = append(allServices, media_service)

	uniqueId_service := workflow.Service[snbounded.UniqueIdService](spec, sn("uniqueid_service"))
	containers = append(containers, applyDockerDefaults(uniqueId_service))
	allServices = append(allServices, uniqueId_service)

	// Tomislav-RetCtx: the social graph calls the user service (GetUserId) and the user service calls the social graph
	// (InsertUser), as upstream's; the user service is named here before it is defined (a cycle over gRPC).
	socialgraph_service := workflow.Service[snbounded.SocialGraphService](spec, sn("socialgraph_service"), social_cache, social_db, sn("user_service"))
	containers = append(containers, applyDockerDefaults(socialgraph_service))
	allServices = append(allServices, socialgraph_service)

	hometimeline_service := workflow.Service[snbounded.HomeTimelineService](spec, sn("hometimeline_service"), hometimeline_cache, post_storage_service, socialgraph_service)
	containers = append(containers, applyDockerDefaults(hometimeline_service))
	allServices = append(allServices, hometimeline_service)

	user_service := workflow.Service[snbounded.UserService](spec, sn("user_service"), user_cache, user_db, socialgraph_service, "secret")
	containers = append(containers, applyDockerDefaults(user_service))
	allServices = append(allServices, user_service)

	text_service := workflow.Service[snbounded.TextService](spec, sn("text_service"), urlshorten_service, usermention_service)
	containers = append(containers, applyDockerDefaults(text_service))
	allServices = append(allServices, text_service)

	usertimeline_service := workflow.Service[snbounded.UserTimelineService](spec, sn("usertimeline_service"), usertimeline_cache, usertimeline_db, post_storage_service)
	containers = append(containers, applyDockerDefaults(usertimeline_service))
	allServices = append(allServices, usertimeline_service)

	composepost_service := workflow.Service[snbounded.ComposePostService](spec, sn("composepost_service"), post_storage_service, usertimeline_service, user_service, uniqueId_service, media_service, text_service, hometimeline_service)
	containers = append(containers, applyDockerDefaults(composepost_service))
	allServices = append(allServices, composepost_service)

	wrk2api_service := workflow.Service[snbounded.Wrk2APIService](spec, sn("wrk2api_service"), user_service, composepost_service, usertimeline_service, hometimeline_service, socialgraph_service)
	containers = append(containers, applyHTTPDefaults(wrk2api_service, trace_collector))
	allServices = append(allServices, wrk2api_service)

	tracepressure_service := workflow.Service[snbounded.TracePressureService](spec, sn("tracepressure_service"))
	containers = append(containers, applyHTTPDefaults(tracepressure_service, trace_collector))
	allServices = append(allServices, tracepressure_service)

	tests := gotests.Test(spec, allServices...)
	containers = append(containers, tests, sn("otelcol"), sn("jaeger"))
	if useES {
		containers = append(containers, sn("elasticsearch")+".ctr")
	}

	return containers, nil
}
