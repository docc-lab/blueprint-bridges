# Deploying the benchmark applications

Two scripts take a DeathStarBench application from source to a running Kubernetes deployment in
one command:

| Application | Script | Kubernetes namespace |
|---|---|---|
| [Social Network](../../examples/dsb_sn) | [`utils/build_deploy_dsb.sh`](../../utils/build_deploy_dsb.sh) | `default` |
| [Hotel Reservation](../../examples/dsb_hotel) | [`utils/build_deploy_hotel.sh`](../../utils/build_deploy_hotel.sh) | `dsb-hotel` |

Each one compiles the application with Blueprint, builds the container images and pushes them to
the cluster's registry, converts the generated Docker Compose deployment to Kubernetes manifests,
and, if asked, deploys it. Both print a complete option reference with `--help`; this guide covers
what you need to use them.

## Before you start

* The cluster is set up as described in [Setting up a cluster](cluster_setup.md), and the
  collector image has been built and pushed once (see
  [Building the collector image](cluster_setup.md#building-the-collector-image)).
* Run the scripts from the repository root, in a shell where the Python environment is active:

  ```sh
  cd ~/blueprint-bridges
  source .venv/bin/activate
  ```

## Social Network

### Quick start

Build the Social Network with the path bridge, deploy it, and load the social graph:

```sh
utils/build_deploy_dsb.sh -s docker_pb_es -n pb --cpd 2 --apply --seed
```

When it finishes, it prints the NodePort of the `wrk2api` frontend:

```
=== DONE: pb-es deployed + seeded. Drive load via wrk2api NodePort 31234, ...
```

Send requests to it with wrk2, for example 500 posts per second for one minute:

```sh
wrk -D exp -t 4 -c 50 -d 60s -R 500 -L \
    -s examples/dsb_sn/scripts/compose-post.lua http://10.10.1.1:31234
```

[`examples/dsb_sn/scripts`](../../examples/dsb_sn/scripts) also has scripts that read timelines and
a mixed workload.

### Choosing the application variant

`-s` selects a wiring spec, which fixes both the version of the application and the tracing:

| Spec | Application |
|---|---|
| `docker_<kind>_es` | Blueprint's Social Network |
| `docker_<kind>_es_bd` | Social Network matching upstream DeathStarBench operation for operation |
| `docker_<kind>_es_nw` | Zero-work Social Network: the same services and calls, without databases, caches or computation |

| `<kind>` | Tracing |
|---|---|
| `pb` | Path bridge |
| `cgpb` | Call-graph path bridge |
| `sb` | Structural bridge |
| `v` | Vanilla OpenTelemetry, the baseline |
| `nt` | No tracing |
| `rc` | Random checkpoints, a control (`docker_rc_es` only) |

These specs store traces in Jaeger backed by Elasticsearch. `go run examples/dsb_sn/wiring/main.go -h`
lists every spec. The script sets the matching bridge kind for code generation (`OT_BRIDGE`) and for
the running services (`BRIDGE_KIND`); see [Interceptors and Middleware](interceptors.md#choosing-the-bridge).

`-n` names the build. Its output goes to `examples/dsb_sn/build_<name>/`, and a rebuild under the
same name replaces it. The Kubernetes resources are named after the spec, for example
`composepost-service-pb-es-ctr`. `--extra <suffix>` appends to that name, which keeps the images of
builds that differ in their configuration apart in the registry.

### Common tasks

**Build without deploying.** Leave out `--apply`; the manifests are written to
`examples/dsb_sn/build_<name>/k8s`:

```sh
utils/build_deploy_dsb.sh -s docker_sb_es -n sb --cpd 2
```

**Deploy.** `--apply` first removes whichever Social Network variant is running, so only one
occupies the cluster, then applies the build and waits until every pod is running. `--seed` then
exposes `wrk2api` as a NodePort and loads the social graph; zero-work variants have no databases, so
use `--noseed` with them, which only exposes the NodePort.

**Set the checkpoint distance.** `--cpd <N>` places a checkpoint every N hops. `--cpd-min` and
`--cpd-max` instead draw each distance at random from a range (1 to 256). The values are written into
the collector's configuration, which the span processors read when they start.

**Choose the collector's behavior.** By default, the collector runs the bridges' priority processor,
which sheds low-priority spans under memory pressure; `--soft`, `--hard` and `--cp-safety` tune it.
For measuring tracing overhead without any shedding, use `--collector passthrough`.

**Choose the garbage collection mode.** `--gc natural` runs the services with Go's default
(`GOGC=100`); `--gc forced` turns automatic collection off and collects ten times a second instead.

**Rebuild the collector image as well**, after changing the collector:

```sh
utils/build_deploy_dsb.sh -s docker_pb_es -n pb --cpd 2 --build-collector --apply --seed
```

**Reuse images from an earlier build.** `--skip-build` regenerates the manifests without building or
pushing images. Set `IMAGE_TAG` to the tag of the images to reuse, which appears in the `image:`
lines of the earlier build's manifests:

```sh
IMAGE_TAG=v20261113-084318 utils/build_deploy_dsb.sh -s docker_pb_es -n pb --cpd 2 --skip-build --apply
```

**Remove a deployment:**

```sh
kubectl delete -f examples/dsb_sn/build_<name>/k8s
```

### Placement

Each build pins its services to nodes. The placement is read from
`examples/dsb_sn/node-pinning-<name>.yaml`; if that file does not exist, the script generates it with
[`utils/gen_node_pinning.py`](../../utils/gen_node_pinning.py) in one of three layouts:

| Layout | Option | Description |
|---|---|---|
| Hot node | (default) | ComposePost and the home timeline share one node, so that node's collector takes most of the tracing load; the other services are spread over the remaining nodes. For studies of load shedding. |
| Anti-affinity | `--anti-affinity` | No two services that call each other share a node, so every traced call crosses the network. For overhead studies. |
| One per node | `--one-per-node` | Every service on its own node, chosen from the schedulable nodes. Needs at least 14. |

The hot-node and anti-affinity layouts name the nodes `node-1` to `node-9`, as on CloudLab, and
reserve up to 25 CPUs for one service. On other clusters, either use `--no-pin-requests`, which keeps
the placement but reserves no CPUs, or write the placement file yourself:

```yaml
# examples/dsb_sn/node-pinning-<name>.yaml: node name -> the deployments placed on it
node-1:
  - composepost-service-pb-es-ctr:
      requests_cpu: 12000      # millicores; optional, as are limits_cpu, requests_memory, limits_memory
  - hometimeline-service-pb-es-ctr: {}
```

An existing file is never overwritten, so hand edits are kept. With `--anti-affinity` and
`--one-per-node`, also pass `--wrk2api-deploy`, which runs the frontend as a single Deployment that
can be placed, instead of one pod per node.

### How the build works

| Step | Action |
|---|---|
| 0 | With `--build-collector`: builds and pushes the collector image. |
| 1 | Compiles the wiring spec into `examples/dsb_sn/build_<name>/`, with `OT_BRIDGE` set from the spec. |
| 2 | Removes unused imports from the generated code (`goimports`). |
| 3 | Sets up the collector's configuration: the priority processor or passthrough, and the checkpoint distance. |
| 4 | Converts the Compose deployment to Kubernetes manifests with [`d2k8s`](../../d2k8s), building and pushing the images. The collector runs as a DaemonSet on every node. |
| 5 | Adds the performance settings Kubernetes conversion does not produce: Elasticsearch bulk-indexing settings for Jaeger, memory and CPU for the collector, and the GC mode ([`utils/inject_perf_env.py`](../../utils/inject_perf_env.py)). |
| 6 | Applies the placement ([`utils/pin_nodes.py`](../../utils/pin_nodes.py)). |
| 7 | With `--apply`: removes the running variant, applies the manifests and waits for the pods. |
| 8 | With `--seed` or `--noseed`: exposes `wrk2api` as a NodePort; with `--seed`, loads the social graph ([`init_social_graph.py`](../../examples/dsb_sn/scripts/init_social_graph.py), Facebook's Reed98 network: 962 users, 37,624 follows). |

## Hotel Reservation

### Quick start

```sh
utils/build_deploy_hotel.sh -s docker_cgpb_es --cpd 3 --apply --nodeport
```

The build goes to `examples/dsb_hotel/build_<name>/` (the name defaults to the spec without
`docker_`, so `cgpb_es` here). The services fill their databases themselves when they start, so
there is no separate seeding step. When the deployment is ready, the script prints how to reach the
frontend and Jaeger:

```
Frontend: kubectl -n dsb-hotel port-forward service/frontend-service-hotel-cgpb-es-ctr 9000:2000
Jaeger:   kubectl -n dsb-hotel port-forward service/jaeger-hotel-cgpb-es-ctr 16686:16686
```

With `--nodeport`, the frontend is also exposed on a NodePort, shown by
`kubectl -n dsb-hotel get service frontend-service-hotel-cgpb-es-ctr`. Load it with the search
workload:

```sh
wrk -D exp -t 4 -c 50 -d 60s -R 500 -L \
    -s examples/dsb_hotel/scripts/search-hotels.lua http://10.10.1.1:<NodePort>
```

### Options

The specs follow the Social Network's naming: `docker_<kind>` (Jaeger with in-memory storage),
`docker_<kind>_es` (Jaeger with Elasticsearch) and the zero-work `docker_<kind>_es_nw`. Most options match
`build_deploy_dsb.sh`: `--cpd`, `--cpd-min` and `--cpd-max`, `--collector passthrough`, `--soft`,
`--hard`, `--cp-safety`, `--gc` (default `forced` here), `--extra`, `--build-collector` and
`--one-per-node`. The differences:

| Option | Effect |
|---|---|
| `-n <name>` | Build name; the output directory must not exist yet. |
| `--node-pinning <file>` | Placement file in the format above; requires `--frontend-deploy`. |
| `--nodeport [port]` | Expose the frontend on a NodePort, chosen by Kubernetes unless given. |
| `--skip-build` | Generate the manifests without building images; `--image-tag` names the existing images to use. |
| `--reverse-truss` | Enable the response path, with the frontend as the root. |
| `--sample-ratio <r>` | Head sampling ratio for traces. |

Deployments go to their own namespace (`--namespace`, default `dsb-hotel`). To remove them:

```sh
kubectl delete namespace dsb-hotel
```

## Configuration

Both scripts take their locations from the environment, so they work wherever the repository lives:

| Variable | Default | Used for |
|---|---|---|
| `BLUEPRINT_REGISTRY` | `10.10.1.1:30000` | Registry that images are pushed to and pulled from |
| `BLUEPRINT_COLLECTOR_SRC` | `opentelemetry-collector-contrib` next to this repository | Collector checkout: its configuration files and `build-and-push.sh` |
| `SEED_HOST` | the registry's host | Node address the Social Network seeder sends requests to |
| `IMAGE_TAG` | a new timestamp | Tag of the application images (Social Network) |

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Pods stay `Pending` | The placement names a node that does not exist, or one without enough free CPU. Check `examples/dsb_sn/node-pinning-<name>.yaml` against `kubectl get nodes`, or rebuild with `--no-pin-requests`. |
| `ImagePullBackOff` after `--skip-build` | The manifests name images that were never pushed. Set `IMAGE_TAG` to the tag of an earlier build. |
| A service exits at startup with `tracing invariant broken: the span processor (BRIDGE_KIND) is the bridge the instrumentation was built for` | The service was generated for one bridge and runs another. The scripts set both; when building by hand, keep `OT_BRIDGE` and `BLUEPRINT_BRIDGE_KIND` equal. |
| Collector pods in `CrashLoopBackOff` | The collector image does not match its configuration. Rebuild it with `--build-collector`. |
| Seeding fails or the application returns errors right after it | The Python environment was not active, so the seeder lacked aiohttp. Activate it and rerun with `--seed`. |
| Load-test results vary from run to run | Check that the CPU frequency is pinned: `utils/pin-cpu-22ghz.sh check --all`. |
