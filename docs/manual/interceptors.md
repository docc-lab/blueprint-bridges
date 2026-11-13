# Interceptors and Middleware

Blueprint can install code around every RPC of a service without changing the service or generating
wrapper services: **gRPC interceptors** for services deployed with the gRPC plugin, and **HTTP
middleware** for services deployed with the HTTP plugin.

This page covers:

1. [Tracing with interceptors](#tracing-with-interceptors): the OpenTelemetry plugin's
   `InstrumentGRPC` and `InstrumentHTTP`, which trace services this way.
2. [Writing your own gRPC interceptor](#writing-your-own-grpc-interceptor): the gRPC plugin accepts
   any interceptor, for logging, metrics, authentication or anything else.
3. [Writing your own HTTP middleware](#writing-your-own-http-middleware): the same for HTTP services.
4. [Reference](#reference): functions, environment variables and wire formats.

## Tracing with interceptors

### Quick start

Trace a gRPC service with `opentelemetry.InstrumentGRPC` and an HTTP service with
`opentelemetry.InstrumentHTTP`. Each takes the service and the collector client to export to:

```go
import (
	"github.com/blueprint-uservices/blueprint/blueprint/pkg/wiring"
	"github.com/blueprint-uservices/blueprint/plugins/goproc"
	"github.com/blueprint-uservices/blueprint/plugins/grpc"
	"github.com/blueprint-uservices/blueprint/plugins/http"
	"github.com/blueprint-uservices/blueprint/plugins/jaeger"
	"github.com/blueprint-uservices/blueprint/plugins/linuxcontainer"
	"github.com/blueprint-uservices/blueprint/plugins/opentelemetry"
	"github.com/blueprint-uservices/blueprint/plugins/otelcol"
	"github.com/blueprint-uservices/blueprint/plugins/workflow"

	"github.com/blueprint-uservices/blueprint/examples/leaf/workflow/leaf"
)

func makeSpec(spec wiring.WiringSpec) ([]string, error) {
	jaegerCollector := jaeger.Collector(spec, "jaeger")
	collector := otelcol.Collector(spec, "otelcol", jaegerCollector, 8080, "jaeger")

	grpcService := func(name string) string {
		opentelemetry.InstrumentGRPC(spec, name, collector)
		grpc.Deploy(spec, name)
		goproc.Deploy(spec, name)
		return linuxcontainer.Deploy(spec, name)
	}
	httpService := func(name string) string {
		opentelemetry.InstrumentHTTP(spec, name, collector)
		http.Deploy(spec, name)
		goproc.Deploy(spec, name)
		return linuxcontainer.Deploy(spec, name)
	}

	leafService := workflow.Service[*leaf.LeafServiceImpl](spec, "leaf_service")
	frontend := workflow.Service[*leaf.NonLeafServiceImpl](spec, "nonleaf_service", leafService)
	return []string{grpcService(leafService), httpService(frontend)}, nil
}
```

`InstrumentGRPC` can be called before or after `grpc.Deploy`, and `InstrumentHTTP` before or after
`http.Deploy`; the interceptors are recorded on the service and picked up when its server and clients
are built. `examples/leaf/wiring/specs/docker_tree.go` and `examples/dsb_sn/wiring/specs/` are
complete specs that use both.

### Interceptors or wrappers

The OpenTelemetry plugin can trace a service in two ways:

| | Wrappers (`opentelemetry.Instrument`) | Interceptors (`InstrumentGRPC`, `InstrumentHTTP`) |
|---|---|---|
| Generated code | A server and a client wrapper service per traced service | None: the runtime package `runtime/plugins/opentelemetry/interceptors` |
| Service interface | Every method gains a `traceCtx` argument and a `retCtx` result | Unchanged |
| Trace context | Inside the request and response messages, as JSON | In gRPC metadata or HTTP headers, in a compact binary form |
| Spans, names, attributes | | Identical to the wrappers |

Both produce the same traces, and they interoperate: a service traced with wrappers (or with HTTP
middleware) can call a service traced with interceptors. To build an existing spec with wrappers
instead, set `OT_GRPC_MODE=wrapper` when compiling: `InstrumentGRPC` and `InstrumentHTTP` then call
`Instrument`, so the same spec builds either way.

### Choosing the bridge

The tracing has one variant per bridge kind: vanilla OpenTelemetry tracing, or one of the bridges:
the path bridge (P-Bridge, `pb`), the call-graph bridge (CG-Bridge, `cgpb`) or the structural bridge
(S-Bridge, `sb`). The [README](../../README.md#bridges) describes what each one preserves. The kind
is chosen twice:

* **At build time**, for the instrumentation, by the `OT_BRIDGE` environment variable of the
  compiler: `v` (or `vanilla`), `pb` (or `path`, `rc`, or empty), `cgpb` or `sb`. Any other value is
  a build error. The value applies to every service in the build.
* **At run time**, for the span processor in each service process, by the `BRIDGE_KIND` environment
  variable: `v`, `pb` (also used when unset), `cgpb`, `sb`, and the experimental `eb`, `rc`,
  `priority` and `realtime`. Any other value stops the process at startup.

The two must agree: an interceptor built for one bridge propagates that bridge's state, and a
processor of another bridge would classify the spans by different rules. Each interceptor checks this
when it is created and stops the process on a mismatch (`v` goes with `vanilla`; `eb`, `rc`,
`priority` and `realtime` are not checked).

With the Docker Compose deployer, `BRIDGE_KIND` can be set at build time too: when the compiler runs
with `BLUEPRINT_BRIDGE_KIND` set, every generated container gets `BRIDGE_KIND` with that value. The
Social Network specs (`examples/dsb_sn/wiring/specs/`) derive `BLUEPRINT_BRIDGE_KIND` from the spec
name, so a build only needs the matching `OT_BRIDGE`:

```sh
cd examples/dsb_sn/wiring
OT_BRIDGE=sb go run main.go -w docker_sb_es_bd -o build
```

### What happens on a call

Each call carries a *forward context* down the call path: the span context and the baggage, which
holds the bridge's truss. With `REVERSE_TRUSS=on`, each response carries a *reverse context* back
up, which holds upward trusses.

On the **caller** side, the client interceptor:

1. starts a client span as a child of the span in the call's context;
2. for the bridges, takes the next child ordinal from the enclosing server call (a bridge client call
   must run inside a traced request);
3. sends the span context and the baggage in the request metadata `bp-trace-ctx`;
4. when the response arrives, hands any reverse context in the response trailer `bp-ret-ctx` to the
   span processor and adds the rest to the enclosing server's reverse context;
5. records the error on the span if the call failed, and ends the span.

On the **callee** side, the server interceptor:

1. reads `bp-trace-ctx` and continues the caller's trace;
2. starts a server span and puts the request's tracing state on the context, so the service's own
   outgoing calls become its children;
3. runs the service;
4. records the children on the span (bridges), lets the span processor finish its checkpoint
   decision, and, with `REVERSE_TRUSS=on`, returns the reverse context in the trailer `bp-ret-ctx`;
5. ends the span.

A call with no `bp-trace-ctx` starts a new trace. A trace that head sampling dropped
(`OTEL_SAMPLE_RATIO`) is passed on with only its IDs, and neither side creates spans for it.

Span and tracer names are those of the wrappers: the server span of method `M` of service `S` is
`SServer_M` (tracer `S`), and the client span is `SClient_M` (tracer `S_OTServerWrapperInterface`).

HTTP services work the same way, with the request header `Bp-Trace-Ctx` and the response header
`Bp-Ret-Ctx`. Because a response header must be set before the body, the server middleware computes
the reverse context when the handler starts writing the response.

### Run-time settings

| Variable | Where | Meaning |
|---|---|---|
| `BRIDGE_KIND` | every traced process | span processor kind; see [Choosing the bridge](#choosing-the-bridge) |
| `REVERSE_TRUSS` | every traced process | `on` enables upward trusses: responses carry trusses back up the call path, in the reverse context |
| `OTEL_SAMPLE_RATIO` | every traced process | head sampling ratio in [0,1]; unset samples everything |
| `GRPC_STREAM_WORKERS` | gRPC servers | worker goroutines that reuse their stacks across calls; default 32 or `GOMAXPROCS`, whichever is larger; `0` turns them off |

The span processors have further settings (export deadline, retry policy, refusal counting); see
`runtime/plugins/otelcol`.

### Errors and invariants

A failed call is recorded on its span; the interceptors never change a call's result. Conditions that
only a bug in the tracing code can cause are checked, and a broken one panics with a
`*backend.InvariantError` that names the rule:

* `OT_BRIDGE` or `BRIDGE_KIND` is unknown, or the two do not agree;
* a bridge client call is made outside a traced request (no enclosing server call);
* `bp-trace-ctx` or `bp-ret-ctx` does not decode, names no valid span, or appears more than once;
* a child ordinal is below 1, or there are more end events than children.

### Limitations

* Only unary gRPC methods; the gRPC plugin does not generate streaming methods.
* One bridge kind per build: `OT_BRIDGE` applies to every service compiled.
* With a bridge, a traced service's outgoing calls must happen while it is handling a traced request.

## Writing your own gRPC interceptor

The gRPC plugin installs any number of unary interceptors on a service's server and on the clients
that call it. It does not care what they do; tracing is one user. An interceptor has three parts:

1. **Run-time code**: a constructor that returns a standard `grpc.UnaryServerInterceptor` or
   `grpc.UnaryClientInterceptor`. Like any plugin's run-time code, it must live in a separate runtime
   module (see [Developing a Plugin](plugin_development.md)).
2. **An IR node** that declares the call to that constructor in the generated code.
3. **A wiring function** that defines the node and registers it with `grpc.AddServerInterceptor` or
   `grpc.AddClientInterceptor`.

### Run-time code

```go
package requestlog

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
)

// NewServerLogger returns an interceptor that logs every call the server handles.
func NewServerLogger(ctx context.Context, prefix string) (grpc.UnaryServerInterceptor, error) {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		slog.Info(prefix, "method", info.FullMethod, "elapsed", time.Since(start), "error", err)
		return resp, err
	}, nil
}
```

The generated code passes the context as the first argument; the other arguments are the IR nodes the
IR node lists (below).

### The IR node

Embed `grpc.ServerInterceptorNode` (or `grpc.ClientInterceptorNode`). It tells the generated
dependency-injection code that the node's instance is an interceptor function rather than a string,
and marks the node as a server (or client) interceptor.

```go
package requestlog

import (
	"github.com/blueprint-uservices/blueprint/blueprint/pkg/ir"
	"github.com/blueprint-uservices/blueprint/plugins/golang"
	"github.com/blueprint-uservices/blueprint/plugins/golang/gocode"
	"github.com/blueprint-uservices/blueprint/plugins/grpc"
)

type ServerLogger struct {
	grpc.ServerInterceptorNode

	NodeName string
	Prefix   string
}

func (n *ServerLogger) Name() string          { return n.NodeName }
func (n *ServerLogger) String() string        { return n.NodeName + " = RequestLog(" + n.Prefix + ")" }
func (n *ServerLogger) ImplementsGolangNode() {}

// AddInstantiation declares `requestlog.NewServerLogger(ctx, prefix)` in the generated process.
func (n *ServerLogger) AddInstantiation(builder golang.NamespaceBuilder) error {
	if builder.Visited(n.NodeName) {
		return nil
	}
	constructor := &gocode.Constructor{
		Package: "github.com/example/requestlog/runtime/requestlog",
		Func: gocode.Func{
			Name: "NewServerLogger",
			Arguments: []gocode.Variable{
				{Name: "ctx", Type: &gocode.UserType{Package: "context", Name: "Context"}},
				{Name: "prefix", Type: &gocode.BasicType{Name: "string"}},
			},
		},
	}
	return builder.DeclareConstructor(n.NodeName, constructor, []ir.IRNode{&ir.IRValue{Value: n.Prefix}})
}
```

### The wiring function

```go
package requestlog

import (
	"github.com/blueprint-uservices/blueprint/blueprint/pkg/ir"
	"github.com/blueprint-uservices/blueprint/blueprint/pkg/wiring"
	"github.com/blueprint-uservices/blueprint/plugins/grpc"
)

// Server logs every call that serviceName's gRPC server handles.
func Server(spec wiring.WiringSpec, serviceName string, prefix string) {
	name := serviceName + ".requestlog"
	spec.Define(name, &ServerLogger{}, func(ns wiring.Namespace) (ir.IRNode, error) {
		return &ServerLogger{NodeName: name, Prefix: prefix}, nil
	})
	grpc.AddServerInterceptor(spec, serviceName, name)
}
```

A wiring spec then calls `requestlog.Server(spec, "leaf", "leaf rpc")` next to `grpc.Deploy`.

### Ordering and scope

* `AddServerInterceptor` installs an interceptor on the service's gRPC server.
  `AddClientInterceptor` installs it on every gRPC client of the service, that is, on the caller side
  of every call to it.
* Both may be called before or after `grpc.Deploy`.
* Interceptors run in the order they were added: the first one added is the outermost, so it sees the
  call first and the response last. `InstrumentGRPC` adds the tracing interceptors when it is called,
  so call it first if other interceptors should run inside the traced span.
* A client interceptor runs inside the call's deadline: the generated client applies the timeout from
  `grpc.Deploy` (one second) or `grpc.DeployWithTimeout` before invoking the interceptors.
* A service with no interceptors is generated and dialed exactly as before.

## Writing your own HTTP middleware

The HTTP plugin offers the same mechanism. A middleware's run-time instance has one of the types of
`runtime/plugins/httpmiddleware`:

```go
// ServerMiddleware wraps the handler of one method; called once per method when the server starts.
type ServerMiddleware func(service, method string, next http.Handler) http.Handler

// ClientMiddleware wraps the transport of an HTTP client of a service; called once per client.
type ClientMiddleware func(service string, next http.RoundTripper) http.RoundTripper
```

Unlike a gRPC interceptor, HTTP middleware has no method descriptor, so the server passes the service
and method names in, and a client middleware reads the method from the request path (`/<Method>`).

The IR node embeds `http.ServerMiddlewareNode` (or `http.ClientMiddlewareNode`), declares a constructor
that returns the middleware, and is registered with `http.AddServerMiddleware` (or
`http.AddClientMiddleware`). Ordering works as for gRPC interceptors.

Two details of the generated HTTP code matter to middleware:

* The service runs with the values on the request's context, so a server middleware can pass
  values to it, but not with the request's cancellation: it runs to completion even if the caller
  goes away. Client requests likewise carry the caller's context values but not its cancellation.
* An HTTP response carries only a status code and a message. A server middleware that needs the Go
  error a method returned can wrap the `ResponseWriter` in a type that implements
  `httpmiddleware.ErrorRecorder`; the generated server calls `httpmiddleware.RecordError` with the
  error before writing the error response.

## Reference

### Wiring functions

| Function | Plugin | Purpose |
|---|---|---|
| `opentelemetry.InstrumentGRPC(spec, service, collector)` | `plugins/opentelemetry` | trace a gRPC service with interceptors |
| `opentelemetry.InstrumentHTTP(spec, service, collector)` | `plugins/opentelemetry` | trace an HTTP service with middleware |
| `opentelemetry.Instrument(spec, service, collector)` | `plugins/opentelemetry` | trace with wrapper services |
| `grpc.AddServerInterceptor(spec, service, node)` | `plugins/grpc` | install an interceptor on a service's server |
| `grpc.AddClientInterceptor(spec, service, node)` | `plugins/grpc` | install an interceptor on a service's clients |
| `grpc.DeployWithTimeout(spec, service, timeout)` | `plugins/grpc` | `grpc.Deploy` with a per-call deadline other than one second |
| `http.AddServerMiddleware(spec, service, node)` | `plugins/http` | install a middleware on a service's server |
| `http.AddClientMiddleware(spec, service, node)` | `plugins/http` | install a middleware on a service's clients |

### Build-time settings

| Variable | Meaning |
|---|---|
| `OT_BRIDGE` | bridge kind of the tracing instrumentation: `v`/`vanilla`, `pb`/`path`/`rc`/empty, `cgpb`, `sb` |
| `OT_GRPC_MODE` | `wrapper` makes `InstrumentGRPC` and `InstrumentHTTP` fall back to wrapper services |

### On the wire

| Transport | Forward context (request) | Reverse context (response) |
|---|---|---|
| gRPC | metadata `bp-trace-ctx` | trailer `bp-ret-ctx` (only with `REVERSE_TRUSS=on`) |
| HTTP | header `Bp-Trace-Ctx` | header `Bp-Ret-Ctx` (only with `REVERSE_TRUSS=on`) |

The forward context is a small binary record, written as header text:

```
sampled trace:    0x01 | trace ID (16 bytes) | span ID (8) | trace flags (1)
                  | uvarint len, trace state | uvarint n, then n x (uvarint len, key, uvarint len, value)
unsampled trace:  0x00 | trace ID (16 bytes) | span ID (8)
```

It is base64-encoded with an alphabet of the printable ASCII characters that have the longest HPACK
Huffman codes, so HTTP/2 sends it as plain bytes instead of Huffman-coding it on every call. The
reverse context is the backend's packed envelope (`runtime/core/backend/reversetruss_packed.go`),
written with the same alphabet.
