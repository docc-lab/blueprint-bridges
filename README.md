# Blueprint Bridges

Blueprint Bridges implements *bridges*, a primitive for distributed tracing, in OpenTelemetry. It
is built on [Blueprint](https://github.com/blueprint-uservices/blueprint), a compiler for
microservice applications.

Tracing infrastructures lose trace records under load, and the traces they store are then often
fragments of a request's workflow, separated by *holes* of missing records. Tools that depend on a
trace's topology, from critical-path analysis to autoscaling, are misled by them. Bridges let trace
fragments be reconnected across holes:

* Services propagate small amounts of ancestry data, *trusses*, with each request, in the trace
  context's baggage.
* A truss only has to be stored reliably once. Every few hops a span becomes a *checkpoint*, exports
  the truss it has accumulated, and starts a new one. The collector protects checkpoints and sheds
  other spans first when it is overloaded.
* From the surviving fragments and checkpoints, a reconstructor reconnects the fragments and marks
  where data was lost.

This repository is a fork of Blueprint. Blueprint's compiler, plugins and applications work as
documented in its [user manual](docs/manual).

## Contents

| Path | Contents |
|---|---|
| [`runtime/plugins/otelcol`](runtime/plugins/otelcol) | The span processors: the bridges, a vanilla baseline, upward trusses and the OTLP export |
| [`runtime/core/backend`](runtime/core/backend) | The trace carrier and the reverse context that holds upward trusses, shared by the instrumentation and the processors |
| [`runtime/plugins/opentelemetry/interceptors`](runtime/plugins/opentelemetry/interceptors) | Tracing instrumentation as gRPC interceptors and HTTP middleware |
| [`plugins/opentelemetry`](plugins/opentelemetry) | Wiring functions that instrument services, and the generated wrapper instrumentation |
| [`plugins/otelcol`](plugins/otelcol) | Deploys the OpenTelemetry Collector the processors export to |
| [`plugins/grpc`](plugins/grpc), [`plugins/http`](plugins/http) | Support for interceptors and middleware in general, not only for tracing |
| [`examples/dsb_sn`](examples/dsb_sn), [`examples/dsb_hotel`](examples/dsb_hotel) | DeathStarBench Social Network and Hotel Reservation, with the variants used in experiments |
| [`examples/leaf`](examples/leaf) | Small synthetic service graphs for testing |
| [`k8s-convert`](k8s-convert), [`d2k8s`](d2k8s) | Tools that turn generated Docker Compose deployments into Kubernetes manifests |
| [`utils`](utils) | Scripts that build, deploy and measure the applications on a cluster |

The other applications from Blueprint (Sock Shop, Train Ticket and the greeter examples) are in
[`examples`](examples).

## Bridges

There are three kinds of bridge. Each preserves more of the topology inside holes than the one
before it, and costs more:

| Bridge | `BRIDGE_KIND` | Preserves | Truss contents |
|---|---|---|---|
| Path bridge (P-Bridge) | `pb` | Connectivity: which fragments belong together, along which path | The last checkpoint, depth, and a Bloom filter of the ancestors since that checkpoint |
| Call-graph bridge (CG-Bridge) | `cgpb` | The call graph, without the order of calls | P-Bridge, plus at most one *fan-out witness*: a span where the call path branched |
| Structural bridge (S-Bridge) | `sb` | The call graph and the order of sibling calls, for critical-path analysis | CG-Bridge, plus the order in which siblings started and ended |

Vanilla OpenTelemetry tracing (`v`) runs on the same export path and serves as the baseline.

* **Vertical trusses** travel down the call path with requests. The S-Bridge also uses *orthogonal
  trusses*, which pass the order in which siblings ended on to later spans.
* **Fan-out witnesses** (CG-Bridge and S-Bridge) record where a call path branches. When a span
  starts its second child, that child's truss gets a witness: the span and the child's depth. The
  witness then passes down through first children only. Later children start without one, a second
  child further down replaces it with its own, and a checkpoint exports it and opens its window
  without one. A truss carries at most one witness, and often none.
* **Upward trusses** travel back up the call path in responses (`REVERSE_TRUSS=on`). Leaf services
  would otherwise have to checkpoint every truss that reaches them; instead, an ancestor checkpoints
  it. A policy picks the ancestor: a TTL, a fixed probability, or a probability that depends on
  depth.
* **Checkpoints** are placed a checkpoint distance apart. The distance is fixed, or drawn at random
  from a range so that checkpoints do not always land on the same services. Spans are exported in
  separate checkpoint and non-checkpoint batches, marked with the `bridges-priority` gRPC metadata.

The span processors read their parameters (checkpoint distance, upward-truss policy) from the
collector when they start. The collector side, which admits checkpoints and sheds other spans first
under memory pressure, is a set of OpenTelemetry Collector components maintained in a separate
repository. Trace reconstruction is not part of this repository.

## Getting started

Start with Blueprint's [requirements](docs/manual/requirements.md) and
[getting started](docs/manual/gettingstarted.md) guide.

To trace a service with a bridge, instrument it in the wiring spec:

```go
collector := otelcol.Collector(spec, "otelcol", jaegerCollector, 8080, "jaeger")

opentelemetry.InstrumentGRPC(spec, serviceName, collector)
grpc.Deploy(spec, serviceName)
```

The bridge is chosen when compiling, with `OT_BRIDGE`, and when running, with `BRIDGE_KIND` in each
service's environment; the two must match. For example, the Social Network with the structural
bridge (its specs set `BRIDGE_KIND` in every generated container):

```sh
cd examples/dsb_sn/wiring
OT_BRIDGE=sb go run main.go -w docker_sb_es_bd -o build
```

[Interceptors and Middleware](docs/manual/interceptors.md) describes the instrumentation, its
settings and how to write your own interceptors.

To run the benchmark applications on a Kubernetes cluster, first prepare the cluster with
[Setting up a cluster](docs/manual/cluster_setup.md), then build and deploy with
[Deploying the benchmark applications](docs/manual/deploying.md).

## Tests

```sh
cd runtime
go test ./core/backend/ ./plugins/otelcol/ ./plugins/opentelemetry/interceptors/
```

## Credits and license

Blueprint is the work of Vaastav Anand, Deepak Garg, Antoine Kaufmann and Jonathan Mace:

> Vaastav Anand, Deepak Garg, Antoine Kaufmann, Jonathan Mace.
> [Blueprint: A Toolchain for Highly-Reconfigurable Microservices](https://blueprint-uservices.github.io/assets/pdf/anand2023blueprint.pdf).
> *29th ACM Symposium on Operating Systems Principles (SOSP '23)*.

This repository is distributed under Blueprint's BSD 3-Clause license; see [`LICENSE`](LICENSE).
