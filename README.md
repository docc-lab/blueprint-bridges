# Blueprint Bridges

Blueprint Bridges adds trace checkpointing to the OpenTelemetry tracing of
[Blueprint](https://github.com/blueprint-uservices/blueprint), a compiler for microservice
applications.

When a tracing pipeline is overloaded it drops spans, and a trace that has lost spans can no longer
be pieced together. The span processors in this repository, the *bridges*, export a small share of
spans as high-priority *checkpoints*. Each checkpoint carries a compact record of the call path
around it, enough to place the spans that survive between checkpoints. The collector can then shed
low-priority spans first and the traces remain reconstructable.

This repository is a fork of Blueprint. Blueprint's compiler, plugins and applications work as
documented in its [user manual](docs/manual).

## Contents

| Path | Contents |
|---|---|
| [`runtime/plugins/otelcol`](runtime/plugins/otelcol) | The span processors: the bridges, a vanilla baseline, the response path and the OTLP export |
| [`runtime/core/backend`](runtime/core/backend) | The trace carrier and the reverse context, shared by the instrumentation and the processors |
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

| Processor | `BRIDGE_KIND` | What a checkpoint records |
|---|---|---|
| Vanilla | `v` | No checkpoints: plain OpenTelemetry export, the baseline |
| Path bridge (PB) | `pb` | The checkpoint above it and a Bloom filter of the spans in between |
| Call-graph path bridge (CGPB) | `cgpb` | PB, plus the spans where the call path branches |
| Structural bridge (SB) | `sb` | CGPB, plus the order in which siblings started and ended |

Checkpoints are spaced a configurable number of hops apart, the checkpoint distance. The bridges
export their spans in separate high- and low-priority batches, marked with the `bridges-priority`
gRPC metadata. With `REVERSE_TRUSS=on`, responses also carry a *reverse context* back up the call
path, so a checkpoint can be exported by an ancestor instead of the span that produced it.

The processors read their parameters (checkpoint distance, reverse-path policy) from the collector
when they start, and the collector admits or refuses each batch by its priority. That collector
component is a processor for the OpenTelemetry Collector, maintained in a separate repository.

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
