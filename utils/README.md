# utils

Scripts for setting up a cluster, deploying the benchmark applications, and running the
experiments.

## Setup and deployment

| Script | Purpose | Guide |
|---|---|---|
| [`setup_environment.sh`](setup_environment.sh) | One-time setup of the head node: toolchain, sibling repositories, image registry, CPU pinning | [Setting up a cluster](../docs/manual/cluster_setup.md) |
| [`install_blueprint_deps.sh`](install_blueprint_deps.sh) | Go, protobuf and kompose only (stage 4 of the setup) | [Setting up a cluster](../docs/manual/cluster_setup.md) |
| [`pin-cpu-22ghz.sh`](pin-cpu-22ghz.sh) | Fix the CPU frequency of one node or of every node | [CPU frequency](../docs/manual/cluster_setup.md#cpu-frequency) |
| [`build_deploy_dsb.sh`](build_deploy_dsb.sh) | Build and deploy the Social Network | [Deploying the benchmark applications](../docs/manual/deploying.md#social-network) |
| [`build_deploy_hotel.sh`](build_deploy_hotel.sh) | Build and deploy Hotel Reservation | [Deploying the benchmark applications](../docs/manual/deploying.md#hotel-reservation) |
| [`registry-*.yaml`](.) | The in-cluster image registry | [Securing the registry](../docs/manual/cluster_setup.md#securing-the-registry) |

`setup_environment.sh`, `pin-cpu-22ghz.sh` and both deploy scripts print their options with `--help`.
`install_blueprint_deps.sh` takes no options and starts installing at once.

### Helpers used by the deploy scripts

| Script | Purpose |
|---|---|
| [`gen_node_pinning.py`](gen_node_pinning.py) | Generate a Social Network placement file |
| [`pin_nodes.py`](pin_nodes.py) | Apply a placement file to a directory of manifests (`--dry-run` shows the changes) |
| [`inject_perf_env.py`](inject_perf_env.py) | Add the Jaeger, Elasticsearch and collector settings that Kubernetes conversion drops |
| [`checkpoint_distance.py`](checkpoint_distance.py), [`reverse_policy.py`](reverse_policy.py) | Validate and write the checkpoint distance and upward-truss options into the collector's configuration |

## Experiments

These scripts ran the measurement campaigns. They are research tooling, written for the cluster the
experiments ran on, and some contain its paths.

| Scripts | Purpose |
|---|---|
| `prepare_*.py`, `build_*.py`, `derive_*.py`, `fit_cluster_*.py`, `run_*.py` | Build an application once, derive variants from it, place them, and run load ramps with per-point measurements |
| `analyze_*.py`, `plot_*.py`, `ramp_*.py` | Tables and figures from the run directories |
| [`spanload/`](spanload/README.md) | OTLP load generator for collector-only experiments |
| [`grpc_interceptors/`](grpc_interceptors/README.md) | Comparison of the interceptor and wrapper instrumentation |

The options for random checkpoint distances and upward-truss routing are described in
[Randomized checkpoint distance](../docs/dev/randomized_checkpoint_distance.md) and
[Reverse checkpoint probability](../docs/dev/reverse_checkpoint_probability.md).
