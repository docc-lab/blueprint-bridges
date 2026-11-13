# Setting up a cluster

This guide prepares a Kubernetes cluster to build and deploy the benchmark applications with
[`utils/build_deploy_dsb.sh`](../../utils/build_deploy_dsb.sh) and
[`utils/build_deploy_hotel.sh`](../../utils/build_deploy_hotel.sh). Most of the work is done by one
script, [`utils/setup_environment.sh`](../../utils/setup_environment.sh), which you run once on the
cluster's head node: the machine you build on and run `kubectl` from.

When you are done, continue with [Deploying the benchmark applications](deploying.md).

## Requirements

The setup script installs the build toolchain, but it does not create the cluster. Have these in
place first.

| Requirement | Details |
|---|---|
| Kubernetes cluster | Kubernetes 1.29 or later, with `kubectl` admin access from the head node. Tested on Kubespray-built clusters of 10 and 15 CloudLab c220g5 nodes. |
| Operating system | Ubuntu 22.04 (the scripts use `apt`), x86-64 or arm64, with passwordless `sudo` on the head node. |
| Docker on the head node | Docker Engine, used to build and push images. The setup script adds you to the `docker` group but does not install Docker. |
| Cluster network | The registry is reached at `10.10.1.1:30000` by default: the head node's address on the cluster network. Elsewhere, set `BLUEPRINT_REGISTRY` (see [Configuration](deploying.md#configuration)). |
| Insecure registry access on every node | The registry serves plain HTTP. Each node's container runtime must accept it: for Docker, `--insecure-registry=10.10.1.0/24`; for containerd, an equivalent `hosts.toml` entry. |
| Storage | A StorageClass named `nfs-client` for the registry's 20 GiB volume, for example from [nfs-subdir-external-provisioner](https://github.com/kubernetes-sigs/nfs-subdir-external-provisioner). |
| Privileged pods | Allowed in your current `kubectl` namespace; CPU frequency pinning runs a short-lived privileged pod on each node. |
| Internet access | On the head node for `apt`, Go, GitHub (the collector, DeathStarBench, kompose) and the public images the builds start from; on every node for the `registry:2` and `busybox` images from Docker Hub. Application images come from the cluster's registry. |
| Disk space | Images and generated builds take tens of gigabytes on the head node. |

Check the cluster before you start:

```sh
kubectl get nodes                  # every node Ready
kubectl get storageclass           # nfs-client is listed
docker info > /dev/null && echo ok # Docker works on the head node
```

## Running the setup

1. Clone this repository on the head node. The script places two more repositories next to it, so
   choose the parent directory with that in mind:

   ```sh
   git clone <this repository> ~/blueprint-bridges
   cd ~/blueprint-bridges
   ```

2. Run the setup script:

   ```sh
   utils/setup_environment.sh
   ```

3. When it finishes, start a new shell so the changes take effect, and activate the Python
   environment:

   ```sh
   exec bash -l                 # picks up the Go PATH
   newgrp docker                # or log out and in: Docker without sudo
   source .venv/bin/activate    # run this in every shell you build or deploy from
   ```

The script is idempotent: running it again skips what is already done and re-applies the rest.

### What it does

| Stage | Action |
|---|---|
| 1 | Installs `git`, `curl`, `ca-certificates`, `python3-venv` and `python3-pip`. |
| 2 | Clones the bridges' collector ([docc-lab/opentelemetry-collector-contrib](https://github.com/docc-lab/opentelemetry-collector-contrib), branch `retctx-reject-impl`) and [DeathStarBench](https://github.com/delimitrou/DeathStarBench) with its submodules, next to this repository. An existing clone is left as it is. |
| 3 | Builds DeathStarBench's load generator, wrk2, and installs it as `/usr/local/bin/wrk`. |
| 4 | Runs [`install_blueprint_deps.sh`](../../utils/install_blueprint_deps.sh): the latest Go (in `/usr/local/go`, added to `PATH` in `~/.bashrc` and `~/.profile`), `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc` and the latest kompose. |
| 5 | Creates `.venv` in this repository with PyYAML and aiohttp. |
| 6 | Adds you to the `docker` group. |
| 7 | Deploys the image registry: namespace `registry`, a 20 GiB volume, and the service on NodePort 30000 ([`utils/registry-*.yaml`](../../utils)). |
| 8 | Pins the CPU frequency of every node to 2.2 GHz (see [CPU frequency](#cpu-frequency)). |

Stages 7 and 8 need `kubectl`; if the cluster cannot be reached, they are skipped with a message
that shows how to run them later.

Afterwards, the head node has this layout:

```
~/
├── blueprint-bridges/                 this repository
│   └── .venv/                         Python environment for the scripts
├── opentelemetry-collector-contrib/   the collector, with the bridges' priority processor
└── DeathStarBench/                    load generator and reference applications
```

### Options

Set these in the environment of `setup_environment.sh`:

| Variable | Default | Effect |
|---|---|---|
| `CPU_GHZ` | `2.2` | Target CPU frequency in stage 8. |
| `COLLECTOR_REPO` | `https://github.com/docc-lab/opentelemetry-collector-contrib.git` | Collector repository to clone. |
| `COLLECTOR_BRANCH` | `retctx-reject-impl` | Collector branch to clone. |
| `DSB_REPO` | `https://github.com/delimitrou/DeathStarBench.git` | DeathStarBench repository to clone. |

For example, `CPU_GHZ=2.0 utils/setup_environment.sh`.

## Verifying the setup

From a new shell with the environment active:

```sh
go version && protoc --version && kompose version   # toolchain
command -v wrk                                      # load generator
kubectl -n registry get pods                        # docker-registry is Running
curl -s http://10.10.1.1:30000/v2/_catalog          # the registry answers with JSON
utils/pin-cpu-22ghz.sh check --all                  # every node at the target frequency
```

## Building the collector image

Every deployment uses the collector image `otelcontribcol` from the registry. Build and push it once
before the first deployment, and again whenever the collector source changes:

```sh
cd ../opentelemetry-collector-contrib
./build-and-push.sh 10.10.1.1:30000
```

The deploy scripts can do the same with `--build-collector`.

## CPU frequency

Measurements are more repeatable when every node runs at one fixed clock.
[`utils/pin-cpu-22ghz.sh`](../../utils/pin-cpu-22ghz.sh) sets the `performance` governor and pins
the minimum and maximum frequency of every CPU to the target. With `--all` it does so on every
node, through a short-lived privileged pod per node.

```sh
utils/pin-cpu-22ghz.sh pin --all          # 2.2 GHz on every node
utils/pin-cpu-22ghz.sh pin 2.0 --all      # another target
utils/pin-cpu-22ghz.sh check --all        # show each node's state
utils/pin-cpu-22ghz.sh unpin --all        # back to the powersave governor
```

The setting does not survive a reboot: run `pin --all` again after any node restarts.
`utils/pin-cpu-22ghz.sh persist` installs a systemd unit that re-applies it at boot, on the node
where you run it.

## Securing the registry

The registry has no authentication, and a NodePort service listens on every address of every node.
On machines with a public interface, such as CloudLab nodes, anyone on the internet can then read
and overwrite your images. Allow port 30000 only from the cluster network. With kube-proxy in IPVS
mode (Kubespray's default), a rule on each node's public interface does it:

```sh
# on every node; replace eno1 with the node's public interface
sudo iptables -I INPUT 1 -i eno1 -p tcp --dport 30000 -j DROP
```

iptables rules do not survive a reboot. Re-apply the rule after nodes restart, or persist it with
your distribution's tools (for example `iptables-persistent`).

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `permission denied` from Docker | Your shell predates the `docker` group change. Run `newgrp docker` or log in again. |
| `go: command not found` | Your shell predates the `PATH` change. Start a new shell, or `source ~/.profile`. |
| `ModuleNotFoundError: No module named 'yaml'` | The Python environment is not active: `source .venv/bin/activate`. |
| Registry pod stays `Pending` | No `nfs-client` StorageClass, so its volume cannot be created. Install a provisioner, or change `storageClassName` in [`utils/registry-pvc.yaml`](../../utils/registry-pvc.yaml). |
| Pods fail with `ImagePullBackOff` and an `http: server gave HTTP response to HTTPS client` message | The node's container runtime does not accept the registry over HTTP. Add the insecure-registry setting on every node. |
| `pin-cpu-22ghz.sh` reports errors | The node has no cpufreq driver in `/sys/devices/system/cpu/cpu*/cpufreq`, or privileged pods are not allowed. |
