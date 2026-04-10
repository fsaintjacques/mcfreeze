# frostmap

Immutable key-value snapshots built from BigQuery, served at sub-millisecond
latency across thousands of nodes. Attach a disk — that's the deployment.

## How It Works

Frostmap separates compute from storage. A snapshot is built once onto a
Hyperdisk ML volume, attached read-only to every node in the fleet, and served
locally over the memcache meta protocol. No cache cluster, no replication, no
state to manage.

```
                    ┌───────────────────────────┐
                    │       Control Plane       │  Go · Deployment
                    │   - dataset registry      │
                    │   - version lifecycle     │
                    │   - rollout / rollback    │
                    └─────────────┬─────────────┘
                                  │ watched API
              ┌───────────────────┴───────────────────┐
              │                                       │
              ▼                                       ▼
   ┌─────────────────────┐        ┌────────────────────────────────────┐
   │   Snapshot Builder  │        │     DaemonSet Pod  (per node)      │
   │      Rust · Job     │        │                                    │
   │                     │        │  ┌──────────────────────────────┐  │
   │  - parallel BQ read │        │  │   node-agent                 │  │
   │  - write kv format  │        │  │   Go · privileged            │  │
   │                     │        │  │  - attach / detach disks     │  │
   └──────────┬──────────┘        │  │  - mount / umount            │  │
              │ provisions disk   │  │  - write catalog.json        │  │
              ▼                   │  └──────────────┬───────────────┘  │
   ┌─────────────────────┐        │                 │ shared EmptyDir  │
   │    Hyperdisk ML     │        │  ┌──────────────▼───────────────┐  │
   │    per version      │        │  │   KV Server                  │  │
   │                     │        │  │   Rust · unprivileged        │  │
   │    data/part-NN/    │        │  │  - mmap index.all            │  │
   │      data.bin       │        │  │  - pread data.bin            │  │
   │    index.all        │        │  │  - MGET over UDS + TCP       │  │
   │    meta.json        │        │  │  - atomic version swap       │  │
   └─────────────────────┘        │  └──────────────────────────────┘  │
                                  └────────────────────────────────────┘
```

## Quick Start

```bash
# Build
make all

# Load a snapshot from CSV (columns: key, value)
printf "key,value\nhello,world\nfoo,bar\n" | fm load --output /tmp/snap --partitions 4 csv

# Look up a key
fm get --snapshot /tmp/snap hello

# Serve it
fm serve snapshot --dir /tmp/snap --tcp 0.0.0.0:7777

# Query (memcache meta protocol)
echo -ne "mg hello v\r\n" | nc localhost 7777
```

## Kubernetes Deployment

Frostmap is Kubernetes-native. Users declare a `Dataset` CR and the platform
handles builds, disk provisioning, fleet-wide rollout, and version retirement.

```yaml
apiVersion: frostmap.dev/v1alpha1
kind: Dataset
metadata:
  name: users
spec:
  keyPrefix: users
  shardCount: 64
  retention: 2
  source:
    keyColumn: user_id
    encoding:
      protobuf:
        messageName: users.UserProfile
    bigquery:
      project: my-project
      table: my-project.prod.users
  trigger:
    cron:
      schedule: "0 2 * * *"
```

Deploy with Helm:

```bash
helm install frostmap k8s/charts/frostmap \
  --set controlPlane.storageClass=hyperdisk-ml \
  --set controlPlane.image=frostmap/fm:latest
```

Target datasets to specific nodes with `DatasetBinding`:

```yaml
apiVersion: frostmap.dev/v1alpha1
kind: DatasetBinding
metadata:
  name: gpu-nodes-users
spec:
  nodeSelector:
    matchLabels:
      pool: gpu
  datasetSelector:
    matchLabels:
      team: recommendations
```

## Documentation

| Document | Description |
|----------|-------------|
| [High-Level Architecture](doc/high-level.md) | System overview, components, data flow |
| [KV Snapshot Format](doc/format.md) | On-disk format: index, data, hashing, construction |
| [KV Server](doc/kv-server.md) | Memcache meta protocol, catalog hot-swap, observability |
| [Node Agent](doc/node-agent.md) | Disk attachment, mounting, reconciliation loop |
| [Control Plane](doc/control-plane.md) | Controllers, HTTP API, build system, assignment broker |
| [Kubernetes Resources](doc/kubernetes.md) | CRDs, RBAC, Helm chart, ownership model |

## Project Layout

```
rust/crates/
  frostmap-format/       on-disk snapshot format (reader + writer)
  frostmap-loader/       parallel scatter-gather build pipeline
  frostmap-bq/           BigQuery Storage Read API source adapter
  frostmap-encode/       Arrow → protobuf transcoding
  frostmap-server/       KV server: memcache meta protocol, catalog hot-swap
  frostmap-cli/          fm binary: load, get, serve

go/
  api/                   shared wire types (HTTP + catalog.json)
  api/v1alpha1/          Kubernetes CRD type definitions
  cmd/fmtctl/            single binary: control-plane, node-agent, job
  internal/
    controlplane/        HTTP server, assignment broker, builder orchestration
    controller/          Kubernetes reconcilers
    nodeagent/           agent loop, volume/mount subsystems

k8s/charts/frostmap/    Helm chart
```

## Building

```bash
make all           # release Rust + Go binaries
make test          # unit + integration tests
make test-kind     # e2e tests in KIND cluster
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
