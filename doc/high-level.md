# High-Level Architecture

## Product Goal

The system's success is measured by a single outcome: **a team can export a
massive BigQuery table and start serving it at high volume, low latency, across
multiple regions — with minimal operational effort.**

Concretely: draft a BigQuery table in a `(key, value)` schema, run a job, and
within minutes that dataset is queryable at sub-millisecond latency from any
node in any region. No cache cluster to deploy, no statefulset to manage, no
replication topology to reason about. The data plane is a shared network disk
— attaching it to a node *is* the deployment.

This is deliberately a separation of compute and storage. The KV server is a
thin serving layer: it holds no data, owns no state, and can be restarted
freely. All durability lives in the snapshot on the Hyperdisk ML volume. The
operational surface shrinks to:

1. **Build** — push a BigQuery query, get back a versioned snapshot on disk.
2. **Attach** — mount the snapshot read-only on every node in the fleet.
3. **Serve** — answer MGET requests directly from the local mmap; no network
   hop to a remote cache.

The cheapness comes from the storage model: a single Hyperdisk ML volume can
be attached to up to 2,500 nodes simultaneously, so the marginal cost of
adding readers is zero. The simplicity comes from immutability: snapshots are
write-once, so there are no consistency protocols, no write conflicts, and no
split-brain scenarios.

---

## Problem Statement

Standard Kubernetes storage is designed for persistent, long-lived volumes attached to
Pods at boot time. This system addresses workloads that require:

- **Massive fan-out:** a single dataset readable by hundreds of nodes simultaneously
- **Zero-downtime updates:** swap multi-TB datasets without restarting Pods
- **High-throughput reads:** sub-millisecond key-value lookups served directly from
  locally-attached disk, bypassing network round-trips to a remote cache cluster
- **Stateless compute:** Pods consume the current version of a dataset without owning
  specific disks

---

## Solution Overview

Data lifecycle is decoupled from Pod lifecycle. Datasets are written once as static
snapshots onto Hyperdisk ML volumes, attached read-only to GKE nodes, and served
locally by a per-node process over a Unix domain socket and TCP port.

```
                    ┌───────────────────────────┐
                    │       Control Plane       │  Go · Deployment
                    │  - dataset registry       │
                    │  - version lifecycle      │
                    │  - rollout / rollback     │
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

---

## Components

### Control Plane

**Language:** Go
**Deployment:** Kubernetes Deployment (single replica, leader-elected for HA)
**State:** Kubernetes CRDs (`Dataset`, `DatasetVersion`, `DatasetBinding`)

The control plane is the authoritative source of truth for dataset and version state.
It never touches nodes or disks directly.

Responsibilities:
- Maintain a registry of datasets via `Dataset` CRs
- Auto-create `DatasetVersion` CRs on cron schedule or manual trigger
- Trigger snapshot builder Jobs when a new `DatasetVersion` appears
- On Job completion: create a `PersistentVolume` referencing the finished disk,
  promote the version to `ready`, then auto-promote to `active`
- Compute per-node assignments from `DatasetBinding` selectors and expose them
  via a long-poll HTTP API
- Collect periodic `NodeState` reports from node-agents; diff reported actual
  state against desired assignments to track rollout convergence
- Retire old versions once all nodes have converged away and the retention
  policy is satisfied; delete the PV (CSI driver deletes the cloud disk)

See [control-plane.md](control-plane.md) for details.

### Node Agent

**Language:** Go
**Deployment:** DaemonSet, privileged container
**Communicates with:** Control plane API (outbound), KV server HTTP (loopback)

One instance per node. Performs all privileged OS operations required to
materialise a dataset version.  Uses the Kubernetes `VolumeAttachment` API for
disk attachment so it requires no cloud credentials — the CSI driver handles the
underlying cloud call.

Responsibilities:
- Watch the control plane API for active `NodeAssignment`s via long-poll;
  each assignment carries a `PVName` (set by the control plane when the version
  becomes ready)
- Create a `VolumeAttachment` for the PV; wait for
  `status.attached == true` and read the device path from
  `status.attachmentMetadata["devicePath"]`
- Mount the block device read-only at `/mnt/kv/<dataset>/v<N>/`
- Write `catalog.json` atomically (via `rename(2)`) to a shared EmptyDir;
  `catalog.json` includes entries for ALL active datasets on the node
- Poll the KV server's HTTP `/version` endpoint until it reports the new
  version as active
- Detach and unmount the previous version's disk after the KV server confirms
- Periodically POST the full `NodeState` to the control plane — level-triggered,
  so a missed report self-heals

See [node-agent.md](node-agent.md) for details.

### KV Server

**Language:** Rust
**Deployment:** DaemonSet, unprivileged container
**Communicates with:** Clients (UDS + TCP), node-agent (shared EmptyDir + HTTP)

One instance per node. The latency-critical serving path. Never performs privileged
operations.

Responsibilities:
- Watch `catalog.json` via inotify for version changes
- On version change: `mmap` `index.all` with `madvise(MADV_RANDOM)` and
  `MADV_HUGEPAGE` (2 MiB aligned partitions), open `data.bin` file descriptors
- Serve requests over the memcache meta protocol:
  - Unix domain socket: `/run/kv/kv.sock` (same-node clients, lowest latency)
  - TCP port `7777` (off-node access and tooling)
- Route each key by prefix `<key_prefix>:<key>` (from `catalog.json`), then
  `xxhash64(key) & (N-1)` to the correct partition
- Perform Robin Hood index probe; on hit, `pread` the value from `data.bin`
- Swap versions atomically using RCU: in-flight requests complete against the old
  mmap before it is released
- Expose `GET /version` returning the currently loaded version per dataset;
  node-agent polls this to confirm the swap before detaching the old disk

See [kv-server.md](kv-server.md) for details.

### Snapshot Builder

**Language:** Rust
**Deployment:** Kubernetes Job (runs to completion)
**Shared library:** `mcfreeze-format`

Stateless. Triggered by the control plane, reports completion and exits.

Responsibilities:
- Accept a dataset source configuration (BigQuery table or CSV)
- Open a BigQuery Storage Read API session; fan out across N parallel gRPC streams
  using Tokio tasks
- Route each `(key, value)` pair to one of N partition writers by
  `xxhash64(key) & (N-1)`; partition writers run concurrently with no shared state
- Each partition writer:
  - Appends values to `data.bin` at 64-byte aligned offsets (12-byte header +
    value + padding per entry)
  - Spills `(compact_fingerprint, aligned_offset)` records to `spill.bin`
- After all data is written, build Robin Hood tables from spill files and
  concatenate into a single `index.all` (8-byte buckets, 2 MiB aligned
  partitions, SIMD-probed in groups of 8)
- Write `meta.json` last as the atomic completion signal

See [format.md](format.md) for the on-disk format specification.

---

## Kubernetes Resources

Three custom resources define the declarative interface:

| CRD | Scope | Owner | Purpose |
|---|---|---|---|
| `Dataset` | Namespaced | User | Desired dataset: key prefix, source, shard count, trigger, retention |
| `DatasetVersion` | Namespaced | Controller | One per snapshot: tracks build state (`building` → `ready` → `active` → `retired`) |
| `DatasetBinding` | Namespaced | User | Selects which datasets are served on which nodes via label selectors |

See [kubernetes.md](kubernetes.md) for CRD schemas, ownership, and garbage collection.

---

## Shared Code

### `go/api`

Go module imported by the control plane and the node-agent. Contains the wire
types for the HTTP APIs and `catalog.json`:

| Type | Description |
|---|---|
| `DatasetSpec` | Dataset name, key prefix, source config, shard count, retention |
| `VersionRecord` | Version ID, disk URL, PV name, state |
| `CatalogFile` / `CatalogEntry` | `catalog.json` schema: per-dataset mount path, key prefix, version |
| `NodeAssignment` | Active version assignment from the watched API; includes PV name |
| `NodeState` / `DatasetState` | Full per-node state report: all datasets, phases, versions |
| `KVVersionResponse` | Response from `GET /version` on the KV server |

### `rust/crates/mcfreeze-format`

Rust library crate that is the single implementation of the on-disk format
(currently format version 4). Compiled into both the snapshot builder (write
path) and the KV server (read path).

| Module | Description |
|---|---|
| `index` | Robin Hood table: 8-byte bucket layout, SIMD group probing, insertion |
| `data` | 64-byte aligned values with 12-byte headers (verify fingerprint + length) |
| `meta` | `meta.json` v4 serialisation: per-partition offsets into `index.all` |
| `reader` | `SnapshotReader`: mmap index + pread data path |
| `writer` | `SnapshotWriter`: concurrent partition writers |
| `spill` | Disk-backed spill for index construction when memory is constrained |

---

## Data Flow

```
1. User creates a Dataset CR (or cron fires)
   → control plane creates a DatasetVersion CR in "building" state

2. Snapshot builder (Kubernetes Job):
   a. Opens N parallel BQ Storage streams
   b. Writes N × data.bin + spill.bin to the provisioned Hyperdisk ML volume
   c. Builds index.all from spill files (Robin Hood tables, 8-byte buckets)
   d. Writes meta.json; Job completes

3. Control plane transitions version to "ready":
   a. Finalizes the build disk (CSI driver converts to read-only multi-attach)
   b. Creates a Kubernetes PersistentVolume referencing the disk
   c. Sets DatasetVersion.status.pvName
   d. Auto-promotes to "active"; surfaces via the long-poll API

4. Node agent (on each assigned node):
   a. Detects new active NodeAssignment via long-poll
   b. Creates a VolumeAttachment for the PV; CSI driver attaches the disk
   c. Waits for VolumeAttachment.status.attached == true
   d. Mounts the block device read-only at /mnt/kv/<dataset>/v<N>/
   e. Writes catalog.json atomically (all active datasets on the node)

5. KV server (on each node):
   a. Detects catalog.json change via inotify
   b. mmaps index.all, opens new data.bin fds
   c. Atomically swaps the active dataset handle (RCU)
   d. Updates GET /version response to reflect new version

6. Node agent polls GET /version until version matches;
   then unmounts the old disk and deletes its VolumeAttachment.
   POSTs NodeState to the control plane to confirm convergence.

7. Clients query via memcache meta protocol on /run/kv/kv.sock or :7777
   Dataset is selected by key prefix: "<key_prefix>:<key>"
```

---

## Project Layout

```
rust/crates/
  mcfreeze-format/       on-disk snapshot format (reader + writer)
  mcfreeze-loader/       parallel scatter-gather build pipeline
  mcfreeze-bq/           BigQuery Storage Read API source adapter
  mcfreeze-encode/       Arrow → protobuf transcoding
  mcfreeze-server/       KV server: memcache meta protocol, catalog hot-swap
  mcfreeze-cli/          mcf binary: load, get, serve subcommands

go/
  api/                   shared wire types (HTTP + catalog.json)
  api/v1alpha1/          Kubernetes CRD type definitions
  cmd/mcfctl/            single binary: control-plane, node-agent, job subcommands
  internal/
    controlplane/        HTTP server, assignment broker, builder orchestration
    controller/          Kubernetes reconcilers (Dataset, DatasetVersion, DatasetBinding)
    nodeagent/           agent loop, volume/mount/assignment/version subsystems

k8s/charts/mcfreeze/    Helm chart (CRDs, RBAC, Deployment, DaemonSet, StorageClass)
docker/                 Dockerfile (mcf + mcfctl in a single image)
```

---

## Infrastructure Requirements

| Requirement | Detail |
|---|---|
| Node machine types | N2, C3, or A3 (required for Hyperdisk ML) |
| Disk type | Hyperdisk ML, max 64 TiB per disk |
| Max concurrent readers | 2,500 (<=256 GiB), 300 (<=2 TiB), 30 (>16 TiB) |
| GKE features | Workload Identity, privileged DaemonSet |
| IAM | `compute.instances.attachDisk`, `storage.objectViewer`, BQ read |
| DaemonSet privileges | `privileged: true`, `mountPropagation: Bidirectional` |
