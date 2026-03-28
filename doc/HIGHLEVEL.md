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
   │  data/part-NN/      │        │  │  - mmap index.idx            │  │
   │    index.idx        │        │  │  - pread data.bin            │  │
   │    data.bin         │        │  │  - MGET over UDS + TCP       │  │
   │  meta.json          │        │  │  - atomic version swap       │  │
   └─────────────────────┘        │  └──────────────────────────────┘  │
                                  └────────────────────────────────────┘
```

---

## Components

### Control Plane

**Language:** Go
**Deployment:** Kubernetes Deployment
**State:** Kubernetes CRDs

The control plane is the authoritative source of truth for dataset and version state.
It never touches nodes or disks directly.

Responsibilities:
- Maintain a registry of datasets and their configurations (`DatasetSpec`)
- Trigger snapshot builder Jobs when a new version is needed
- Monitor Job completion and promote finished snapshots to active
- Track per-node acknowledgement of the active version (`NodeStatus`)
- Drive rollout (advance active version) and rollback (revert to previous)
- Expose a watched API that node-agents poll for version changes

### node-agent

**Language:** Go
**Deployment:** DaemonSet, privileged container
**Communicates with:** Control plane API (outbound), KV server (shared EmptyDir)

One instance per node. Performs all privileged OS and GCP operations.

Responsibilities:
- Watch the control plane API for the active version of each dataset
- Call the Compute Engine API to attach Hyperdisk ML volumes to the local node
- Wait for block devices to appear, then mount them read-only at
  `/mnt/kv/<dataset>/v<N>/`
- Write `catalog.json` atomically (via `rename(2)`) to a shared EmptyDir when a
  new version is mounted and ready
- Detach and unmount the previous version's disk after the KV server acknowledges
  the swap
- Report per-node version state back to the control plane

### KV Server

**Language:** Rust
**Deployment:** DaemonSet, unprivileged container
**Communicates with:** Clients (UDS + TCP), lifecycle manager (shared EmptyDir)

One instance per node. The latency-critical serving path. Never performs privileged
operations.

Responsibilities:
- Watch `catalog.json` via inotify for version changes
- On version change: `mmap` the new `index.idx` partitions with
  `madvise(MADV_RANDOM)`, open `data.bin` file descriptors
- Serve MGET requests over:
  - Unix domain socket: `/run/kv/kv.sock` (same-node clients, lowest latency)
  - TCP port `7777` (off-node access and tooling)
- Route each key by prefix `<dataset>:<key>`, then `xxhash64(key) & (N-1)` to
  the correct partition
- Perform Robin Hood index probe; on hit, `pread` the value from `data.bin`
- Swap versions atomically using RCU: in-flight requests complete against the old
  mmap before it is released
- Acknowledge version swap to the lifecycle manager via the shared EmptyDir

### Snapshot Builder

**Language:** Rust
**Deployment:** Kubernetes Job (runs to completion)
**Shared library:** `frostmap-format`

Stateless. Triggered by the control plane, reports completion and exits.

Responsibilities:
- Accept a `DatasetSpec` describing the source and target disk
- Open a BigQuery Storage Read API session; fan out across N parallel gRPC streams
  using Tokio tasks
- Route each `(key, value)` pair to one of N partition writers by
  `xxhash64(key) & (N-1)`; partition writers run concurrently with no shared state
- Each partition writer:
  - Appends values to `data.bin` at 64-byte aligned offsets
  - Accumulates `(fingerprint, aligned_offset, size)` tuples in memory
- After all data is written, build `index.idx` per partition using Robin Hood
  insertion (parallelised across partitions via rayon)
- Write `meta.json` last as the atomic completion signal
- Report the finished snapshot to the control plane

---

## Shared Modules

### `go/api`

Go module imported by both the control plane and the lifecycle manager. Contains
the canonical type definitions for the system's wire contracts:

| Type | Description |
|---|---|
| `DatasetSpec` | Dataset name, source config, shard count, retention |
| `VersionRecord` | Version ID, disk URL, state (building / ready / active / retired) |
| `CatalogEntry` | Per-dataset entry written to `catalog.json` |
| `NodeStatus` | Per-node version acknowledgement reported to the control plane |
| `NodeAssignment` | Active version assignment returned by the control plane watched API |

### `rust/crates/frostmap-format`

Rust library crate that is the single implementation of the on-disk format described
in `FORMAT.md`. Compiled into both the snapshot builder (write path) and the KV
server (read path).

| Module | Description |
|---|---|
| `index` | Robin Hood table: bucket layout, insertion, probe |
| `data` | 64-byte aligned value writes and `pread` reads |
| `meta` | `meta.json` serialisation and deserialisation |

---

## Data Flow

```
1. Control plane triggers a snapshot builder Job for dataset D, version V

2. Snapshot builder:
   a. Opens N parallel BQ Storage streams
   b. Writes N × (index.idx, data.bin) to the provisioned Hyperdisk ML volume
   c. Writes meta.json
   d. Reports completion to control plane

3. Control plane marks version V as active

4. Lifecycle manager (on each node):
   a. Detects new active version via control plane watch
   b. Attaches the Hyperdisk ML volume to the node
   c. Mounts it read-only at /mnt/kv/D/vV/
   d. Writes catalog.json atomically

5. KV server (on each node):
   a. Detects catalog.json change via inotify
   b. mmaps new index partitions, opens new data.bin fds
   c. Atomically swaps the active dataset handle (RCU)
   d. Acknowledges swap; lifecycle manager detaches the old disk

6. Clients query via MGET on /run/kv/kv.sock or :7777
   Dataset is selected by key prefix: "<dataset>:<key>"
```

---

## Infrastructure Requirements

| Requirement | Detail |
|---|---|
| Node machine types | N2, C3, or A3 (required for Hyperdisk ML) |
| Disk type | Hyperdisk ML, max 64 TiB per disk |
| Max concurrent readers | 2,500 (≤256 GiB), 300 (≤2 TiB), 30 (>16 TiB) |
| GKE features | Workload Identity, privileged DaemonSet |
| IAM | `compute.instances.attachDisk`, `storage.objectViewer`, BQ read |
| DaemonSet privileges | `privileged: true`, `mountPropagation: Bidirectional` |
