# Orchestration: fmtctl

`fmtctl` is the Go-based orchestration layer for frostmap. It consists of three
components that together drive the full lifecycle of datasets and versions: from
triggering snapshot builds, through disk attachment on nodes, to serving
acknowledgement and version retirement.

The Rust side (`fm`, `frostmap-format`, `frostmap-loader`) owns the data plane —
writing and reading the on-disk format. `fmtctl` owns the control plane — deciding
*what* runs, *where*, and *when*.

```
┌──────────────────────────────────────────────────────────────┐
│                        fmtctl                                │
│                                                              │
│  ┌─────────────────┐   triggers   ┌──────────────────────┐  │
│  │  control-plane  │─────────────▶│        job           │  │
│  │   Deployment    │              │  Kubernetes Job      │  │
│  │                 │◀─────────────│                      │  │
│  │                 │   reports    └──────────────────────┘  │
│  │                 │                                         │
│  │                 │   watched    ┌──────────────────────┐  │
│  │                 │─────────────▶│    node-agent        │  │
│  │                 │◀─────────────│    DaemonSet         │  │
│  └─────────────────┘   ack/state  └──────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

---

## Components

### control-plane

**Binary:** `fmtctl control-plane`
**Deployment:** Kubernetes `Deployment` (single replica)
**State:** Kubernetes CRDs

The authoritative source of truth for all dataset and version state. Never
touches nodes or disks directly — it delegates all node-level work to
`node-agent` and all build work to `job`.

Responsibilities:
- Maintain a registry of `DatasetSpec` resources (name, source, shard count,
  retention policy) via Kubernetes CRDs
- Accept requests to produce a new version of a dataset; create a `job` and
  track its progress
- Promote a finished snapshot to `active` once the `job` reports completion
- Expose a watched HTTP API that `node-agent` instances poll for the active
  version of each dataset
- Collect per-node `NodeStatus` acknowledgements from `node-agent`
- Drive rollout: advance the active version across nodes as acknowledgements
  arrive
- Drive rollback: revert the active version to a previous `ready` snapshot on
  operator request or policy trigger
- Retire old versions once all nodes have acknowledged the new active version
  and the retention window has elapsed

### job

**Binary:** `fmtctl job`
**Deployment:** Kubernetes `Job` (run-to-completion), one per dataset version
**Shared library:** `frostmap-format` (via the `fm` snapshot builder binary)

Stateless. Created by the control-plane, reports completion, and exits.
The `job` component is the Go wrapper that configures and launches the Rust
`fm` snapshot builder as the main container, then reports results back to the
control-plane.

Responsibilities:
- Receive a `DatasetSpec` and a target `VersionRecord` from the control-plane
  (via Job environment / ConfigMap)
- Provision a Hyperdisk ML volume for the new version
- Run the `fm` snapshot builder container to populate the volume
- Monitor builder progress and surface logs / metrics
- On successful completion: report the finished snapshot (disk URL, shard
  count, `meta.json` checksum) to the control-plane
- On failure: report the error and allow the control-plane to decide whether
  to retry or mark the version as failed
- Clean up the provisioned disk if the build fails and no retry is scheduled

### node-agent

**Binary:** `fmtctl node-agent`
**Deployment:** Kubernetes `DaemonSet`, privileged container
**Communicates with:** control-plane API (outbound), KV server (shared EmptyDir)

> **Terminology note:** `node-agent` is the binary name for the component referred to as
> "Lifecycle Manager" in `HIGHLEVEL.md`. The names are interchangeable; this document uses
> the binary name to stay consistent with `fmtctl` subcommand conventions.

One instance per node. Performs all privileged OS and GCP operations required
to materialise a dataset version onto the local node.

Responsibilities:
- Poll the control-plane watched API for the active version of each dataset
- Call the Compute Engine API to attach the active version's Hyperdisk ML
  volume to the local node
- Wait for the block device to appear (`udevadm settle` / polling), then mount
  it read-only at `/mnt/kv/<dataset>/v<N>/`
- Write `catalog.json` atomically (via `rename(2)`) to the shared EmptyDir to
  signal the KV server that a new version is available
- Wait for the KV server to acknowledge the version swap (via the shared
  EmptyDir) — see `HIGHLEVEL.md § KV Server` for the full acknowledgement
  protocol; the KV server is a separate Rust component outside `fmtctl`
- Detach and unmount the previous version's disk after acknowledgement
- Report per-node `NodeStatus` back to the control-plane (active version,
  swap timestamp, error state)
- Handle node drain / shutdown: gracefully detach all attached disks

---

## State Machine

Each `VersionRecord` transitions through the following states:

```
  [building] ──(job succeeds)──▶ [ready] ──(control-plane promotes)──▶ [active]
      │                                                                     │
      └──(job fails)──▶ [failed]              (all nodes ack + retention)──▶ [retired]
                            │
                            └──(control-plane retries)──▶ [building]  (new VersionRecord)
```

> **Retry semantics:** a retry creates a fresh `VersionRecord` in `building` state rather
> than transitioning the failed record. The failed record is retained for audit purposes.

| State | Owner | Meaning |
|---|---|---|
| `building` | control-plane / job | Snapshot builder Job is running |
| `ready` | control-plane | Disk is written; not yet active on any node |
| `active` | control-plane | Currently being served; node-agents are attaching |
| `retired` | control-plane | Superseded; disk pending deletion |
| `failed` | control-plane / job | Build failed; disk may be incomplete |

---

## API Contract

The control-plane exposes a single long-poll HTTP endpoint consumed by
`node-agent` instances:

```
GET /api/v1/node/{node-name}/assignments
```

Returns the current active `VersionRecord` for each dataset assigned to this
node. `node-agent` uses a `?generation=N` query parameter to block until the
assignment changes (watched / long-poll pattern).

`node-agent` reports state back via:

```
POST /api/v1/node/{node-name}/status
Body: NodeStatus { dataset, active_version, state, timestamp }
```

---

## Shared Go Module

Both `control-plane` and `node-agent` import the `go/api` package which
contains the canonical type definitions:

| Type | Description |
|---|---|
| `DatasetSpec` | Dataset name, BQ source, shard count, retention |
| `VersionRecord` | Version ID, disk URL, state, build metadata |
| `CatalogEntry` | Per-dataset entry written to `catalog.json` |
| `NodeStatus` | Per-node version acknowledgement reported to control-plane |
| `NodeAssignment` | Active version assignment returned by the watched API |
