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
│  └─────────────────┘   NodeState  └──────────────────────┘  │
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
- Maintain a registry of `DatasetSpec` resources (name, key prefix, source,
  shard count, retention policy) via Kubernetes CRDs
- Accept requests to produce a new version of a dataset; create a `job` and
  track its progress
- On job completion: create a Kubernetes `PersistentVolume` referencing the
  finished disk, set `VersionRecord.PVName`, and promote the version to `ready`
- Promote a `ready` version to `active` and expose it via the watched API
- Expose a watched HTTP API that `node-agent` instances poll for active
  assignments; `NodeAssignment` carries the `PVName` so node-agents never need
  cloud credentials
- Collect periodic `NodeState` reports from `node-agent` instances; diff
  reported actual state against desired assignments to drive rollout
- Drive rollout: advance the active version across nodes as `NodeState` reports
  confirm convergence
- Drive rollback: revert the active version to a previous `ready` snapshot on
  operator request or policy trigger
- Retire old versions once all nodes have converged on the new active version
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
**Communicates with:** control-plane API (outbound), KV server HTTP (loopback)

One instance per node. Performs all privileged OS operations required to
materialise a dataset version onto the local node.

Responsibilities:
- Poll the control-plane watched API for active `NodeAssignment`s
- Create a Kubernetes `VolumeAttachment` referencing the assignment's
  `PVName`; the CSI driver handles the underlying cloud attach call — no
  cloud credentials are required in the pod
- Wait for `VolumeAttachment.status.attached == true` and read the device
  path from `status.attachmentMetadata["devicePath"]`
- Mount the block device read-only at `/mnt/kv/<dataset>/v<N>/`
- Write `catalog.json` atomically (via `rename(2)`) to the shared EmptyDir to
  signal the KV server that a new version is available; the file uses the
  `CatalogFile` envelope (`{"entries":[...]}`) and includes entries for ALL
  active datasets on the node, not just the one being promoted
- Poll `GET http://localhost:7777/version` until the KV server reports the
  new version as active (converging check — if the KV server crashes and
  restarts it will reload from `catalog.json` and the poll naturally
  resolves; if the file does not exist yet the server starts with an empty
  catalog and the poll resolves once the node-agent writes the first catalog)
- Unmount and detach the previous version's disk immediately after the KV
  server confirms the new version — this is a local operation that does not
  require control-plane coordination; freeing the disk slot promptly allows
  the node to accept new attachments
- Periodically report the full `NodeState` (all datasets, phases, versions) to
  the control-plane; this is a level-triggered converging report — a missed
  report never causes permanent divergence
- Handle node drain / shutdown: gracefully detach all attached disks

#### Node-agent phase lifecycle (per dataset)

```
[attaching] ──(VolumeAttachment ready)──▶ [mounting] ──(mount + catalog.json)──▶ [active]
     │                                                                                │
     └──(error)──▶ [error] ──(next reconcile cycle)──▶ [attaching]                  │
                                         (new version assigned)──▶ [unmounting] ─────┘
```

| Phase | Meaning |
|---|---|
| `attaching` | VolumeAttachment created; waiting for CSI driver to attach the disk |
| `mounting` | Block device present; mount syscall in progress |
| `active` | Mounted, catalog.json written, KV server confirmed via `/version` |
| `unmounting` | Previous version being unmounted and disk detached |
| `error` | An operation failed; full error in `NodeState.DatasetState.Error` |

---

## State Machine

Each `VersionRecord` transitions through the following states:

```
  [building] ──(job succeeds)──▶ [ready] ──(control-plane promotes)──▶ [active]
      │                              │                                      │
      └──(job fails)──▶ [failed]     └──(PV created, PVName set)           │
                            │                                   (all nodes converged
                            └──(retry)──▶ [building]             + retention)──▶ [retired]
```

> **Retry semantics:** a retry creates a fresh `VersionRecord` in `building`
> state rather than transitioning the failed record.

| State | Owner | Meaning |
|---|---|---|
| `building` | control-plane / job | Snapshot builder Job is running |
| `ready` | control-plane | Disk written; PV created; `PVName` set; not yet active on any node |
| `active` | control-plane | Currently being served; node-agents are attaching |
| `retired` | control-plane | Superseded; disk and PV pending deletion |
| `failed` | control-plane / job | Build failed; disk may be incomplete |

---

## API Contract

### control-plane → node-agent (watched)

```
GET /api/v1/node/{node-name}/assignments
```

Returns the current active `NodeAssignment` for each dataset assigned to this
node.  `node-agent` uses a `?generation=N` query parameter to block until the
assignment changes (long-poll).  Each `NodeAssignment` includes the
`PVName` so node-agents need no cloud credentials.

### node-agent → control-plane (converging state report)

```
POST /api/v1/node/{node-name}/state
Body: NodeState { node, datasets: []DatasetState, reported_at }
```

Full state of all datasets on the node.  Reported periodically (default 30 s)
and on every assignment change.  The control-plane diffs this against desired
assignments to drive rollout.  A missed report is harmless — the next one
self-heals.

### node-agent → KV server (version confirmation)

```
GET http://localhost:7777/version
Response: KVVersionResponse { datasets: []KVDatasetVersion }
```

The node-agent polls this endpoint after writing `catalog.json` and waits until
the reported `version_id` for the dataset matches the desired version.  If the
KV server is unreachable the call fails, which is itself a signal that the swap
is not yet complete.  If the KV server crashes and restarts it reloads from
`catalog.json` and the poll resolves naturally — no stale state, no cleanup.
If `catalog.json` does not exist at server startup (pod startup race), the
server starts with an empty catalog and returns an empty `datasets` array;
the first `catalog.json` write triggers the initial load via the filesystem
watcher.

---

## Shared Go Module

Both `control-plane` and `node-agent` import the `go/api` package which
contains the canonical type definitions:

| Type | Description |
|---|---|
| `DatasetSpec` | Dataset name, key prefix, BQ source, shard count, retention |
| `VersionRecord` | Version ID, disk URL, PV name, state, build metadata |
| `CatalogFile` | Top-level `catalog.json` document: `{"entries":[...]}` |
| `CatalogEntry` | Per-dataset entry in `CatalogFile`; includes key prefix |
| `NodeAssignment` | Active version assignment returned by the watched API; includes PV name and key prefix |
| `NodeState` | Full per-node state report: all datasets, phases, versions |
| `DatasetState` | Phase, version, key prefix, PV name, mount path, error for one dataset on one node |
| `DatasetPhase` | Node-local lifecycle phase: attaching / mounting / active / unmounting / error |
| `KVVersionResponse` | Response from `GET /version` on the KV server |
| `KVDatasetVersion` | Per-dataset version entry in `KVVersionResponse` |

## Go module structure

```
go/
  go.mod                          module frostmap.io/fmtctl
  api/types.go                    shared wire types
  internal/
    volume/
      volume.go                   VolumeManager interface (AttachDisk, WaitForDevice, DetachDisk)
      k8s.go                      ComputeDiskManager — Kubernetes VolumeAttachment via CSI
      fs.go                       FSVolumeManager — filesystem simulation for local dev / tests
      fake.go                     FakeVolumeManager — in-memory fake for unit tests
    mount/
      mount.go                    Mounter interface (Mount, Unmount)
      mount_linux.go              LinuxMounter — syscall.Mount
      mount_stub.go               stub for non-Linux builds
      fs.go                       FSMounter — symlink-based for local dev / tests
      fake.go                     FakeMounter — in-memory fake for unit tests
    nodeagent/
      agent.go                    Agent struct, reconcile loop, catalog write, old-version cleanup
      interfaces.go               AssignmentSource, StateReporter, VersionChecker interfaces
      http.go                     HTTP client implementations of the three interfaces
      fake.go                     In-memory fakes with call recording for unit tests
    testutil/
      snapshot.go                 BuildSnapshot helper — shells out to fm load csv
      server.go                   StartCatalogServer — launches fm serve catalog on free ports
  cmd/
    node-agent/
      main.go                     flags, signal handling, dependency wiring
```
