# Control Plane

## Overview

`fmtctl control-plane` is the authoritative source of truth for dataset and
version state. It runs as a single-replica Kubernetes Deployment with
leader-elected HA. It never touches nodes or disks directly — it delegates
node-level work to the node-agent and build work to Kubernetes Jobs.

The control plane has two halves:

1. **Kubernetes controllers** — reconcile `Dataset`, `DatasetVersion`, and
   `DatasetBinding` CRDs. Drive the version state machine
   (building → ready → active → retired) and enforce retention.
2. **HTTP server** — exposes a long-poll API for node-agent assignments and
   state reporting, plus a build trigger endpoint.

Both halves communicate through the **AssignmentBroker**, an in-memory store
that maps nodes to their desired assignments and tracks reported state.

---

## Configuration

```
fmtctl control-plane \
  --listen :8080 \
  --namespace frostmap-system \
  --image frostmap/fm:latest \
  --storage-class hyperdisk-ml \
  --disk-size-gb 100 \
  --builder-pod-template '{"serviceAccountName":"fm-builder"}' \
  --build-timeout 30m
```

| Flag | Default | Description |
|---|---|---|
| `--listen` | `:8080` | HTTP server bind address |
| `--namespace` | `default` / `$NAMESPACE` | Kubernetes namespace for CRDs and Jobs |
| `--image` | *(required)* | Container image for builder Jobs |
| `--image-pull-policy` | `IfNotPresent` | Image pull policy |
| `--storage-class` | *(required)* | StorageClass for build PVCs |
| `--disk-size-gb` | `10` | PVC size in GiB |
| `--builder-pod-template` | `{}` | JSON `BuilderPodTemplate` (see below) |
| `--build-timeout` | `30m` | Max build duration before cancellation |
| `--leader-elect` | `true` | Leader election via Lease objects |
| `--metrics-bind-address` | `:8081` | Controller-runtime metrics |
| `--health-probe-bind-address` | `:8082` | Health probes |

### BuilderPodTemplate

Scheduling and identity overrides for every builder Job's pod spec:

```json
{
  "serviceAccountName": "fm-builder",
  "tolerations": [{"key": "dedicated", "value": "build", "effect": "NoSchedule"}],
  "nodeSelector": {"pool": "build"},
  "affinity": null
}
```

---

## HTTP API

### `GET /api/v1/node/{node}/assignments`

Long-poll endpoint. Node-agents call this to receive their dataset assignments.

Query parameter: `generation` (int64) — the client's last-seen generation.
If the broker's generation for this node is greater, it responds immediately.
Otherwise the request blocks until assignments change (per-node notify channel).

Response:
```json
{
  "generation": 42,
  "assignments": [
    {
      "dataset": "users",
      "key_prefix": "users",
      "version": {
        "id": "v42",
        "pv_name": "pv-users-v42",
        "shard_count": 64,
        "descriptor": "<base64>",
        "message_name": "users.UserProfile"
      }
    }
  ]
}
```

Nodes are auto-registered on first poll.

### `POST /api/v1/node/{node}/state`

Node-agent state report. Body: `NodeState` JSON (max 1 MiB). The broker stores
the report and notifies the NodeAssignment reconciler so it can update rollout
status.

Response: `200 OK` (no body).

### `POST /api/v1/dataset/{name}/build`

Trigger a new build. Creates a `DatasetVersion` CR.

Request:
```json
{
  "spec": { "...DatasetSpec..." },
  "version_id": "v43"
}
```

Response: `202 Accepted` with `{"version_id": "v43"}`.
Returns `409 Conflict` if a build is already in progress.

### `POST /admin/node/{node}/assignments`

Admin override for testing. Directly sets a node's assignments in the broker.

---

## Assignment Broker

The broker is the in-memory bridge between controllers (which set desired
assignments) and the HTTP server (which delivers them to node-agents).

**Per-node state:**
- `assignments` — current `[]NodeAssignment`
- `generation` — monotonic counter, bumped only when assignments actually change
- `notify` — channel closed on generation bump to wake blocked long-polls
- `nodeState` — latest reported `NodeState`

**Diff-on-set:** `SetAssignments` compares the new value against the previous
one with `DeepEqual`. Identical content (e.g. periodic reconciler resyncs) does
not bump generation or wake long-polls.

**`IsDrained(dataset, versionID)`** — returns true when every registered node
has reported state at least once AND none reports the given version. Used by
the retirement reconciler to gate PV deletion.

---

## Controllers

### Dataset Reconciler

Watches `Dataset` CRs. Responsible for:

- **Auto-create versions.** If no version exists in `building`, `ready`,
  `active`, or `failed` state, creates a new `DatasetVersion` CR. This ensures
  applying a `Dataset` always kicks off a build.
- **Cron sync.** Registers or updates the dataset's cron schedule with the
  `CronRunnable`.
- **Retention enforcement.** When retired versions exceed `spec.retention`,
  deletes the oldest.
- **Status aggregation.** Patches `Dataset.status.activeVersion` from child
  `DatasetVersion` CRs.

### DatasetVersion Reconciler (Build Lifecycle)

Watches `DatasetVersion` CRs. Drives the version state machine:

```
(CR created)
    │
    ▼
[building] ──(Job succeeds)──▶ [ready] ──(auto-promote)──▶ [active]
    │                                                          │
    │──(Job fails)──▶ [failed]              (new version)──────┘
    │──(timeout)─────▶ [failed]                                │
                                                               ▼
                                                          [retired]
                                                               │
                                                    (drained + PV deleted)
                                                               │
                                                          (CR deleted)
```

**building:** Starts a builder Job (or reattaches to an existing one via
deterministic naming). Polls every 5 seconds. On completion: finalizes the
build disk via `DiskManager.FinalizeBuild`, reads the protobuf descriptor from
`meta.json`, and transitions to `ready`. On timeout (default 30 min) or
failure: cancels the Job and transitions to `failed`.

**ready → active:** Auto-promotes immediately. If another version is currently
`active`, demotes it to `retired` first. Only one version per dataset can be
active at a time.

**retired:** Watches `VolumeAttachment` objects. Once no VolumeAttachment
references the version's PV, calls `DiskManager.DeletePV` and deletes the
`DatasetVersion` CR. Safety-net requeue every 30 seconds.

**failed:** Idempotent cleanup of builder resources (Job, ConfigMap, PVC).
Terminal state — no requeue. Manual CR deletion triggers `ensureVersion` in
the Dataset reconciler, which creates a fresh build.

### Node Assignment Reconciler

Bridges CRD state to the node-agent HTTP protocol:

1. Lists all `active` `DatasetVersion` CRs and joins with parent `Dataset`
   to get `KeyPrefix` and labels.
2. For each registered node in the broker:
   - Reads the Kubernetes `Node` object for its labels.
   - Evaluates `DatasetBinding` selectors to determine which datasets the
     node should serve:
     - **No binding matches the node** → open-world default: all datasets.
     - **Bindings match** → union of datasets selected by those bindings.
   - Calls `Broker.SetAssignments(node, filtered)`.
3. On state report (via HTTP callback): patches
   `DatasetVersion.status.rollout` with aggregated convergence counts
   (total, converged, pending, error).

**Watches:** `DatasetVersion` (primary), `DatasetBinding` (enqueue all active
versions), Kubernetes `Node` with label-change predicate (enqueue all active
versions).

### Cron Trigger

The `CronRunnable` maintains an in-process cron scheduler (`robfig/cron/v3`).
It is leader-elected — only the leader fires triggers.

- **Sync:** called by the Dataset reconciler on every reconcile. Adds, updates,
  or removes the cron entry based on `spec.trigger.cron.schedule`.
- **Fire:** creates a `DatasetVersion` CR with version ID
  `YYYYMMDD-HHMMSS` (UTC). Skips if a sibling is already `building`.
- **Catch-up:** on startup, fires once per dataset with a cron trigger
  (staggered 1s apart to avoid thundering herd).

---

## Build System

### Builder Interface

```go
type Async interface {
    Start(ctx, spec, versionID) (Handle, error)
    Poll(ctx, handle)           (Status, error)
    Cancel(ctx, handle)         error
}
```

`Handle` is an opaque string (Job name for K8s, directory path for fork).
`Status.Phase` is one of: `running`, `complete`, `failed`, `not_found`.

### Kubernetes Job Builder (production)

Creates three objects per build, all with deterministic names
(`fm-build-`, `fm-config-`, `fm-pvc-` + dataset + version):

1. **PVC** — `ReadWriteOnce`, referencing the configured StorageClass.
2. **ConfigMap** — contains `worker.json` with source spec, output path, and
   partition count.
3. **Job** — runs `fmtctl job --config /config/worker.json`, mounts the PVC
   at `/output` and the ConfigMap at `/config`. `backoffLimit: 0`,
   `restartPolicy: Never`.

On Job completion, the reconciler calls `DiskManager.FinalizeBuild(pvcName)`
to convert the build disk to a read-only multi-attach PV. The resulting PV
name is recorded as a Job annotation for idempotency (crash-safe).

On failure or cancellation, all three objects are deleted (best-effort).

### Fork Builder (development)

Forks `fm load config` as a subprocess. Output goes to
`<output-base>/<dataset>/<version>/`. Detects completion by checking for
`meta.json`. Used for local development and integration tests.

---

## Disk Lifecycle

The control plane never calls cloud APIs directly. All disk operations flow
through Kubernetes storage primitives (PVC, PV, StorageClass). The CSI driver
handles the underlying cloud calls.

### DiskManager Interface

```go
type Manager interface {
    CreateBuildPVC(ctx, name, storageClass string, sizeGB int64) error
    FinalizeBuild(ctx, pvcName string) (pvName string, err error)
    DeletePV(ctx, pvName string) error
}
```

### FinalizeBuild Flow

Transitions a build-phase RWO PVC into a read-only multi-attach PV:

1. Wait for PVC to bind (poll 500ms, timeout 2 min).
2. Patch PV `reclaimPolicy` to `Retain` (prevents CSI from deleting the disk
   when the PVC is deleted).
3. Delete the PVC.
4. Wait for PVC deletion (poll 500ms, timeout 30s).
5. Patch PV: clear `claimRef`, set `accessModes` to `ReadOnlyMany`.
6. Return the PV name.

For Hyperdisk ML, this triggers the CSI driver's disk mode conversion from
`RW_SINGLE` to `RO_MANY`.

---

## Component Wiring

```
controller-manager (leader-elected)
│
├── DatasetReconciler
│     watches: Dataset
│     owns: CronRunnable (sync/forget)
│
├── DatasetVersionReconciler
│     watches: DatasetVersion, VolumeAttachment
│     uses: Builder (Job), DiskManager
│
├── NodeAssignmentReconciler
│     watches: DatasetVersion, DatasetBinding, Node
│     uses: AssignmentBroker (read/write)
│     callback: OnStateReport (from HTTP server)
│
├── CronRunnable (leader-elected)
│     robfig/cron scheduler
│     creates: DatasetVersion CRs
│
└── HTTP Server (leader-elected)
      AssignmentBroker (read)
      routes: /assignments, /state, /build, /admin
```
