# PRD: Kubernetes-Native Dataset Lifecycle

**Status:** Draft
**Date:** 2026-04-04

---

## Executive Summary

Frostmap today has the core data plane (snapshot format, builder, KV server) and
a working orchestration prototype (in-memory store, fork builder, node-agent).
This PRD defines the path to a production Kubernetes-native system where users
declare a `DatasetSpec` CRD, and the platform handles everything else: version
creation, snapshot building on ephemeral PDs, multi-cloud disk provisioning,
fleet-wide rollout, and automatic retirement.

---

## Goals

1. **Declarative interface.** Users create a `DatasetSpec` CR; the system
   converges toward serving the latest version of that dataset on every node.
2. **Pluggable version triggers.** New versions are produced by manual API call,
   Kubernetes events (e.g. BigQuery table update notification), or cron
   schedules — all expressed in the CRD.
3. **Multi-cloud disk provisioning.** The build job writes to a cloud PD that
   the node fleet later mounts read-only. The PD provider is pluggable: GCP
   Hyperdisk ML (day 1), with a
   filesystem-based provider for local development.
4. **Job-based builds.** Replace the fork builder with Kubernetes Jobs so builds
   survive control-plane restarts, have resource limits, and produce observable
   logs.
5. **Automatic version retirement.** Old versions are garbage-collected (disk +
   PV deleted) once all nodes have converged away from them and the retention
   policy is satisfied.

## Non-Goals

- Multi-region replication (handled externally by cloning PDs across regions).
- Write-path (frostmap is immutable by design).
- Data source plugins beyond BigQuery (CSV is dev-only; new sources are additive
  and don't change the control plane).
- Autoscaling the KV server — it's a DaemonSet, one per node.

---

## Background: What Exists Today

| Component | Status | Notes |
|---|---|---|
| Snapshot format (`frostmap-format`) | Production-ready | v4, SIMD probing, 2 MiB-aligned index |
| Build pipeline (`frostmap-loader`) | Production-ready | Parallel scatter/index with Arrow sources |
| KV server (`frostmap-server`) | Production-ready | Catalog mode, inotify hot-swap, memcache meta protocol |
| BigQuery source (`frostmap-bq`) | Production-ready | Storage Read API, field projection, row restriction |
| Protobuf encoding (`frostmap-encode`) | Production-ready | Arrow → protobuf transcoding, auto-generated descriptors |
| Go API types (`go/api`) | Stable | `DatasetSpec`, `VersionRecord`, `CatalogEntry`, `NodeAssignment`, etc. |
| In-memory store (`controlplane/store.go`) | Prototype | Full version lifecycle, rollout tracking, retirement checks |
| Fork builder (`controlplane/fork_builder.go`) | Prototype | Forks `fm load config` as a subprocess; PID-based tracking |
| Orchestrator (`controlplane/orchestrator.go`) | Prototype | Ties store + builder; reconcile loop with build timeout |
| Node agent (`nodeagent/agent.go`) | Production-ready | Attach → mount → catalog.json → confirm → cleanup; EBUSY retry on shutdown |
| VolumeManager interface | Defined | `FSVolumeManager` (dev), `FakeVolumeManager` (tests), `ComputeDiskManager` (stub) |
| Mounter interface | Defined | `FSMounter` (dev), `FakeMounter` (tests), `LinuxMounter` (linux) |

The gap is clear: the control plane is an in-memory prototype that cannot
survive restarts, builds run as local subprocesses, there is no CRD schema, no
K8s Job integration, and no multi-cloud disk provisioning.

---

## Architecture

### CRD Schema

Two custom resources. `DatasetSpec` is user-facing (desired state).
`DatasetVersion` is system-managed (actual state per version).

```yaml
apiVersion: frostmap.io/v1alpha1
kind: DatasetSpec
metadata:
  name: users
  namespace: frostmap-system
spec:
  keyPrefix: users           # routing prefix for KV server
  shardCount: 64             # number of partitions (power of 2)
  retention: 3               # number of ready/active versions to keep

  source:
    keyColumn: user_id
    # Exactly one of valueColumn or encoding must be set.
    encoding:
      protobuf:
        messageName: users.UserProfile
        descriptorURI: gs://my-bucket/users.desc
    bigquery:
      project: my-project
      table: my-project.prod.users
      rowRestriction: "active = true"

  # Version trigger policy. At least one must be set. Triggers are additive:
  # when both manual and cron are set, cron fires on schedule AND manual
  # triggers are accepted via API/kubectl. Each trigger independently
  # creates DatasetVersion CRs; the at-most-one-building invariant
  # prevents concurrent builds regardless of trigger source.
  trigger:
    # Manual: versions are created via API/kubectl only.
    manual: {}
    # Cron: versions are created on a schedule.
    cron:
      schedule: "0 2 * * *"       # daily at 2 AM
      timezone: America/New_York  # optional, defaults to UTC
    # Event: versions are created in response to external signals.
    # (Future: BigQuery completion notifications, Pub/Sub, webhooks)

  # Cloud disk configuration for the build output.
  # Cloud-specific tuning (throughput, IOPS, disk type) lives in the
  # StorageClass, which is cluster infrastructure — not here.
  disk:
    storageClassName: hyperdisk-ml  # pre-created by cluster admin
    sizeGb: 100                     # auto-sized if omitted

status:
  activeVersion: v42
  activeVersionReady: true
  observedGeneration: 7
  conditions:
    - type: Ready
      status: "True"
      reason: VersionActive
      message: "Version v42 is active on 150/150 nodes"
    - type: Building
      status: "False"
```

```yaml
apiVersion: frostmap.io/v1alpha1
kind: DatasetVersion
metadata:
  name: users-v42
  namespace: frostmap-system
  ownerReferences:
    - apiVersion: frostmap.io/v1alpha1
      kind: DatasetSpec
      name: users
spec:
  dataset: users
  versionId: v42
status:
  state: active            # building | ready | active | retired | failed
  pvName: pv-users-v42    # set after finalize; node-agent uses this to attach
  shardCount: 64
  descriptor: <base64>     # FileDescriptorSet, if protobuf-encoded
  messageName: users.UserProfile
  buildJob: fm-build-users-v42
  error: ""
  createdAt: "2026-04-04T02:00:00Z"
  readyAt: "2026-04-04T02:12:00Z"
  activeSince: "2026-04-04T02:12:05Z"
  rollout:
    total: 150
    converged: 150
    pending: 0
    error: 0
```

### Ownership and Garbage Collection

Controllers set `ownerReferences` at creation time. Kubernetes garbage
collection cascades deletions automatically through the ownership tree:

```
DatasetSpec  (user-created, namespaced)
  └─ owns ─▶  DatasetVersion  (controller-created, namespaced)
                ├─ owns ─▶  PersistentVolumeClaim  (build phase, RWO)
                ├─ owns ─▶  ConfigMap              (worker.json)
                └─ owns ─▶  Job                    (build)
                              └─ owns ─▶  Pod      (automatic, K8s built-in)
```

Deleting a `DatasetVersion` automatically cleans up its Job, ConfigMap, and
build PVC (which in turn triggers CSI disk deletion if the build failed before
finalizing). Deleting a `DatasetSpec` cascades through all its child versions.

**Cluster-scoped objects are not owned.** `PersistentVolume` and
`VolumeAttachment` are cluster-scoped; namespaced resources cannot own them
via `ownerReferences`. These require explicit deletion:

- **Finalized PV:** The DatasetVersion controller deletes it during retirement,
  after confirming no node still reports the version. CSI driver then deletes
  the underlying cloud disk.
- **VolumeAttachment:** The node-agent deletes it when cleaning up the old
  version after a successful swap.

| Object | Owner | Deletion |
|---|---|---|
| `DatasetVersion` | `DatasetSpec` | GC cascade on spec delete, or explicit on retirement |
| Build PVC | `DatasetVersion` | GC cascade (failure path); `FinalizeBuild` deletes (success path) |
| Build ConfigMap | `DatasetVersion` | GC cascade |
| Build Job | `DatasetVersion` | GC cascade |
| Build Pod | Job | GC cascade (K8s built-in) |
| Finalized PV | *none* (cluster-scoped) | Controller deletes explicitly during retirement |
| VolumeAttachment | *none* (cluster-scoped) | Node-agent deletes explicitly during cleanup |

### Controllers

Three controllers running in the control-plane Deployment:

#### 1. DatasetSpec Controller

Watches `DatasetSpec` CRs. Responsible for:

- **Cron scheduling.** If `spec.trigger.cron` is set, maintains an internal
  cron scheduler. On tick, creates a new `DatasetVersion` CR with a
  deterministic name (`<dataset>-v<timestamp>`). Skips if a version is already
  in `building` state (at-most-one-building invariant).
- **Status aggregation.** Reads child `DatasetVersion` CRs and updates
  `DatasetSpec.status` with the active version and rollout progress.
- **Retention enforcement.** When the number of `ready` + `active` versions
  exceeds `spec.retention`, deletes the oldest retired versions. The
  controller deletes the finalized PV explicitly (cluster-scoped), then deletes
  the `DatasetVersion` CR — Kubernetes GC cascades to child objects.

#### 2. DatasetVersion Controller (Build Lifecycle)

Watches `DatasetVersion` CRs. Drives the build state machine:

```
                                    ┌─────────────────────────────────┐
                                    │                                 │
[building] ──(Job succeeds)──▶ [ready] ──(promote)──▶ [active] ──▶ [retired]
    │                                                                 │
    └──(Job fails)──▶ [failed]                                        │
                                                     (retention GC)───┘
```

**building → ready:**
1. Creates a `PersistentVolumeClaim` (`ReadWriteOnce`, referencing the
   StorageClass from `spec.disk.storageClassName`). The CSI driver dynamically
   provisions the underlying cloud disk (e.g. Hyperdisk ML).
2. Creates a Kubernetes `Job` that mounts the PVC and runs
   `fm load config --config /etc/fm/worker.json`.
3. Polls the Job status. On success:
   - Reads `meta.json` from the snapshot to extract the protobuf descriptor
     (via the build Pod before it exits, or a short-lived reader Pod).
   - Calls `DiskManager.FinalizeBuild(pvcName)` — the implementation does
     whatever the storage backend requires to make the disk ready for
     multi-reader fan-out (see Disk Lifecycle Interface).
   - Sets `status.pvName` from the returned PV name.
   - Transitions to `ready`.
4. On failure: sets `status.error`, transitions to `failed`. Deleting the
   `DatasetVersion` CR cascades to the PVC via `ownerReferences`, and the
   CSI driver cleans up the underlying disk.
5. On timeout (configurable, default 30 min): cancels the Job, transitions to
   `failed`.

**ready → active:**
- The controller auto-promotes the newest `ready` version (unless manual
  promotion is configured).
- Promotion atomically retires the current `active` version.
- Updates node assignments via the existing watched API — no change to the
  node-agent protocol.

**retired → deleted:**
- A retired version is eligible for deletion when:
  1. All registered nodes have reported state (no unknown nodes).
  2. No node reports the retired version as its current version.
  3. The retention count allows it.
- Ordering: by the time retirement eligibility is confirmed, all node-agents
  have already deleted their VolumeAttachments (they do so immediately after
  the KV server confirms the new version). The controller can safely delete
  the PV without orphaning any VolumeAttachments.
- The controller deletes the `PersistentVolume` (cluster-scoped, not
  owned — must be explicit). The CSI driver deletes the underlying cloud disk.
- The controller then deletes the `DatasetVersion` CR. Kubernetes GC
  automatically cleans up any remaining child objects (Job, ConfigMap, PVC).

#### 3. Node Assignment Controller

Bridges CRD state to the existing node-agent protocol:

- Watches `DatasetVersion` CRs in `active` state.
- For each active version, pushes `NodeAssignment`s to all registered nodes
  via the existing long-poll HTTP API.
- Collects `NodeState` reports and writes rollout progress back to
  `DatasetVersion.status.rollout`.

This controller is intentionally thin — it translates between CRD world and
the existing HTTP protocol so that the node-agent requires zero changes.

---

### Disk Lifecycle Interface

The control plane never calls cloud APIs directly. All disk provisioning and
cleanup flows through Kubernetes storage primitives (PVC, PV, StorageClass).
The CSI driver — which runs as its own DaemonSet with cloud IAM — handles
the underlying cloud calls.

```go
// DiskManager manages the PVC/PV lifecycle for snapshot builds.
// All cloud-specific behavior is delegated to the CSI driver via the
// StorageClass. Implementations encapsulate storage-specific details
// (e.g. disk mode conversion) behind a uniform interface.
type DiskManager interface {
    // CreateBuildPVC creates a ReadWriteOnce PVC backed by the given
    // StorageClass. The CSI driver dynamically provisions the underlying
    // cloud disk.
    CreateBuildPVC(ctx context.Context, name string, storageClass string, sizeGB int64) error

    // FinalizeBuild transitions the build output into a read-only,
    // multi-attach-ready PersistentVolume. What this means is
    // storage-specific:
    //   - Hyperdisk ML: delete PVC, clear claimRef, flip PV to ROX
    //     (triggers CSI disk mode conversion RW_SINGLE → RO_MANY)
    //   - EBS io2: similar PV patching for multi-attach
    //   - Filesystem/hostPath: no-op (already readable)
    // Returns the PV name that node-agents will reference in
    // VolumeAttachments.
    //
    // Precondition: the PV's reclaimPolicy must be Retain. FinalizeBuild
    // checks this before deleting the PVC and returns an error if the
    // policy is Delete (which would cause the CSI driver to destroy the
    // disk the moment the PVC is deleted).
    FinalizeBuild(ctx context.Context, pvcName string) (pvName string, err error)

    // DeletePV deletes the PV and sets reclaimPolicy to Delete so the
    // CSI driver deletes the underlying cloud disk.
    DeletePV(ctx context.Context, pvName string) error
}
```

The controller calls `FinalizeBuild` after the Job succeeds. It doesn't
know or care whether that involves a disk mode conversion, a no-op, or
something else — storage-specific details stay inside the implementation.

**Dev/test: filesystem provider.** `FinalizeBuild` is a no-op. Uses a
`hostPath` StorageClass or the existing `FSVolumeManager`.

### Node-Side Volume Attachment

The node-agent attaches finalized PVs to its node using the Kubernetes
`VolumeAttachment` API. No cloud credentials are needed — the CSI driver
handles the underlying cloud attach/detach calls.

```go
// K8sVolumeManager implements the existing VolumeManager interface using
// Kubernetes VolumeAttachment objects instead of direct cloud API calls.
type K8sVolumeManager struct {
    KubeClient kubernetes.Interface
    CSIDriver  string // e.g. "pd.csi.storage.gke.io", "ebs.csi.aws.com"
}

// AttachDisk creates a VolumeAttachment:
//   spec:
//     attacher: <CSIDriver>
//     source:
//       persistentVolumeName: <pvName>
//     nodeName: <nodeName>
// Idempotent: if the VolumeAttachment already exists, this is a no-op.
func (m *K8sVolumeManager) AttachDisk(ctx context.Context, nodeName, pvName string) error

// WaitForDevice polls the VolumeAttachment until status.attached == true,
// then reads the device path from status.attachmentMetadata["devicePath"].
func (m *K8sVolumeManager) WaitForDevice(ctx context.Context, pvName string) (string, error)

// DetachDisk deletes the VolumeAttachment. The CSI driver detaches the
// underlying cloud disk. Idempotent: deleting a non-existent attachment
// is not an error.
func (m *K8sVolumeManager) DetachDisk(ctx context.Context, nodeName, pvName string) error
```

This replaces the `ComputeDiskManager` stub in `volume/k8s.go`. The
`VolumeManager` interface is unchanged — the node-agent code requires no
modifications. Only the implementation changes from "call Compute Engine
API" to "CRUD VolumeAttachment objects."

---

### StorageClass (Cluster Admin Prerequisite)

Cloud-specific tuning lives in the StorageClass, created once per cluster by
the cluster admin. The DatasetSpec only references it by name.

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: hyperdisk-ml
parameters:
  type: hyperdisk-ml
  provisioned-throughput-on-create: "2400Mi"
provisioner: pd.csi.storage.gke.io
reclaimPolicy: Retain          # we manage PV deletion ourselves
allowVolumeExpansion: false
volumeBindingMode: WaitForFirstConsumer
```

For AWS, the same pattern with a different provisioner:
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: ebs-io2
parameters:
  type: io2
  iopsPerGB: "50"
provisioner: ebs.csi.aws.com
reclaimPolicy: Retain
volumeBindingMode: WaitForFirstConsumer
```

---

### End-to-End Data Flow

```
Phase       Actor                    K8s Objects                     Cloud Side-Effect
─────       ─────                    ───────────                     ─────────────────

BUILD       DatasetVersion ctrl      PVC (RWO, StorageClass)         CSI provisions disk
            DatasetVersion ctrl      Job (mounts PVC at /output)     —
            fm container             —                               BQ read → write snapshot

FINALIZE    DatasetVersion ctrl      DiskManager.FinalizeBuild()     storage-specific
                                     (e.g. delete PVC, patch PV)     (e.g. CSI disk mode convert)

PROMOTE     DatasetVersion ctrl      Update DatasetVersion status    —
            NodeAssignment ctrl      Push NodeAssignment (pvName)    —

ATTACH      node-agent               Create VolumeAttachment         CSI attaches disk
                                     (attacher, pvName, nodeName)      to node
            node-agent               Poll VA.status.attached         —
            node-agent               —                               mount(device, /mnt/kv/…)
            node-agent               Write catalog.json              —

SERVE       KV server                —                               inotify → mmap → serve

CLEANUP     node-agent               Delete old VolumeAttachment     CSI detaches old disk
            node-agent               —                               umount old mount

RETIRE      DatasetVersion ctrl      Delete PV                       CSI deletes cloud disk
                                     Delete DatasetVersion CR        —
```

### Build Job Spec

The DatasetVersion controller creates three objects per build:

**1. PersistentVolumeClaim** (RWO, triggers dynamic disk provisioning):
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: fm-build-users-v42
  namespace: frostmap-system
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: hyperdisk-ml     # from DatasetSpec.spec.disk.storageClassName
  resources:
    requests:
      storage: 100Gi                 # from DatasetSpec.spec.disk.sizeGb
```

**2. ConfigMap** (worker.json, same format as `fork_builder.go`'s `workerConfig`):
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: fm-build-users-v42-config
  namespace: frostmap-system
data:
  worker.json: |
    {
      "source": { ... },
      "output": "/output",
      "partitions": 64
    }
```

**3. Job** (mounts PVC, runs `fm load config`):
```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: fm-build-users-v42
  namespace: frostmap-system
  ownerReferences:
    - apiVersion: frostmap.io/v1alpha1
      kind: DatasetVersion
      name: users-v42
spec:
  backoffLimit: 0            # no automatic retries; control plane decides
  activeDeadlineSeconds: 1800
  template:
    spec:
      restartPolicy: Never
      serviceAccountName: fm-builder
      containers:
        - name: builder
          image: frostmap/fm:latest
          command: ["fm", "load", "config", "--config", "/etc/fm/worker.json"]
          volumeMounts:
            - name: output
              mountPath: /output
            - name: config
              mountPath: /etc/fm
          resources:
            requests:
              cpu: "4"
              memory: 8Gi
      volumes:
        - name: output
          persistentVolumeClaim:
            claimName: fm-build-users-v42
        - name: config
          configMap:
            name: fm-build-users-v42-config
```

On Job completion, the controller calls `FinalizeBuild` (see Disk Lifecycle
Interface above) and transitions the DatasetVersion to `ready`.

---

## Version Trigger Mechanisms

### Manual

```bash
kubectl apply -f - <<EOF
apiVersion: frostmap.io/v1alpha1
kind: DatasetVersion
metadata:
  name: users-v43
  namespace: frostmap-system
spec:
  dataset: users
  versionId: v43
EOF
```

Or via the HTTP admin API:
```
POST /api/v1/dataset/users/build
```

### Cron

The DatasetSpec controller maintains an in-process cron scheduler (e.g.
`robfig/cron`). On tick:

1. Check if a `building` version already exists for this dataset. If so, skip.
2. Generate a version ID: `v<unix-timestamp>`.
3. Create a `DatasetVersion` CR. The DatasetVersion controller takes it from
   there.

The cron state is ephemeral — on restart, the controller recalculates the next
fire time from the schedule. Missed ticks during downtime fire at most once per
dataset on startup (catch-up policy). When multiple datasets have cron triggers,
catch-up builds are staggered to avoid overwhelming the cluster with concurrent
Jobs.

### Event-Driven (Future)

A webhook endpoint or Pub/Sub subscriber that creates `DatasetVersion` CRs in
response to external signals:

- BigQuery job completion notifications
- Cloud Scheduler HTTP triggers
- GitHub Actions / CI pipeline callbacks
- Pub/Sub messages from data pipeline orchestrators (Airflow, Dataflow)

The event handler is a thin adapter — it validates the event, resolves the
dataset name, and creates a `DatasetVersion` CR. All build logic remains in the
DatasetVersion controller.

---

## Implementation Plan

See [`doc/plan/KUBERNETES.md`](plan/KUBERNETES.md) for the detailed phased
implementation plan (7 phases, from KIND infrastructure through production
GKE). The design preserves the existing node-agent protocol and KV server
unchanged — migration is control-plane only.

---

## Operational Concerns

### Observability

- **Build metrics:** Job duration, success/failure rate, snapshot size, key
  count — emitted by the `fm` builder and scraped by Prometheus.
- **Rollout metrics:** Time from version creation to full convergence, per-node
  phase durations — emitted by the control plane.
- **Serving metrics:** Already exist in `frostmap-server` (lookup latency, hit
  rate, version swap count).
- **CRD status conditions:** Standard Kubernetes conditions on both CRDs for
  `kubectl` visibility.

### Failure Modes

| Failure | Impact | Recovery |
|---|---|---|
| Build Job OOM/timeout | Version stays in `building` | Controller marks `failed` after `activeDeadlineSeconds`; cron trigger creates a new version on next tick |
| Disk provision failure | Build cannot start | Controller retries with exponential backoff; surfaces error in `DatasetVersion.status` |
| Control plane crash | No new builds or promotions | Restarts with CRD state intact (Phase 4+); node-agents continue serving the last active version |
| Node agent crash | Node stops receiving updates | DaemonSet restarts it; agent re-reads assignments and converges; KV server continues serving from `catalog.json` |
| KV server crash | Node stops serving | DaemonSet restarts it; server reloads from `catalog.json`; node-agent confirms via `/version` poll |
| Cloud API outage | Disk attach/detach fails | Node agent retries on next reconcile cycle; exponential backoff on assignment fetch |

### High Availability

The control-plane Deployment should run with multiple replicas for HA.
Controller-runtime provides built-in leader election via Lease objects — only
the leader reconciles, and failover is automatic. Leader election must be
enabled before running more than one replica.

### CRD Validation

CRD structural schemas (OpenAPI v3) should enforce basic constraints at
admission time: `shardCount` must be a power of 2, `retention` must be ≥ 1,
`schedule` must be a valid cron expression, exactly one of `valueColumn` or
`encoding` must be set. A validating admission webhook can enforce cross-field
constraints that OpenAPI v3 cannot express.

### Security

- **Build Job:** Runs with `fm-builder` ServiceAccount. Needs BigQuery read
  access only. Disk provisioning is handled by the CSI driver, not the Job.
- **Control plane:** Needs RBAC for Jobs, ConfigMaps, PVCs (namespaced) and
  PVs (cluster-scoped). Also needs CRD read/write for DatasetSpec and
  DatasetVersion.
- **Node agent:** Privileged DaemonSet. Needs a ClusterRole with
  `VolumeAttachment` create/delete (cluster-scoped — cannot be namespaced).
  No cloud credentials — the CSI driver handles cloud calls.
- **KV server:** Unprivileged. No cloud access. Reads only from the local
  mount.
- **CRDs:** RBAC restricts `DatasetSpec` creation to authorized namespaces.
  `DatasetVersion` creation is restricted to the control plane ServiceAccount
  and operators.

---

## Open Questions

1. **Auto-sizing disks.** Should the build Job estimate the output size from
   the BigQuery source metadata and auto-provision the disk? Or require
   explicit `sizeGb` in the spec?

2. **Multi-region.** When the same dataset needs to be served in multiple
   regions, should the CRD express region targets, or should each region have
   its own `DatasetSpec` with a shared source?

3. **Canary rollout.** Should promotion support a canary phase where only a
   subset of nodes receive the new version before full rollout?

4. **Build caching.** If the BigQuery table hasn't changed since the last
   build, should the cron trigger skip? Requires a cheap staleness check
   (e.g. table modification time or row count).

5. **StorageClass per dataset vs shared.** Should each DatasetSpec reference
   its own StorageClass (for per-dataset throughput tuning), or should a
   single cluster-wide StorageClass suffice?
