# Kubernetes-Native Control Plane: Implementation Plan

This plan breaks the PRD (`doc/PRD-KUBERNETES-NATIVE.md`) into incremental
phases. Each phase is independently testable and delivers a working system at
its boundary. Phases 0-3 produce a working pipeline in KIND. Phases 4-5 make
it CRD-driven. Phase 6 targets production GKE.

All KIND testing uses podman on macOS for local dev and docker in CI.

---

## Phase 0: KIND Infrastructure + Container Images

### Goal

`make kind-up` creates a cluster. `make kind-load` builds and loads the `fm`
and `fmtctl` container images. No application code changes.

### Work

- Dockerfile for `fm` (Rust, multi-stage: builder + distroless runtime)
- Dockerfile for `fmtctl` (Go, multi-stage)
- KIND cluster config (`kind/cluster.yaml`): single-node, local-path
  StorageClass (ships by default with KIND)
- Deploy `csi-driver-host-path` into the KIND cluster. This lightweight CSI
  driver supports VolumeAttachment, so the full `K8sVolumeManager` path can
  be integration-tested in KIND rather than waiting for Phase 6 (GKE).
- Makefile targets:
  - `kind-up`: create cluster (podman provider on macOS, docker in CI)
  - `kind-down`: delete cluster
  - `kind-load`: build images + `kind load docker-image` into cluster
- CI workflow: `kind-up` → `kind-load` → smoke test → `kind-down`

### Test gate

```
make kind-up kind-load
kubectl get nodes              # Ready
kubectl get storageclass       # local-path (default) + csi-hostpath-sc
kubectl get csidriver          # hostpath.csi.k8s.io
# verify fm image is pullable inside the cluster
kubectl run smoke --image=frostmap/fm:dev --restart=Never -- fm --version
```

### Files

```
kind/cluster.yaml
docker/Dockerfile.fm
docker/Dockerfile.fmtctl
Makefile                    (new targets)
.github/workflows/kind.yaml (or equivalent CI)
```

---

## Phase 1: JobBuilder + DiskManager

### Goal

Builds run as Kubernetes Jobs instead of forked subprocesses, writing to a
PVC that survives the Job. `FinalizeBuild` produces a PV that node-agents
can mount. The `JobBuilder` implements the existing `AsyncBuilder` interface.
Everything else stays the same: in-memory store, FS volume manager, existing
orchestrator.

### Work

- Add `k8s.io/client-go` dependency to `go.mod`
- Define `DiskManager` interface:
  ```go
  type DiskManager interface {
      CreateBuildPVC(ctx, name, storageClass string, sizeGB int64) error
      FinalizeBuild(ctx, pvcName string) (pvName string, err error)
      DeletePV(ctx, pvName string) error
  }
  ```
- Implement `LocalPathDiskManager` for KIND's `local-path` StorageClass:
  - `CreateBuildPVC`: create RWO PVC
  - `FinalizeBuild`: wait for PVC to be bound, read the PV name from
    `pvc.spec.volumeName`, check `reclaimPolicy == Retain` (error
    otherwise), clear `pv.spec.claimRef`, set
    `pv.spec.accessModes = [ReadOnlyMany]`. For local-path this doesn't
    enable real multi-attach, but it works on a single-node KIND cluster.
  - `DeletePV`: delete PV (local-path cleans up the host directory)
- Implement `JobBuilder`:
  - `Start(spec, versionID)`:
    1. Call `DiskManager.CreateBuildPVC`
    2. Create ConfigMap with `worker.json` (same format as `ForkBuilder`'s
       `workerConfig`)
    3. Create Job that mounts the PVC at `/output` and the ConfigMap
    4. Return `BuildHandle(jobName)`
  - `Poll(handle)`: read Job `.status.conditions` —
    `type: Complete, status: True` → call `DiskManager.FinalizeBuild`,
    return `BuildComplete` with PV name;
    `type: Failed, status: True` → `BuildFailed`;
    otherwise → `BuildRunning`.
  - `Cancel(handle)`: delete the Job with `propagationPolicy: Background`
- `BuildHandle` = Job name (deterministic: `fm-build-<dataset>-<version>`)

### Test gate (unit)

`JobBuilder` + `DiskManager` with `fake.NewSimpleClientset()`:
- `Start` creates PVC + ConfigMap + Job with correct spec
- `Poll` returns correct phase for each Job condition
- `FinalizeBuild` clears claimRef, sets ROX, validates reclaimPolicy
- `Cancel` deletes the Job
- Idempotent: calling `Start` twice returns the same handle

### Test gate (KIND)

Integration test (build tag `kind`):
1. Deploy `fm` image into KIND (Phase 0)
2. Create a `JobBuilder` + `LocalPathDiskManager` with a real clientset
3. Build a snapshot from inline CSV via `fm load csv`
4. Poll until complete — PVC bound, Job succeeded, `FinalizeBuild` ran
5. Verify PV exists with `accessModes: [ReadOnlyMany]`, `claimRef` cleared
6. Create a reader Pod that mounts the PV read-only and verifies `meta.json`
7. `DeletePV` → PV gone

### Files

```
go/internal/controlplane/disk.go              (interface)
go/internal/controlplane/disk_localpath.go     (local-path implementation)
go/internal/controlplane/job_builder.go
go/internal/controlplane/job_builder_test.go
go/internal/controlplane/job_builder_kind_test.go   (build tag: kind)
```

---

## Phase 2: K8sVolumeManager

### Goal

Node-agent attaches and detaches disks via the Kubernetes VolumeAttachment
API instead of `FSVolumeManager` or the stubbed `ComputeDiskManager`.

### Work

- Implement `K8sVolumeManager` behind the existing `VolumeManager` interface:
  ```go
  type K8sVolumeManager struct {
      KubeClient kubernetes.Interface
      CSIDriver  string
  }
  ```
  - `AttachDisk(nodeName, pvName)`: create VolumeAttachment
    `{attacher: CSIDriver, source: {pvName}, nodeName}`
  - `WaitForDevice(pvName)`: poll VolumeAttachment `.status.attached`,
    read device path from `.status.attachmentMetadata["devicePath"]`
  - `DetachDisk(nodeName, pvName)`: delete VolumeAttachment
- All operations are idempotent (create-if-not-exists, delete-if-exists)

### Test gate (unit)

`K8sVolumeManager` with `fake.NewSimpleClientset()`:
- `AttachDisk` creates VolumeAttachment with correct spec
- `WaitForDevice` returns device path when status is updated
- `DetachDisk` deletes the VolumeAttachment
- Idempotent: double-attach and double-detach are no-ops

### Test gate (KIND)

Integration test using `csi-driver-host-path` (deployed in Phase 0):
- Create a PV backed by `hostpath.csi.k8s.io`
- `AttachDisk` → VolumeAttachment created, CSI driver responds
- `WaitForDevice` → returns device path from attachment metadata
- `DetachDisk` → VolumeAttachment deleted

This closes the integration gap — the real VolumeAttachment API path is
tested in KIND, not deferred to Phase 6 (GKE).

### Files

```
go/internal/volume/k8s.go     (replace existing ComputeDiskManager stub)
go/internal/volume/k8s_test.go
go/internal/volume/k8s_kind_test.go
```

---

## Phase 3: KIND End-to-End

### Goal

Full pipeline running in KIND: control-plane triggers build → Job writes
snapshot to PVC → finalize → assign to node → node-agent mounts and writes
catalog.json → KV server serves data via memcache → hot-swap to v2.

### Design decisions

- **Single-node KIND cluster.** Sufficient for validating the full pipeline.
  Multi-node multi-attach is tested on real cloud (Phase 6).
- **Node-agent uses `K8sVolumeManager` with `csi-driver-host-path`.**
  Phase 0 deploys the CSI driver into KIND, so the full VolumeAttachment
  path is exercised here.
- **Reuse existing integration test patterns.** The test harness in
  `go/internal/testutil/` already knows how to build snapshots, start KV
  servers, and assert memcache responses.

### Work

- Kubernetes manifests:
  - Deployment: control-plane (orchestrator + HTTP server)
  - DaemonSet: node-agent + KV server (two containers, shared EmptyDir for
    catalog.json)
  - ServiceAccount + RBAC: `fm-builder` (Job creation), `node-agent`
    (VolumeAttachment create/delete — cluster-scoped ClusterRole),
    `control-plane` (Job/PVC/PV management)
  - StorageClass: `local-path` (already default in KIND)
- E2e test harness (Go, build tag `kind`):
  1. Ensure KIND cluster is up with images loaded
  2. Deploy manifests via `kubectl apply`
  3. Register the node with the control-plane
  4. Trigger a build via HTTP API (`POST /api/v1/dataset/users/build`)
  5. Wait for Job completion and `FinalizeBuild`
  6. Wait for node-agent to reach `PhaseActive`
  7. Assert memcache response via TCP to the KV server
  8. Trigger v2 build, wait for hot-swap, assert new data
  9. Verify old version is eligible for retirement
- Makefile target: `make test-kind` (requires `make kind-up kind-load` first)
- **Cleanup is manual** in the e2e test (no CRDs yet, so no ownerReferences
  cascade). The test tears down objects with `kubectl delete` at the end.
  Proper ownership cascade comes in Phase 4 with CRDs.

### Test gate

`make test-kind` passes:
- v1 build succeeds, KV server responds to memcache queries
- v2 build succeeds, KV server hot-swaps, new data served
- Old version reaches retirement eligibility

### Files

```
k8s/base/control-plane.yaml
k8s/base/daemonset.yaml
k8s/base/rbac.yaml
k8s/kind/kustomization.yaml    (KIND-specific overrides)
go/internal/testutil/kind.go    (cluster helpers)
go/internal/testutil/kind_e2e_test.go
Makefile                        (test-kind target)
```

---

## Phase 4: CRDs + CRD-Backed Store

### Goal

`DatasetSpec` and `DatasetVersion` are real Kubernetes CRDs. State survives
control-plane restarts. `kubectl get datasetspecs` works.

### Work

- CRD Go types with kubebuilder markers in `go/api/v1alpha1/types.go`
  (natural choice given existing hand-written types in `go/api/types.go`).
  Kubebuilder generates CRD YAML, deepcopy, and OpenAPI validation from
  markers:
  - `DatasetSpec` (`frostmap.io/v1alpha1`): spec fields from
    `go/api/types.go`, status with conditions
  - `DatasetVersion` (`frostmap.io/v1alpha1`): spec (dataset, versionId),
    status (state, pvName, buildJob, rollout, error, timestamps)
- Structural schema validation via kubebuilder markers: `shardCount` power
  of 2, `retention` ≥ 1, required fields, enum constraints on `state`.
- Code generation: deepcopy, typed client, informers, listers
- `CRDStore` implementing the same method set as the in-memory `Store`:
  - `RegisterDataset` → create/update `DatasetSpec` CR
  - `CreateVersion` → create `DatasetVersion` CR with ownerRef to
    `DatasetSpec`
  - `MarkReady/MarkFailed/Promote` → update `DatasetVersion.status`
  - `GetAssignments` → derive from active `DatasetVersion` CRs
  - `GetBuildingVersions` → list with field selector `status.state=building`
- `ownerReferences`:
  - `DatasetVersion` owned by `DatasetSpec`
  - Build Job, ConfigMap, PVC owned by `DatasetVersion`
  - PV and VolumeAttachment: cluster-scoped, deleted explicitly (unchanged)
- Register CRDs in KIND cluster setup (`make kind-up` applies them)

### Test gate (KIND)

1. `kubectl apply -f datasetspec.yaml` → `kubectl get datasetspecs` shows it
2. Swap `Store` for `CRDStore` in the Phase 3 e2e test — same test, same
   outcome
3. Kill and restart the control-plane Pod → state is preserved, node-agent
   continues serving
4. `kubectl delete datasetspec users` → cascade deletes all child
   `DatasetVersion` CRs, their Jobs, ConfigMaps, and PVCs

### Files

```
go/api/v1alpha1/types.go         (CRD Go types)
go/api/v1alpha1/zz_deepcopy.go   (generated)
k8s/crds/datasetspec.yaml
k8s/crds/datasetversion.yaml
go/internal/controlplane/store_crd.go
go/internal/controlplane/store_crd_test.go
go/internal/controlplane/store_crd_kind_test.go
```

---

## Phase 5: Controllers

### Goal

Replace the orchestrator's reconcile loop with proper controller-runtime
reconcilers. The system is fully declarative: `kubectl apply` a
`DatasetSpec` and everything happens.

### Work

- Add `sigs.k8s.io/controller-runtime` dependency
- Enable **leader election** via controller-runtime's built-in Lease-based
  mechanism. Required before running multiple control-plane replicas for HA.
- `DatasetVersionReconciler`:
  - Watches `DatasetVersion` CRs
  - `building`: create PVC + ConfigMap + Job (if not exists), poll Job
    status, call `FinalizeBuild` on success, transition to `ready`
  - `ready`: auto-promote (newest ready becomes active, previous active
    becomes retired)
  - `retired`: check retirement eligibility, delete PV, delete CR
  - `failed`: no-op (terminal state, cleaned up by retention)
- `DatasetSpecReconciler`:
  - Watches `DatasetSpec` CRs
  - Cron: maintain in-process scheduler (`robfig/cron`), create
    `DatasetVersion` CR on tick (skip if one is already `building`).
    On startup, catch-up fires at most once per dataset, staggered to
    avoid thundering herd.
  - Status: aggregate child `DatasetVersion` statuses into
    `DatasetSpec.status`
  - Retention: delete oldest retired `DatasetVersion` CRs when count exceeds
    `spec.retention`
- `NodeAssignmentReconciler`:
  - Watches `DatasetVersion` CRs in `active` state
  - Pushes `NodeAssignment`s to all nodes via the existing HTTP long-poll API
  - Collects `NodeState` reports, writes rollout progress to
    `DatasetVersion.status.rollout`
- Retire the `Orchestrator` struct, its `ReconcileBuilds` loop, and the
  `CRDStore` abstraction from Phase 4. The reconcilers own all state
  transitions directly — the store indirection is no longer needed.
- Control-plane binary becomes a controller-manager

### Test gate (KIND)

1. `kubectl apply` a `DatasetSpec` with `trigger: {manual: {}}` →
   `kubectl apply` a `DatasetVersion` → controller creates Job → builds →
   promotes → KV serves data
2. `kubectl apply` a `DatasetSpec` with `trigger: {cron: {schedule: "* * * * *"}}` →
   version is created automatically within 60s → builds → serves
3. `kubectl delete datasetspec` → full cascade cleanup (CRs, Jobs, PVCs, PVs)
4. Create 5 versions with `retention: 2` → verify 3 oldest retired versions
   are cleaned up
5. Kill control-plane Pod → restarts → resumes reconciliation from CRD state

### Files

```
go/internal/controller/datasetversion.go
go/internal/controller/datasetversion_test.go
go/internal/controller/datasetspec.go
go/internal/controller/datasetspec_test.go
go/internal/controller/nodeassignment.go
go/internal/controller/nodeassignment_test.go
go/cmd/control-plane/main.go    (controller-manager entrypoint)
```

---

## Phase 6: GCP Hyperdisk ML

### Goal

Run on real GKE with real Hyperdisk ML disks and multi-node multi-attach.

### Work

- `HyperdiskDiskManager` implementing `DiskManager`:
  - `CreateBuildPVC`: RWO PVC with StorageClass `hyperdisk-ml`
  - `FinalizeBuild`: delete PVC, clear claimRef, set PV to ROX. CSI driver
    converts disk `READ_WRITE_SINGLE` → `READ_ONLY_MANY`.
  - `DeletePV`: delete PV, CSI driver deletes cloud disk
- StorageClass manifest for `hyperdisk-ml` (throughput, reclaimPolicy: Retain)
- `K8sVolumeManager` already implemented in Phase 2 — just configure with
  `CSIDriver: "pd.csi.storage.gke.io"`
- GKE-specific DaemonSet manifest (privileged, mountPropagation: Bidirectional)
- Real `LinuxMounter` instead of `FSMounter`
- Infrastructure-as-code for test GKE cluster (Terraform or Pulumi):
  - Machine type supporting Hyperdisk ML (C3, N2, etc.)
  - Workload Identity for `fm-builder` ServiceAccount (BigQuery access)
  - Node pool with appropriate labels

### Test gate (GKE)

Same e2e test flow as Phase 3/5, but on a multi-node GKE cluster:
1. Build snapshot from BigQuery (or CSV ConfigMap for test isolation)
2. Verify Hyperdisk ML disk created in Compute Engine
3. Verify disk mode conversion to `READ_ONLY_MANY`
4. Verify VolumeAttachment on multiple nodes
5. Verify memcache responses from multiple nodes
6. Hot-swap to v2, verify old disk detached and deleted

### Files

```
go/internal/controlplane/disk_hyperdisk.go
go/internal/controlplane/disk_hyperdisk_test.go
k8s/gke/kustomization.yaml
k8s/gke/storageclass.yaml
k8s/gke/daemonset.yaml
infra/gke/                     (Terraform/Pulumi)
```

---

## Dependency Graph

```
Phase 0 ─── Phase 1 ───┐
                        ├── Phase 3 ─── Phase 4 ─── Phase 5 ─── Phase 6
         Phase 2 ───────┘
```

Phases 1 and 2 can run in parallel (no dependency). Phase 3 merges them.
Phase 6 can start infrastructure setup in parallel with Phase 5.

## Summary

| Phase | What | Tests On | Deliverable |
|---|---|---|---|
| 0 | KIND infra + container images + CSI driver | KIND | `make kind-up kind-load` |
| 1 | JobBuilder + DiskManager | fake client + KIND | Builds as K8s Jobs, output on PVC |
| 2 | K8sVolumeManager | fake client + KIND | VolumeAttachment CRUD via CSI |
| 3 | KIND end-to-end | KIND | Full pipeline: build → serve → hot-swap |
| 4 | CRDs + CRD Store (kubebuilder) | KIND | `kubectl get datasetspecs`, state persists |
| 5 | Controllers | KIND | Fully declarative, orchestrator retired |
| 6 | GCP Hyperdisk ML | GKE | Real multi-attach, production-ready |
