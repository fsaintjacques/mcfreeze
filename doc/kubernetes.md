# Kubernetes Resources

## Custom Resource Definitions

Three CRDs define the declarative interface. API group: `mcfreeze.dev`,
version: `v1alpha1`.

### Dataset (`fmd`)

User-facing. Describes an immutable KV dataset and its build configuration.
The resource name is the dataset name.

```yaml
apiVersion: mcfreeze.dev/v1alpha1
kind: Dataset
metadata:
  name: users
  namespace: mcfreeze-system
spec:
  keyPrefix: users
  shardCount: 64
  retention: 2
  source:
    keyColumn: user_id
    encoding:
      protobuf:
        messageName: users.UserProfile
        descriptorUri: gs://my-bucket/users.desc
    bigquery:
      project: my-project
      table: my-project.prod.users
      rowRestriction: "active = true"
  trigger:
    cron:
      schedule: "0 2 * * *"
  builderResources:
    requests:
      cpu: "4"
      memory: 8Gi
status:
  activeVersion: v42
  conditions: [...]
```

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.keyPrefix` | string | yes | Routing prefix for the KV server; unique per node |
| `spec.shardCount` | int | yes | Hash partitions (must be power of 2) |
| `spec.retention` | int | no (default 2) | Ready versions to keep before cleanup |
| `spec.source` | SourceSpec | yes | How to produce key-value pairs (see below) |
| `spec.trigger` | TriggerSpec | no | Build trigger: `manual` or `cron` |
| `spec.builderResources` | ResourceRequirements | no | CPU/memory overrides for builder Jobs |
| `status.activeVersion` | string | — | Version ID currently being served |
| `status.conditions` | []Condition | — | Standard Kubernetes conditions |

**SourceSpec:**

| Field | Type | Description |
|---|---|---|
| `keyColumn` | string | Arrow column name whose bytes become the KV key |
| `valueColumn` | string | Arrow column for raw value mode (mutually exclusive with `encoding`) |
| `encoding.protobuf` | ProtobufEncoding | Arrow → protobuf transcoding config |
| `bigquery` | BigQuerySource | `project`, `table`, `selectedFields`, `rowRestriction` |
| `csv` | CsvSource | `data` (inline) or `path` (filesystem) |

**TriggerSpec** (discriminated union — at most one):

| Field | Description |
|---|---|
| `manual` | Build-on-demand only (no fields) |
| `cron.schedule` | Standard 5-field cron expression |

**Printer columns:** `KeyPrefix`, `Shards`, `Active`, `Age`

---

### DatasetVersion (`fmv`)

System-managed. Tracks a single snapshot version of a Dataset. The spec is
**immutable after creation** (enforced by CEL validation rule). The resource
name is deterministic: `<dataset>-<versionId>`.

```yaml
apiVersion: mcfreeze.dev/v1alpha1
kind: DatasetVersion
metadata:
  name: users-v42
  namespace: mcfreeze-system
  ownerReferences:
    - apiVersion: mcfreeze.dev/v1alpha1
      kind: Dataset
      name: users
      controller: true
      blockOwnerDeletion: true
spec:
  dataset: users
  versionId: v42
  shardCount: 64
status:
  state: active
  pvName: pv-users-v42
  buildJob: mcf-build-users-v42
  rollout:
    totalNodes: 150
    convergedNodes: 150
    pendingNodes: 0
    errorNodes: 0
```

| Field | Type | Description |
|---|---|---|
| `spec.dataset` | string | Parent Dataset name |
| `spec.versionId` | string | Version identifier |
| `spec.shardCount` | int | Partitions (copied from Dataset at creation) |
| `status.state` | enum | `building` · `ready` · `active` · `retired` · `failed` |
| `status.pvName` | string | PersistentVolume name (set on transition to `ready`) |
| `status.snapshotPath` | string | On-disk path (local builds only) |
| `status.buildJob` | string | Builder Job name |
| `status.diskUrl` | string | Cloud disk resource URL |
| `status.descriptor` | string | Base64 FileDescriptorSet (protobuf snapshots) |
| `status.messageName` | string | Fully-qualified protobuf message name |
| `status.error` | string | Failure reason (when `failed`) |
| `status.rollout` | RolloutStatus | Per-node convergence while `active` |
| `status.conditions` | []Condition | Standard Kubernetes conditions |

**RolloutStatus:**

| Field | Description |
|---|---|
| `totalNodes` | Nodes the version has been pushed to |
| `convergedNodes` | Nodes reporting `active` for this version |
| `pendingNodes` | Nodes not yet reached `active` |
| `errorNodes` | Nodes reporting `error` |

**Printer columns:** `Dataset`, `Version`, `State`, `PV`, `Age`

---

### DatasetBinding (`fmb`)

User-facing. Selects which datasets are served on which nodes via label
selectors. No status subresource.

```yaml
apiVersion: mcfreeze.dev/v1alpha1
kind: DatasetBinding
metadata:
  name: gpu-nodes-users
  namespace: mcfreeze-system
spec:
  nodeSelector:
    matchLabels:
      pool: gpu
  datasetSelector:
    matchLabels:
      team: recommendations
```

| Field | Type | Description |
|---|---|---|
| `spec.nodeSelector` | LabelSelector | Selects Kubernetes nodes (nil = all) |
| `spec.datasetSelector` | LabelSelector | Selects Datasets by `metadata.labels` (nil = all) |

**Semantics:**
- **No binding matches a node** → open-world default: the node receives all
  datasets.
- **Multiple bindings match a node** → dataset sets are unioned.
- A binding with both selectors nil matches all nodes and all datasets
  (equivalent to no binding).

**Printer columns:** `Age`

---

## Ownership and Garbage Collection

Controllers set `ownerReferences` at creation time. Kubernetes garbage
collection cascades deletions automatically:

```
Dataset  (user-created)
  └─ owns ─▶  DatasetVersion  (controller-created)
                ├─ owns ─▶  PersistentVolumeClaim  (build phase, RWO)
                ├─ owns ─▶  ConfigMap              (worker.json)
                └─ owns ─▶  Job                    (build)
                              └─ owns ─▶  Pod      (K8s built-in)
```

Deleting a `Dataset` cascades through all its child versions, which cascade to
their Jobs, ConfigMaps, and PVCs.

**Cluster-scoped objects are not owned.** `PersistentVolume` and
`VolumeAttachment` are cluster-scoped; namespaced resources cannot own them via
`ownerReferences`. These require explicit deletion:

| Object | Owner | Deletion |
|---|---|---|
| `DatasetVersion` | `Dataset` | GC cascade on Dataset delete, or explicit on retirement |
| Build PVC | `DatasetVersion` | GC cascade (failure); `FinalizeBuild` deletes (success) |
| Build ConfigMap | `DatasetVersion` | GC cascade |
| Build Job | `DatasetVersion` | GC cascade |
| Finalized PV | *none* (cluster-scoped) | Controller deletes explicitly during retirement |
| VolumeAttachment | *none* (cluster-scoped) | Node-agent deletes explicitly during cleanup |

---

## Version State Machine

```
(DatasetVersion created)
    │
    ▼
[building] ──(Job succeeds)──▶ [ready] ──(auto-promote)──▶ [active]
    │                                                          │
    │──(Job fails)──▶ [failed]              (new version)──────┘
    │──(timeout)─────▶ [failed]                                │
                                                               ▼
                                                          [retired]
                                                               │
                                                    (all VAs gone + PV deleted)
                                                               │
                                                         (CR deleted)
```

| State | Managed by | Meaning |
|---|---|---|
| `building` | DatasetVersion controller | Builder Job running; polled every 5s |
| `ready` | DatasetVersion controller | Disk finalized, PV created, `pvName` set |
| `active` | DatasetVersion controller | Currently served; node-agents attaching |
| `retired` | DatasetVersion controller | Superseded; awaiting VolumeAttachment drain |
| `failed` | DatasetVersion controller | Build failed or timed out; resources cleaned up |

**ready → active** is automatic: the newest `ready` version auto-promotes,
demoting the current `active` to `retired`.

**retired → deleted** is gated by VolumeAttachments: the controller watches
VAs and only deletes the PV + CR once no VA references the version's PV.

---

## RBAC

### Control Plane

**Namespaced Role:**

| API Group | Resources | Verbs |
|---|---|---|
| `batch` | jobs | create, get, update, delete, list |
| `""` (core) | configmaps | create, get, delete |
| `""` (core) | persistentvolumeclaims | create, get, update, delete, list |
| `""` (core) | pods | list, delete |
| `mcfreeze.dev` | datasets, datasetversions, datasetbindings | create, get, update, delete, list, watch, patch |
| `mcfreeze.dev` | datasets/status, datasetversions/status | get, update, patch |
| `coordination.k8s.io` | leases | get, list, watch, create, update, patch, delete |
| `""` (core) | events | create, patch |

**ClusterRole** (cluster-scoped resources):

| API Group | Resources | Verbs |
|---|---|---|
| `""` (core) | nodes | get, list, watch |
| `""` (core) | persistentvolumes | create, get, update, delete, list |
| `storage.k8s.io` | volumeattachments | get, list, watch |

### Node Agent

**ClusterRole** (all resources are cluster-scoped):

| API Group | Resources | Verbs |
|---|---|---|
| `storage.k8s.io` | volumeattachments | create, get, delete, list, watch |
| `""` (core) | persistentvolumes | get |

### Builder

A separate ServiceAccount with no RBAC rules by default. Annotate it for
Workload Identity (BigQuery read access):

```yaml
serviceAccount:
  builderAnnotations:
    iam.gke.io/gcp-service-account: mcf-builder@my-project.iam.gserviceaccount.com
```

---

## Helm Chart

Chart: `k8s/charts/mcfreeze` (v0.1.0, requires Kubernetes >= 1.27.0).

### Deployed Resources

| Resource | Name | Condition |
|---|---|---|
| Deployment | `<release>-control-plane` | always |
| DaemonSet | `<release>-node-agent` | `nodeAgent.enabled` |
| Service | `<release>-control-plane` | always |
| ServiceAccount × 3 | control-plane, node-agent, builder | `serviceAccount.create` |
| Role + RoleBinding | control-plane | `rbac.create` |
| ClusterRole + ClusterRoleBinding × 2 | control-plane, node-agent | `rbac.create` |
| StorageClass | `controlPlane.storageClass` | `storageClass.create` |
| CRDs × 3 | datasets, datasetversions, datasetbindings | always (in `crds/`) |

### DaemonSet Pod

Two containers share an EmptyDir (`catalog`) and a hostPath (`mounts`):

| Container | Image | Privileged | Key mounts |
|---|---|---|---|
| `node-agent` | mcfreeze | yes (root) | catalog (rw), mounts (Bidirectional), /dev |
| `kv-server` | mcfreeze (`mcf serve catalog`) | no | catalog (ro), mounts (HostToContainer) |

Mount propagation: the node-agent mounts block devices under `/mnt/kv` with
`Bidirectional` propagation, making them visible to the KV server container
via `HostToContainer`.

### Key Values

```yaml
controlPlane:
  storageClass: ""         # REQUIRED
  diskSizeGB: 10
  builderPodTemplate: {}

nodeAgent:
  csiDriver: pd.csi.storage.gke.io
  mounter: linux           # "linux" or "fs" (KIND)
  kvServer:
    memcachePort: 11211    # standard memcache port (CLI examples use 7777 for convenience)
    metricsPort: 7777     # HTTP server: /version + /metrics

storageClass:
  create: false            # set true to auto-create
  provisioner: ""
  reclaimPolicy: Delete
  volumeBindingMode: WaitForFirstConsumer
```

### KIND Development Profile

`values-kind.yaml` overrides for local development:

```yaml
controlPlane:
  storageClass: csi-hostpath-sc
  diskSizeGB: 1
  leaderElection: false

nodeAgent:
  csiDriver: hostpath.csi.k8s.io
  mounter: fs              # symlinks instead of real mounts
  csiHostPath: /var/lib/csi-hostpath-data
```

---

## StorageClass

Cloud-specific tuning lives in the StorageClass, created once per cluster.
The `Dataset` CRD references it indirectly via the control-plane flag.

Example for GCP Hyperdisk ML:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: hyperdisk-ml
provisioner: pd.csi.storage.gke.io
parameters:
  type: hyperdisk-ml
  provisioned-throughput-on-create: "2400Mi"
reclaimPolicy: Retain
volumeBindingMode: WaitForFirstConsumer
```

`reclaimPolicy: Retain` is critical — `FinalizeBuild` deletes the PVC after
the build, and the PV must survive. The control plane sets `Retain` explicitly
during finalization, but the StorageClass default avoids a race if the PVC
binds before the patch.
