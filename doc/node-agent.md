<!-- SPDX-License-Identifier: Apache-2.0 -->

# Node Agent

## Overview

`mcfctl node-agent` is the node-side lifecycle manager. One instance runs per
node as a privileged DaemonSet container alongside the KV server. It converges
the node toward the desired dataset assignments by attaching disks, mounting
them read-only, writing `catalog.json`, and confirming the KV server has loaded
the new version before cleaning up the old one.

The agent follows a **converging reconciliation model**: it maintains full
actual state in memory, reconciles against desired assignments on every
control-plane poll, and periodically reports complete `NodeState`. A missed
report never causes permanent divergence — the next one self-heals.

---

## Configuration

```
mcfctl node-agent \
  -control-plane-url http://mcfreeze-control-plane:8080 \
  -node-name $NODE_NAME \
  -mount-base /mnt/kv \
  -catalog-dir /run/kv \
  -csi-driver pd.csi.storage.gke.io \
  -mounter linux
```

| Flag | Default | Description |
|---|---|---|
| `-control-plane-url` | *(required)* | Base URL of the control-plane HTTP API |
| `-node-name` | `$NODE_NAME` | Kubernetes node name |
| `-mount-base` | `/mnt/kv` | Root directory for snapshot mounts |
| `-catalog-dir` | `/run/kv` | Shared EmptyDir where `catalog.json` is written |
| `-csi-driver` | `pd.csi.storage.gke.io` | CSI driver name for VolumeAttachments |
| `-mounter` | `linux` | Mount backend: `linux` (syscall) or `fs` (symlink, for dev) |

---

## Reconciliation Loop

Two concurrent loops run independently:

1. **Assignment loop** — blocks on long-poll to the control-plane; on response,
   reconciles each assignment and reports state immediately.
2. **Report loop** — fires every 30 seconds, posts full `NodeState` to the
   control-plane.

```
                    ┌──────────────────────┐
                    │    Control Plane     │
                    │  GET  /assignments   │◄── long-poll (generation)
                    │  POST /state         │◄── every 30s + after each reconcile
                    └──────────┬───────────┘
                               │
                    ┌──────────▼───────────┐
                    │    Agent Loop        │
                    │                      │
                    │ for each assignment: │
                    │   reconcile(assign)  │
                    │ doReport()           │
                    └──────────────────────┘
```

Assignment fetching uses exponential backoff on failure (1s → 30s, reset on
success). The long-poll uses a generation counter: the server blocks until the
generation changes, so the agent sleeps without polling.

---

## Per-Dataset Phase Lifecycle

Each dataset on the node transitions through these phases:

```
(new assignment)
    │
    ▼
[attaching] ──(VolumeAttachment ready)──▶ [mounting] ──(mount + catalog.json + KV confirm)──▶ [active]
    │                                         │                                                   │
    └──(error)──▶ [error]◄──(error)───────────┘                                                   │
                     │                                                                            │
                     └──(next reconcile)──▶ [attaching]                  (new version)────────────┘
```

| Phase | Meaning |
|---|---|
| `attaching` | VolumeAttachment created; waiting for CSI driver to attach the disk |
| `mounting` | Block device present; mount syscall in progress |
| `active` | Mounted, `catalog.json` written, KV server confirmed via `/version` |
| `error` | An operation failed; error message in `DatasetState.Error` |

### Reconciliation Steps

For each assignment received from the control-plane:

1. **Skip if converged** — if the dataset is already `active` at the desired
   version, do nothing.
2. **Attach disk** — create a `VolumeAttachment` (idempotent); phase →
   `attaching`.
3. **Wait for device** — poll the `VolumeAttachment` until
   `status.attached == true`; resolve the device path.
4. **Mount** — mount the block device read-only; phase → `mounting`.
5. **Write catalog.json** — atomic `rename(2)` with all active datasets on
   the node (see below).
6. **Confirm KV server** — poll `GET /version` until the KV server reports
   the new version (timeout: 2 minutes); phase → `active`.
7. **Clean up old version** — unmount and detach the previous version's disk.

All errors are non-fatal: the dataset transitions to `error` and the agent
continues reconciling other datasets. The next assignment fetch retries.

---

## Catalog Write

`catalog.json` is the interface between the node-agent and the KV server. It
is written atomically via a temp file + `rename(2)` into the shared EmptyDir.

Every write includes entries for **all** active datasets on the node, not just
the one being promoted. This ensures the KV server always has a complete view.

```json
{
  "entries": [
    {
      "dataset": "users",
      "key_prefix": "users",
      "version_id": "v42",
      "mount_path": "/mnt/kv/users/v42"
    },
    {
      "dataset": "products",
      "key_prefix": "products",
      "version_id": "v7",
      "mount_path": "/mnt/kv/products/v7"
    }
  ]
}
```

The KV server detects the file change (via filesystem watcher), loads the new
snapshots, and atomically swaps its active catalog. The node-agent then polls
`GET /version` to confirm the swap completed before cleaning up the old disk.

---

## Volume Attachment

The node-agent attaches disks via the Kubernetes `VolumeAttachment` API. No
cloud credentials are needed — the CSI driver (running as its own DaemonSet)
handles the underlying cloud calls.

### VolumeAttachment Object

```yaml
apiVersion: storage.k8s.io/v1
kind: VolumeAttachment
metadata:
  name: mcf-va-<pv-name>        # deterministic, lowercase, hyphens
spec:
  attacher: pd.csi.storage.gke.io
  source:
    persistentVolumeName: <pv-name>
  nodeName: <node-name>
```

### Device Path Resolution

After `status.attached == true`, the device path is resolved by:

1. `status.attachmentMetadata["devicePath"]` (if present)
2. Scan `/dev/disk/by-id/` for a symlink containing the PV name
3. PV spec lookup for hostPath-style CSI drivers (KIND dev clusters)

### Detach

Deleting the `VolumeAttachment` object triggers the CSI driver to detach the
cloud disk from the node. Idempotent — deleting a non-existent attachment is
not an error.

---

## Mounting

### Linux (production)

Flags: `MS_RDONLY | MS_NODEV | MS_NOSUID`

The mount syscall tries ext4 first, then xfs (Hyperdisk ML volumes may be
formatted as either). Before mounting, any stale mount at the target path is
lazily unmounted (`MNT_DETACH`).

Mount path convention: `<mount-base>/<dataset>/<version-id>`
(e.g. `/mnt/kv/users/v42`).

Unmount uses `MNT_DETACH` (lazy unmount): the mount is detached from the
namespace immediately, but the filesystem stays alive until all references
(open fds, mmaps) are dropped. This allows the KV server to finish in-flight
requests against the old mmap during a hot-swap.

### Filesystem simulation (development)

The `fs` mounter uses symlinks instead of real mounts, requiring no root
privileges. Used for local development and KIND integration tests.

---

## Graceful Shutdown

On `SIGTERM` (Kubernetes pod termination):

1. The main reconciliation loop exits (context cancelled).
2. `Shutdown()` runs with a 25-second timeout (5s buffer within the default
   30s Kubernetes grace period).
3. For each dataset: unmount, then detach.

Unmount retries with exponential backoff (500ms → 5s) on `EBUSY`. This handles
the race where Kubernetes sends `SIGTERM` to all containers in parallel — the
KV server may still hold mmapped file descriptors when the node-agent tries to
unmount.

---

## State Reporting

The agent periodically POSTs the full `NodeState` to the control-plane:

```
POST /api/v1/node/{node-name}/state
```

```json
{
  "node": "gke-pool-abc123",
  "datasets": [
    {
      "dataset": "users",
      "key_prefix": "users",
      "version_id": "v42",
      "phase": "active",
      "pv_name": "pv-users-v42",
      "mount_path": "/mnt/kv/users/v42",
      "error": ""
    }
  ],
  "reported_at": "2026-04-10T02:12:05Z"
}
```

Reports are level-triggered (full state, not deltas). The control-plane diffs
reported state against desired assignments to track rollout convergence.
Failed reports are logged but do not block the agent.

---

## API Contracts

### Control-plane → Node-agent (long-poll)

```
GET /api/v1/node/{node-name}/assignments?generation={N}
```

Returns the current `NodeAssignment` for each dataset assigned to this node.
The server blocks until the generation changes (long-poll). Each assignment
includes the `PVName` so node-agents need no cloud credentials.

### Node-agent → KV Server (version confirmation)

```
GET http://localhost:<metrics-port>/version
```

The metrics port is the KV server's HTTP port (Helm default: `7777`). Polled
every 500ms after writing `catalog.json`. The agent waits until the response
includes the expected dataset + version pair (timeout: 2 minutes).

---

## Timing Constants

| Parameter | Value |
|---|---|
| Report interval | 30s |
| Assignment fetch backoff | 1s → 30s (exponential) |
| Assignment fetch timeout | 5 min (long-poll) |
| State report timeout | 10s |
| VolumeAttachment poll interval | 2s |
| VolumeAttachment poll timeout | 2 min |
| KV version check interval | 500ms |
| KV version check timeout | 2 min |
| Unmount EBUSY backoff | 500ms → 5s (exponential) |
| Shutdown grace period | 25s |

---

## Dependency Injection

All external dependencies are injected as interfaces for testability:

| Interface | Production | Dev/Test |
|---|---|---|
| `assignment.Source` | `HTTPSource` (long-poll) | `FakeSource` (channel) |
| `assignment.StateReporter` | `HTTPStateReporter` (POST) | `FakeStateReporter` (in-memory) |
| `volume.Manager` | `K8sManager` (VolumeAttachment API) | `FSManager` (mkdir), `FakeManager` (in-memory) |
| `mount.Mounter` | `LinuxMounter` (syscall) | `FSMounter` (symlink), `FakeMounter` (in-memory) |
| `version.Checker` | `HTTPChecker` (poll /version) | `FakeChecker` (in-memory) |
