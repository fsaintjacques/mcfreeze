# Architecture Design: Dynamic Hyperdisk ML KV Store

## 1. The Problem Statement
Standard Kubernetes storage is designed for persistent, long-lived volumes attached to Pods at boot time. However, modern ML and data-intensive applications require:
* **Zero-Downtime Updates:** The ability to swap massive datasets (Multi-TB) daily without restarting Pods or dropping connections.
* **Massive Fan-out:** A single data volume must be readable by thousands of nodes simultaneously.
* **High Performance:** Sub-millisecond Key-Value lookups with minimal IO overhead.
* **Stateless Compute:** Pods should not "own" specific disks; they should simply consume the "current" version of the data available on the node.

Traditional `PersistentVolumeClaims` (PVC) in a Deployment spec do not support "hot-swapping" without a rolling restart, creating a bottleneck for daily data refreshes.

---

## 2. The Solution: "The Sidecar-less Orchestrator"
We decouple the **Data Lifecycle** from the **Pod Lifecycle** using a custom Go-based Control Plane and a Node-level DaemonSet.

### High-Level Workflow
1.  **Ingestion:** BigQuery exports sharded data to Google Cloud Storage (GCS).
2.  **Hydration:** A GKE Job transforms raw GCS data into a specialized **4KB-aligned Flat File** format and hydrates a new **Hyperdisk ML** volume.
3.  **Orchestration:** The **Go Control Plane** marks the new volume as the "Active Version."
4.  **Mounting:** A **Go DaemonSet** on each node detects the new version, attaches/mounts the disk to the Host, and updates a local **JSON Manifest**.
5.  **Consumption:** The ML App polls the JSON manifest and `mmap`s the new data files instantly with zero downtime.

---

## 3. Data Format Optimization
To achieve **Single-IO Seek** performance, we bypass traditional databases in favor of a static binary format.

* **`index.idx`**: A Static Perfect Hash Table (or Cuckoo Hash) containing fixed-width structs: `[Hash(Key) | Offset | Size]`. This is kept in RAM via `mmap`.
* **`data.bin`**: The raw values, where every entry is **4KB Aligned**. This ensures that any `pread` call results in exactly one physical hardware block read.
* **Probabilistic Filter**: A Bloom or Xor Filter is embedded in the index header to prevent unnecessary disk seeks for non-existent keys.

---

## 4. System Components

### A. The Go Control Plane
Acts as the "Brain" of the storage layer.
* **Job Management**: Triggers BQ-to-GCS exports and monitors the GKE Volume Populator.
* **Version Registry**: Maintains a mapping of `VersionID` to `GCP_Disk_Link`.
* **Health Tracking**: Ensures a new version is hydrated and "Ready" before signaling a swap.

### B. The Go DaemonSet (The Mount Manager)
Runs with `privileged: true` and `mountPropagation: Bidirectional`.
* **Disk Attachment**: Calls the Compute Engine API to attach Hyperdisk ML volumes to the local Node.
* **Linux Plumbing**: Executes `mount -o ro` commands and manages symbolic links.
* **The "Stamp"**: Writes an atomic `catalog.json` file to a shared `hostPath` volume accessible by all ML Pods on that node.

### C. The ML Application (The Client)
* **Stateless Consumer**: Mounts the `hostPath` containing `catalog.json` and the data directories.
* **Hot-Reload Logic**: A background thread polls the JSON file. Upon a version change, it calls `munmap` on the old index and `mmap` on the new one.

---

## 5. Key Advantages

| Feature | Benefit |
| :--- | :--- |
| **Statelessness** | Pods can be killed/rescheduled without worrying about disk attachments. |
| **Scalability** | Hyperdisk ML supports up to 2,500 readers, allowing the cluster to scale to thousands of nodes. |
| **Efficiency** | `mmap` + 4KB alignment + Perfect Hashing provides the theoretical limit of read performance. |
| **Zero Downtime** | Data versions are swapped in-memory; the application process never stops. |

---

## 6. Infrastructure Requirements
* **Machine Types**: N2, C3, or A3 (Required for Hyperdisk ML).
* **GKE Features**: GKE Volume Populator (using `GCPDataSource` CRD).
* **IAM**: Workload Identity with `compute.instances.attachDisk` and `storage.objectViewer` permissions.

---

Would you like me to draft the **Go struct** for the `catalog.json` or the **Kubernetes Manifest** for the privileged DaemonSet to get you started?