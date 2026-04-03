//! Catalog-mode data structures: [`DatasetHandle`], [`ActiveCatalog`], [`Registry`].
//!
//! ## Concurrency model
//!
//! `ActiveCatalog` is immutable after construction and stored inside an
//! [`ArcSwap`] cell.  The hot read path is:
//!
//! ```text
//! CatalogLookup::get
//!   → Arc::clone(&*registry.load())   // clone Arc, drop Guard immediately
//!   → active_catalog.get(dataset, key).await
//! ```
//!
//! Callers clone the `Arc<ActiveCatalog>` before awaiting so the seqlock-pinned
//! `Guard` is never held across an `.await` point, keeping the future `Send`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use arc_swap::ArcSwap;
use bytes::Bytes;
use frostmap_format::reader::SnapshotReader;

use crate::ServeError;

// ---------------------------------------------------------------------------
// DatasetHandle
// ---------------------------------------------------------------------------

/// An opened, named snapshot.
pub struct DatasetHandle {
    pub name: String,
    pub version: String,
    reader: Arc<SnapshotReader>,
}

impl DatasetHandle {
    /// Open the snapshot at `dir` and wrap it in a named handle.
    pub fn open(name: String, version: String, dir: &Path) -> Result<Self, ServeError> {
        let reader = SnapshotReader::open(dir)?;
        Ok(Self {
            name,
            version,
            reader: Arc::new(reader),
        })
    }

    /// Look up `key`.  Offloads `pread` to the blocking thread pool.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, ServeError> {
        let key = Bytes::copy_from_slice(key);
        let reader = Arc::clone(&self.reader);
        tokio::task::spawn_blocking(move || {
            reader
                .get(key.as_ref())
                .map(|v| v.map(Bytes::from))
                .map_err(Into::into)
        })
        .await
        .map_err(|e| ServeError::BlockingTaskPanicked(e.to_string()))?
    }
}

// ---------------------------------------------------------------------------
// ActiveCatalog
// ---------------------------------------------------------------------------

/// One entry returned by [`ActiveCatalog::version_snapshot`]; used to build
/// the `GET /version` HTTP response.
pub struct VersionEntry {
    pub dataset: String,
    pub version_id: String,
    pub loaded_at: SystemTime,
}

/// Immutable snapshot of the currently-active dataset set.
///
/// Stored behind an `ArcSwap`; the catalog-watcher task atomically replaces
/// it on each hot-swap.
pub struct ActiveCatalog {
    datasets: HashMap<String, DatasetHandle>,
    /// Monotonically increasing; incremented on each successful hot-swap.
    pub generation: u64,
    /// Wall-clock time at which this catalog was loaded.
    pub loaded_at: SystemTime,
}

impl ActiveCatalog {
    pub fn new(
        datasets: HashMap<String, DatasetHandle>,
        generation: u64,
        loaded_at: SystemTime,
    ) -> Self {
        Self {
            datasets,
            generation,
            loaded_at,
        }
    }

    pub fn dataset_count(&self) -> usize {
        self.datasets.len()
    }

    /// Snapshot of per-dataset version info for the `GET /version` HTTP response.
    /// Sorted by dataset name for deterministic JSON output.
    pub fn version_snapshot(&self) -> Vec<VersionEntry> {
        let mut entries: Vec<_> = self
            .datasets
            .values()
            .map(|h| VersionEntry {
                dataset: h.name.clone(),
                version_id: h.version.clone(),
                loaded_at: self.loaded_at,
            })
            .collect();
        entries.sort_by(|a, b| a.dataset.cmp(&b.dataset));
        entries
    }

    /// Look up `key` in `dataset`.  Returns `Ok(None)` for unknown datasets.
    pub async fn get(&self, dataset: &str, key: &[u8]) -> Result<Option<Bytes>, ServeError> {
        match self.datasets.get(dataset) {
            None => Ok(None),
            Some(h) => h.get(key).await,
        }
    }
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// RCU cell for the active catalog.  `CatalogLookup` holds an `Arc<Registry>`.
pub struct Registry {
    inner: ArcSwap<ActiveCatalog>,
}

impl Registry {
    pub fn new(initial: ActiveCatalog) -> Arc<Self> {
        Arc::new(Self {
            inner: ArcSwap::from_pointee(initial),
        })
    }

    /// Hot path — seqlock-pinned guard, no atomic strong-count increment.
    ///
    /// Callers should clone the inner `Arc` and drop this guard before `.await`
    /// to keep the future `Send` and avoid holding the epoch pin unnecessarily.
    pub fn load(&self) -> arc_swap::Guard<Arc<ActiveCatalog>> {
        self.inner.load()
    }

    /// Replace the active catalog.  Called only by the catalog-watcher task.
    ///
    /// Returns the previous `Arc<ActiveCatalog>`; it is dropped by the caller
    /// after writing the ack file, allowing mmaps and fds to be released.
    pub fn swap(&self, new: Arc<ActiveCatalog>) -> Arc<ActiveCatalog> {
        self.inner.swap(new)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use frostmap_format::writer::SnapshotWriter;
    use tempfile::TempDir;

    fn build_snapshot(pairs: &[(&[u8], &[u8])]) -> TempDir {
        let dir = TempDir::new().unwrap();
        let mut w = SnapshotWriter::new(dir.path(), 4).unwrap();
        for &(k, v) in pairs {
            w.write(k, v).unwrap();
        }
        w.finish(dir.path()).unwrap();
        dir
    }

    // --- DatasetHandle ---

    #[tokio::test]
    async fn dataset_handle_hit() {
        let dir = build_snapshot(&[(b"k", b"v")]);
        let h = DatasetHandle::open("ds".into(), "v1".into(), dir.path()).unwrap();
        let got = h.get(b"k").await.unwrap();
        assert_eq!(got, Some(Bytes::from_static(b"v")));
    }

    #[tokio::test]
    async fn dataset_handle_open_nonexistent_returns_error() {
        let result = DatasetHandle::open("ds".into(), "v1".into(), "/nonexistent/path".as_ref());
        assert!(result.is_err());
    }

    // --- ActiveCatalog ---

    #[tokio::test]
    async fn active_catalog_hit() {
        let dir = build_snapshot(&[(b"key", b"val")]);
        let mut datasets = HashMap::new();
        datasets.insert(
            "myds".into(),
            DatasetHandle::open("myds".into(), "v1".into(), dir.path()).unwrap(),
        );
        let catalog = ActiveCatalog::new(datasets, 1, SystemTime::UNIX_EPOCH);
        let got = catalog.get("myds", b"key").await.unwrap();
        assert_eq!(got, Some(Bytes::from_static(b"val")));
    }

    #[tokio::test]
    async fn active_catalog_miss_known_dataset() {
        let dir = build_snapshot(&[(b"key", b"val")]);
        let mut datasets = HashMap::new();
        datasets.insert(
            "myds".into(),
            DatasetHandle::open("myds".into(), "v1".into(), dir.path()).unwrap(),
        );
        let catalog = ActiveCatalog::new(datasets, 1, SystemTime::UNIX_EPOCH);
        let got = catalog.get("myds", b"absent").await.unwrap();
        assert_eq!(got, None);
    }

    #[tokio::test]
    async fn active_catalog_miss_unknown_dataset() {
        let catalog = ActiveCatalog::new(HashMap::new(), 0, SystemTime::UNIX_EPOCH);
        let got = catalog.get("unknown", b"key").await.unwrap();
        assert_eq!(got, None);
    }

    // --- Registry ---

    #[tokio::test]
    async fn registry_load_and_swap() {
        let dir1 = build_snapshot(&[(b"a", b"1")]);
        let dir2 = build_snapshot(&[(b"a", b"2")]);

        let mut ds1 = HashMap::new();
        ds1.insert(
            "ds".into(),
            DatasetHandle::open("ds".into(), "v1".into(), dir1.path()).unwrap(),
        );
        let mut ds2 = HashMap::new();
        ds2.insert(
            "ds".into(),
            DatasetHandle::open("ds".into(), "v2".into(), dir2.path()).unwrap(),
        );

        let reg = Registry::new(ActiveCatalog::new(ds1, 0, SystemTime::UNIX_EPOCH));

        // Initial load.
        let catalog = Arc::clone(&*reg.load());
        assert_eq!(catalog.generation, 0);
        assert_eq!(
            catalog.get("ds", b"a").await.unwrap(),
            Some(Bytes::from_static(b"1"))
        );

        // Swap to generation 1.
        let old = reg.swap(Arc::new(ActiveCatalog::new(ds2, 1, SystemTime::UNIX_EPOCH)));
        assert_eq!(old.generation, 0);

        let catalog = Arc::clone(&*reg.load());
        assert_eq!(catalog.generation, 1);
        assert_eq!(
            catalog.get("ds", b"a").await.unwrap(),
            Some(Bytes::from_static(b"2"))
        );
    }
}
