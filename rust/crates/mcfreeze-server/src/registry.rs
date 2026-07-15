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
use mcfreeze_format::Snapshot;
use prometheus_client::metrics::{counter::Counter, family::Family};

use crate::lookup::LookupOutcome;
use crate::metrics::{DatasetResultLabels, LookupCounters, UNKNOWN_DATASET};
use crate::ServeError;

// ---------------------------------------------------------------------------
// DatasetHandle
// ---------------------------------------------------------------------------

/// An opened, named snapshot.
pub struct DatasetHandle {
    pub name: String,
    pub version: String,
    reader: Arc<Snapshot>,
    counters: LookupCounters,
}

impl DatasetHandle {
    /// Open the snapshot at `dir` and wrap it in a named handle whose
    /// lookups are counted under `mcf_lookup_total{dataset=name}`.
    ///
    /// `Snapshot::open` performs all residency work (including populating
    /// the index into the page cache), so the first request against the
    /// new handle does not page-fault against cold storage. Callers must
    /// run this on a blocking-friendly context (the catalog builder uses
    /// `spawn_blocking`).
    pub fn open(
        name: String,
        version: String,
        dir: &Path,
        lookup_total: &Family<DatasetResultLabels, Counter>,
    ) -> Result<Self, ServeError> {
        let reader = Snapshot::open_path(dir)?;
        let counters = LookupCounters::new(lookup_total, &name);
        Ok(Self {
            name,
            version,
            reader: Arc::new(reader),
            counters,
        })
    }

    /// Expected paid-miss rate of this dataset's snapshot format.
    pub fn expected_miss_io_rate(&self) -> f64 {
        self.reader.expected_miss_io_rate()
    }

    /// Look up `key`.  Offloads `pread` to the blocking thread pool.
    /// Records the outcome under this dataset's `mcf_lookup_total`.
    pub async fn get(&self, key: &[u8]) -> Result<LookupOutcome, ServeError> {
        let key = bytes::Bytes::copy_from_slice(key);
        let reader = Arc::clone(&self.reader);
        let result = tokio::task::spawn_blocking(move || {
            reader
                .get(key.as_ref())
                .map(LookupOutcome::from)
                .map_err(Into::into)
        })
        .await
        .map_err(|e| ServeError::BlockingTaskPanicked(e.to_string()))?;
        self.counters.record(&result);
        result
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
    /// `mcf_lookup_total{dataset="_unknown", result="miss"}` — lookups
    /// whose key prefix matched no dataset. A bounded sentinel: raw
    /// client-supplied prefixes must never become label values.
    unknown_miss: Counter,
}

impl ActiveCatalog {
    pub fn new(
        datasets: HashMap<String, DatasetHandle>,
        generation: u64,
        loaded_at: SystemTime,
        lookup_total: &Family<DatasetResultLabels, Counter>,
    ) -> Self {
        let unknown_miss = lookup_total
            .get_or_create(&DatasetResultLabels {
                dataset: UNKNOWN_DATASET.to_string(),
                result: "miss",
            })
            .clone();
        Self {
            datasets,
            generation,
            loaded_at,
            unknown_miss,
        }
    }

    pub fn dataset_count(&self) -> usize {
        self.datasets.len()
    }

    /// Per-dataset expected paid-miss rates, for the
    /// `mcf_expected_miss_io_rate{dataset}` gauge family.
    pub fn expected_miss_io_rates(&self) -> impl Iterator<Item = (&str, f64)> {
        self.datasets
            .values()
            .map(|h| (h.name.as_str(), h.expected_miss_io_rate()))
    }

    /// Iterator over `(key_prefix, dataset_name, version_id)` triples.
    pub fn datasets(&self) -> impl Iterator<Item = (&str, &str, &str)> {
        self.datasets
            .iter()
            .map(|(prefix, h)| (prefix.as_str(), h.name.as_str(), h.version.as_str()))
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

    /// Look up `key` in `dataset`.  Returns a free miss for unknown
    /// datasets (no index was ever consulted), counted under the
    /// `_unknown` sentinel dataset.
    pub async fn get(&self, dataset: &str, key: &[u8]) -> Result<LookupOutcome, ServeError> {
        match self.datasets.get(dataset) {
            None => {
                self.unknown_miss.inc();
                Ok(LookupOutcome::Miss { io: false })
            }
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
    /// Returns the previous `Arc<ActiveCatalog>`; the caller detaches its
    /// teardown to the blocking pool. Mmaps and fds are released whenever
    /// the last reference (detached drop or in-flight request) goes away —
    /// there is no ordering guarantee relative to the reload sequence.
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
    use mcfreeze_format::writer::SnapshotWriter;
    use tempfile::TempDir;

    fn build_snapshot(pairs: &[(&[u8], &[u8])]) -> TempDir {
        let dir = TempDir::new().unwrap();
        let mut w = SnapshotWriter::new(dir.path(), 4).unwrap();
        for &(k, v) in pairs {
            w.write(k, v).unwrap();
        }
        w.finish().unwrap();
        dir
    }

    fn family() -> Family<DatasetResultLabels, Counter> {
        Family::default()
    }

    fn assert_hit(o: LookupOutcome, expected: &[u8]) {
        match o {
            LookupOutcome::Hit(v) => assert_eq!(v, expected),
            other => panic!("expected Hit({expected:?}), got {other:?}"),
        }
    }

    fn assert_miss(o: LookupOutcome) {
        assert!(
            matches!(o, LookupOutcome::Miss { .. }),
            "expected Miss, got {o:?}"
        );
    }

    // --- DatasetHandle ---

    #[tokio::test]
    async fn dataset_handle_hit() {
        let dir = build_snapshot(&[(b"k", b"v")]);
        let h = DatasetHandle::open("ds".into(), "v1".into(), dir.path(), &family()).unwrap();
        assert_hit(h.get(b"k").await.unwrap(), b"v");
    }

    #[tokio::test]
    async fn dataset_handle_open_nonexistent_returns_error() {
        let result = DatasetHandle::open(
            "ds".into(),
            "v1".into(),
            "/nonexistent/path".as_ref(),
            &family(),
        );
        assert!(result.is_err());
    }

    // --- ActiveCatalog ---

    #[tokio::test]
    async fn active_catalog_hit() {
        let dir = build_snapshot(&[(b"key", b"val")]);
        let mut datasets = HashMap::new();
        datasets.insert(
            "myds".into(),
            DatasetHandle::open("myds".into(), "v1".into(), dir.path(), &family()).unwrap(),
        );
        let catalog = ActiveCatalog::new(datasets, 1, SystemTime::UNIX_EPOCH, &family());
        assert_hit(catalog.get("myds", b"key").await.unwrap(), b"val");
    }

    #[tokio::test]
    async fn active_catalog_miss_known_dataset() {
        let dir = build_snapshot(&[(b"key", b"val")]);
        let mut datasets = HashMap::new();
        datasets.insert(
            "myds".into(),
            DatasetHandle::open("myds".into(), "v1".into(), dir.path(), &family()).unwrap(),
        );
        let catalog = ActiveCatalog::new(datasets, 1, SystemTime::UNIX_EPOCH, &family());
        assert_miss(catalog.get("myds", b"absent").await.unwrap());
    }

    #[tokio::test]
    async fn active_catalog_miss_unknown_dataset() {
        let catalog = ActiveCatalog::new(HashMap::new(), 0, SystemTime::UNIX_EPOCH, &family());
        assert_miss(catalog.get("unknown", b"key").await.unwrap());
    }

    // --- per-dataset metrics ---

    #[tokio::test]
    async fn lookup_total_counts_hit_miss_and_unknown() {
        let fam = family();
        let dir = build_snapshot(&[(b"k", b"v")]);
        let mut ds = HashMap::new();
        ds.insert(
            "myds".into(),
            DatasetHandle::open("myds".into(), "v1".into(), dir.path(), &fam).unwrap(),
        );
        let catalog = ActiveCatalog::new(ds, 1, SystemTime::UNIX_EPOCH, &fam);

        assert_hit(catalog.get("myds", b"k").await.unwrap(), b"v");
        assert_miss(catalog.get("myds", b"absent").await.unwrap());
        assert_miss(catalog.get("nope", b"key").await.unwrap());

        let count = |dataset: &str, result: &'static str| {
            fam.get_or_create(&DatasetResultLabels {
                dataset: dataset.to_string(),
                result,
            })
            .get()
        };
        assert_eq!(count("myds", "hit"), 1);
        assert_eq!(count("myds", "miss"), 1);
        assert_eq!(count("myds", "miss_io"), 0);
        assert_eq!(count("myds", "error"), 0);
        // Unknown prefixes land on the bounded sentinel, never on a raw
        // client-supplied label value.
        assert_eq!(count(UNKNOWN_DATASET, "miss"), 1);
        assert_eq!(count("nope", "miss"), 0);
    }

    #[tokio::test]
    async fn expected_miss_io_rates_lists_all_datasets() {
        let fam = family();
        let dir1 = build_snapshot(&[(b"a", b"1")]);
        let dir2 = build_snapshot(&[(b"b", b"2")]);
        let mut ds = HashMap::new();
        ds.insert(
            "d1".into(),
            DatasetHandle::open("d1".into(), "v1".into(), dir1.path(), &fam).unwrap(),
        );
        ds.insert(
            "d2".into(),
            DatasetHandle::open("d2".into(), "v1".into(), dir2.path(), &fam).unwrap(),
        );
        let catalog = ActiveCatalog::new(ds, 1, SystemTime::UNIX_EPOCH, &fam);

        let mut rates: Vec<_> = catalog.expected_miss_io_rates().collect();
        rates.sort_by(|a, b| a.0.cmp(b.0));
        assert_eq!(rates, vec![("d1", 0.0), ("d2", 0.0)]); // V4: ≈0 expected

        let empty = ActiveCatalog::new(HashMap::new(), 0, SystemTime::UNIX_EPOCH, &fam);
        assert_eq!(empty.expected_miss_io_rates().count(), 0);
    }

    // --- Registry ---

    #[tokio::test]
    async fn registry_load_and_swap() {
        let dir1 = build_snapshot(&[(b"a", b"1")]);
        let dir2 = build_snapshot(&[(b"a", b"2")]);

        let mut ds1 = HashMap::new();
        ds1.insert(
            "ds".into(),
            DatasetHandle::open("ds".into(), "v1".into(), dir1.path(), &family()).unwrap(),
        );
        let mut ds2 = HashMap::new();
        ds2.insert(
            "ds".into(),
            DatasetHandle::open("ds".into(), "v2".into(), dir2.path(), &family()).unwrap(),
        );

        let reg = Registry::new(ActiveCatalog::new(
            ds1,
            0,
            SystemTime::UNIX_EPOCH,
            &family(),
        ));

        // Initial load.
        let catalog = Arc::clone(&*reg.load());
        assert_eq!(catalog.generation, 0);
        assert_hit(catalog.get("ds", b"a").await.unwrap(), b"1");

        // Swap to generation 1.
        let old = reg.swap(Arc::new(ActiveCatalog::new(
            ds2,
            1,
            SystemTime::UNIX_EPOCH,
            &family(),
        )));
        assert_eq!(old.generation, 0);

        let catalog = Arc::clone(&*reg.load());
        assert_eq!(catalog.generation, 1);
        assert_hit(catalog.get("ds", b"a").await.unwrap(), b"2");
    }
}
