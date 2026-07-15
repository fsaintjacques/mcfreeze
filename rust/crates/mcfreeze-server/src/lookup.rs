//! Lookup trait, factory, and implementations.
//!
//! ## Design
//!
//! [`Lookup`] takes `&mut self` so each connection can maintain per-connection
//! state without a lock.  The accept loop calls [`LookupFactory::for_connection`]
//! once per accepted connection to obtain a `Box<dyn Lookup>`; the connection
//! handler owns it exclusively for its lifetime.
//!
//! [`SnapshotLookup`] — single static snapshot; factory clones the inner Arc.
//!
//! [`CatalogLookup`] — shared factory for catalog mode; each connection gets a
//! [`PerConnectionCatalogLookup`] that caches the current `Arc<ActiveCatalog>`
//! and refreshes it on generation change.  On the hot path (no swap since last
//! request) there is zero atomic traffic to the registry.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use mcfreeze_format::{GetOutcome, Snapshot};

use crate::metrics::LookupCounters;
use crate::registry::{ActiveCatalog, Registry};
use crate::ServeError;

// ---------------------------------------------------------------------------
// LookupOutcome
// ---------------------------------------------------------------------------

/// Result of a successful lookup.
///
/// Mirrors [`GetOutcome`]'s cost semantics: `Miss { io: true }` paid one
/// or more `pread`s to conclude absence (a fingerprint or filter false
/// positive, surfaced as `result="miss_io"` in `/metrics`); `Miss { io:
/// false }` touched no disk. The wire response is identical for both.
#[derive(Debug)]
pub enum LookupOutcome {
    Hit(Bytes),
    Miss { io: bool },
}

impl From<GetOutcome> for LookupOutcome {
    fn from(o: GetOutcome) -> Self {
        match o {
            GetOutcome::Hit(v) => LookupOutcome::Hit(Bytes::from(v)),
            GetOutcome::Miss { io } => LookupOutcome::Miss { io },
        }
    }
}

// ---------------------------------------------------------------------------
// Lookup
// ---------------------------------------------------------------------------

/// Per-connection interface between the protocol layer and storage.
///
/// `&mut self` allows implementations to cache state across calls (e.g. the
/// current [`ActiveCatalog`]) without any locking.
#[async_trait]
pub trait Lookup: Send + 'static {
    async fn get(&mut self, key: &[u8]) -> Result<LookupOutcome, ServeError>;
}

// ---------------------------------------------------------------------------
// LookupFactory
// ---------------------------------------------------------------------------

/// Shared factory (`Arc<dyn LookupFactory>`) held by the accept loops.
/// Called once per accepted connection to produce a per-connection `Lookup`.
pub trait LookupFactory: Send + Sync + 'static {
    fn for_connection(&self) -> Box<dyn Lookup>;
}

// ---------------------------------------------------------------------------
// SnapshotLookup
// ---------------------------------------------------------------------------

/// Single-snapshot lookup.  `pread` is offloaded to the blocking thread pool;
/// the inner `Arc<Snapshot>` is cheaply cloned per connection.
/// Outcomes are counted under `mcf_lookup_total{dataset="snapshot"}`.
pub struct SnapshotLookup {
    inner: Arc<Snapshot>,
    counters: LookupCounters,
}

impl SnapshotLookup {
    pub fn new(reader: Arc<Snapshot>, counters: LookupCounters) -> Self {
        Self {
            inner: reader,
            counters,
        }
    }
}

#[async_trait]
impl Lookup for SnapshotLookup {
    async fn get(&mut self, key: &[u8]) -> Result<LookupOutcome, ServeError> {
        let key = Bytes::copy_from_slice(key);
        let inner = Arc::clone(&self.inner);
        let result = tokio::task::spawn_blocking(move || {
            inner
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

impl LookupFactory for SnapshotLookup {
    fn for_connection(&self) -> Box<dyn Lookup> {
        Box::new(Self {
            inner: Arc::clone(&self.inner),
            counters: self.counters.clone(),
        })
    }
}

// ---------------------------------------------------------------------------
// CatalogLookup  (factory)
// ---------------------------------------------------------------------------

/// Catalog-mode factory.  Shared across all connections; each call to
/// [`for_connection`] snapshots the current `Arc<ActiveCatalog>` and hands it
/// to a [`PerConnectionCatalogLookup`].
pub struct CatalogLookup {
    registry: Arc<Registry>,
}

impl CatalogLookup {
    pub fn new(registry: Arc<Registry>) -> Self {
        Self { registry }
    }
}

impl LookupFactory for CatalogLookup {
    fn for_connection(&self) -> Box<dyn Lookup> {
        let catalog = Arc::clone(&*self.registry.load());
        Box::new(PerConnectionCatalogLookup {
            registry: Arc::clone(&self.registry),
            catalog,
        })
    }
}

// ---------------------------------------------------------------------------
// PerConnectionCatalogLookup
// ---------------------------------------------------------------------------

/// Per-connection catalog lookup.
///
/// Holds one `Arc<ActiveCatalog>` for the connection lifetime and refreshes it
/// only when the catalog generation changes.  On the hot path (no swap) the
/// only registry access is a seqlock `load()` to read the generation — no
/// atomic increment, no heap allocation.
struct PerConnectionCatalogLookup {
    registry: Arc<Registry>,
    catalog: Arc<ActiveCatalog>,
}

#[async_trait]
impl Lookup for PerConnectionCatalogLookup {
    async fn get(&mut self, key: &[u8]) -> Result<LookupOutcome, ServeError> {
        // Guard is a temporary dropped before .await — future remains Send.
        {
            let guard = self.registry.load();
            if guard.generation != self.catalog.generation {
                self.catalog = Arc::clone(&*guard);
            }
        }

        let (dataset_bytes, actual_key) = split_prefix(key)?;
        let dataset =
            std::str::from_utf8(dataset_bytes).map_err(|_| ServeError::InvalidDatasetName)?;
        // Clone the Arc so we don't borrow self across the .await.
        Arc::clone(&self.catalog).get(dataset, actual_key).await
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Split `b"<dataset>:<actual-key>"` on the first `b':'`.
/// Both slices borrow from `key`; no allocation.
fn split_prefix(key: &[u8]) -> Result<(&[u8], &[u8]), ServeError> {
    let pos = key
        .iter()
        .position(|&b| b == b':')
        .ok_or(ServeError::MissingDatasetPrefix)?;
    Ok((&key[..pos], &key[pos + 1..]))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{DatasetHandle, Registry};
    use mcfreeze_format::writer::SnapshotWriter;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn family() -> prometheus_client::metrics::family::Family<
        crate::metrics::DatasetResultLabels,
        prometheus_client::metrics::counter::Counter,
    > {
        Default::default()
    }

    fn test_counters() -> LookupCounters {
        LookupCounters::new(&family(), "test")
    }

    fn build_snapshot(pairs: &[(&[u8], &[u8])]) -> TempDir {
        let dir = TempDir::new().unwrap();
        let mut w = SnapshotWriter::new(dir.path(), 4).unwrap();
        for &(k, v) in pairs {
            w.write(k, v).unwrap();
        }
        w.finish().unwrap();
        dir
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

    // --- SnapshotLookup ---

    #[tokio::test]
    async fn hit_returns_value() {
        let dir = build_snapshot(&[(b"hello", b"world")]);
        let reader = Snapshot::open_path(dir.path()).unwrap();
        let mut lookup = SnapshotLookup::new(Arc::new(reader), test_counters());
        assert_hit(lookup.get(b"hello").await.unwrap(), b"world");
    }

    #[tokio::test]
    async fn miss_returns_miss() {
        let dir = build_snapshot(&[(b"present", b"yes")]);
        let reader = Snapshot::open_path(dir.path()).unwrap();
        let mut lookup = SnapshotLookup::new(Arc::new(reader), test_counters());
        assert_miss(lookup.get(b"absent").await.unwrap());
    }

    #[tokio::test]
    async fn factory_creates_independent_connection_lookups() {
        let dir = build_snapshot(&[(b"k", b"v")]);
        let reader = Snapshot::open_path(dir.path()).unwrap();
        let factory = SnapshotLookup::new(Arc::new(reader), test_counters());
        let mut c1 = factory.for_connection();
        let mut c2 = factory.for_connection();
        assert_hit(c1.get(b"k").await.unwrap(), b"v");
        assert_hit(c2.get(b"k").await.unwrap(), b"v");
    }

    #[tokio::test]
    async fn io_error_surfaces_as_format_error() {
        // Use 1 partition so b"key" is guaranteed to land in part-0.
        let dir = {
            let d = TempDir::new().unwrap();
            let mut w = SnapshotWriter::new(d.path(), 1).unwrap();
            w.write(b"key", b"value").unwrap();
            w.finish().unwrap();
            d
        };
        let reader = Snapshot::open_path(dir.path()).unwrap();
        let mut lookup = SnapshotLookup::new(Arc::new(reader), test_counters());
        std::fs::OpenOptions::new()
            .write(true)
            .open(dir.path().join("data/part-0/data.bin"))
            .unwrap()
            .set_len(0)
            .unwrap();
        let err = lookup.get(b"key").await.unwrap_err();
        assert!(matches!(err, ServeError::Format(_)));
    }

    // --- CatalogLookup / PerConnectionCatalogLookup ---

    fn make_registry(pairs: &[(&[u8], &[u8])], gen: u64) -> (Arc<Registry>, TempDir) {
        let dir = build_snapshot(pairs);
        let mut ds = HashMap::new();
        ds.insert(
            "ds".into(),
            DatasetHandle::open("ds".into(), "v1".into(), dir.path(), &family()).unwrap(),
        );
        (
            Registry::new(ActiveCatalog::new(
                ds,
                gen,
                std::time::SystemTime::UNIX_EPOCH,
                &family(),
            )),
            dir,
        )
    }

    #[tokio::test]
    async fn catalog_hit() {
        let (reg, _dir) = make_registry(&[(b"key", b"val")], 1);
        let mut conn = CatalogLookup::new(reg).for_connection();
        assert_hit(conn.get(b"ds:key").await.unwrap(), b"val");
    }

    #[tokio::test]
    async fn catalog_miss_known_dataset() {
        let (reg, _dir) = make_registry(&[(b"key", b"val")], 1);
        let mut conn = CatalogLookup::new(reg).for_connection();
        assert_miss(conn.get(b"ds:absent").await.unwrap());
    }

    #[tokio::test]
    async fn catalog_miss_unknown_dataset() {
        let reg = Registry::new(ActiveCatalog::new(
            HashMap::new(),
            0,
            std::time::SystemTime::UNIX_EPOCH,
            &family(),
        ));
        let mut conn = CatalogLookup::new(reg).for_connection();
        assert_miss(conn.get(b"unknown:key").await.unwrap());
    }

    #[tokio::test]
    async fn catalog_missing_prefix_returns_error() {
        let reg = Registry::new(ActiveCatalog::new(
            HashMap::new(),
            0,
            std::time::SystemTime::UNIX_EPOCH,
            &family(),
        ));
        let mut conn = CatalogLookup::new(reg).for_connection();
        assert!(matches!(
            conn.get(b"no-colon").await.unwrap_err(),
            ServeError::MissingDatasetPrefix
        ));
    }

    #[tokio::test]
    async fn catalog_refreshes_on_generation_change() {
        let dir1 = build_snapshot(&[(b"k", b"v1")]);
        let dir2 = build_snapshot(&[(b"k", b"v2")]);

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
            std::time::SystemTime::UNIX_EPOCH,
            &family(),
        ));
        let mut conn = CatalogLookup::new(Arc::clone(&reg)).for_connection();

        assert_hit(conn.get(b"ds:k").await.unwrap(), b"v1");

        reg.swap(Arc::new(ActiveCatalog::new(
            ds2,
            1,
            std::time::SystemTime::UNIX_EPOCH,
            &family(),
        )));

        assert_hit(conn.get(b"ds:k").await.unwrap(), b"v2");
    }

    // --- split_prefix ---

    #[test]
    fn split_prefix_valid() {
        let (ds, key) = split_prefix(b"myds:mykey").unwrap();
        assert_eq!((ds, key), (b"myds".as_ref(), b"mykey".as_ref()));
    }

    #[test]
    fn split_prefix_first_colon_only() {
        let (ds, key) = split_prefix(b"ds:key:with:colons").unwrap();
        assert_eq!((ds, key), (b"ds".as_ref(), b"key:with:colons".as_ref()));
    }

    #[test]
    fn split_prefix_no_colon() {
        assert!(matches!(
            split_prefix(b"nocolon"),
            Err(ServeError::MissingDatasetPrefix)
        ));
    }
}
