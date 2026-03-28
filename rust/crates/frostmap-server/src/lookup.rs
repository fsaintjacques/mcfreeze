use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use frostmap_format::reader::SnapshotReader;

use crate::ServeError;

// ---------------------------------------------------------------------------
// Lookup trait
// ---------------------------------------------------------------------------

/// The single interface between the connection/protocol layer and the storage
/// layer.  Both `SnapshotLookup` and `CatalogLookup` implement this trait;
/// the connection handler works exclusively against `Arc<dyn Lookup>`.
#[async_trait]
pub trait Lookup: Send + Sync + 'static {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, ServeError>;
}

// ---------------------------------------------------------------------------
// SnapshotLookup
// ---------------------------------------------------------------------------

/// Wraps a single [`SnapshotReader`] behind an `Arc` so it can be shared
/// across tokio tasks without cloning the mmap handles.
///
/// `get` offloads the `pread` syscall to the blocking thread pool via
/// `spawn_blocking`, keeping the async runtime free for I/O multiplexing.
/// When io_uring becomes the primary target, only this `impl` block changes;
/// nothing above it is affected.
pub struct SnapshotLookup {
    inner: Arc<SnapshotReader>,
}

impl SnapshotLookup {
    pub fn new(reader: Arc<SnapshotReader>) -> Self {
        Self { inner: reader }
    }
}

#[async_trait]
impl Lookup for SnapshotLookup {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, ServeError> {
        let key   = Bytes::copy_from_slice(key);
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            inner.get(key.as_ref())
                 .map(|v| v.map(Bytes::from))
                 .map_err(Into::into)
        })
        .await
        .map_err(|e| ServeError::BlockingTaskPanicked(e.to_string()))?
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use frostmap_format::{reader::SnapshotReader, writer::SnapshotWriter};
    use tempfile::TempDir;

    fn build_snapshot(pairs: &[(&[u8], &[u8])], n_partitions: u32) -> TempDir {
        let dir = TempDir::new().unwrap();
        let mut w = SnapshotWriter::new(dir.path(), n_partitions).unwrap();
        for &(k, v) in pairs {
            w.write(k, v).unwrap();
        }
        w.finish(dir.path()).unwrap();
        dir
    }

    #[tokio::test]
    async fn hit_returns_value() {
        let dir    = build_snapshot(&[(b"hello", b"world")], 4);
        let reader = SnapshotReader::open(dir.path()).unwrap();
        let lookup = SnapshotLookup::new(Arc::new(reader));

        let got = lookup.get(b"hello").await.unwrap();
        assert_eq!(got, Some(Bytes::from_static(b"world")));
    }

    #[tokio::test]
    async fn miss_returns_none() {
        let dir    = build_snapshot(&[(b"present", b"yes")], 4);
        let reader = SnapshotReader::open(dir.path()).unwrap();
        let lookup = SnapshotLookup::new(Arc::new(reader));

        let got = lookup.get(b"absent").await.unwrap();
        assert_eq!(got, None);
    }

    #[tokio::test]
    async fn dyn_lookup_works() {
        let dir    = build_snapshot(&[(b"k", b"v")], 4);
        let reader = SnapshotReader::open(dir.path()).unwrap();
        let lookup: Arc<dyn Lookup> = Arc::new(SnapshotLookup::new(Arc::new(reader)));

        assert_eq!(lookup.get(b"k").await.unwrap(), Some(Bytes::from_static(b"v")));
        assert_eq!(lookup.get(b"missing").await.unwrap(), None);
    }

    #[tokio::test]
    async fn io_error_surfaces_as_format_error() {
        // Build a single-partition snapshot so the data file is at a known path.
        let dir    = build_snapshot(&[(b"key", b"value")], 1);
        let reader = SnapshotReader::open(dir.path()).unwrap();
        let lookup = SnapshotLookup::new(Arc::new(reader));

        // Truncate data.bin to 0 bytes while the reader still holds the fd open.
        // The next pread will hit an UnexpectedEof, which propagates as ServeError::Format.
        std::fs::OpenOptions::new()
            .write(true)
            .open(dir.path().join("data/part-0/data.bin"))
            .unwrap()
            .set_len(0)
            .unwrap();

        let err = lookup.get(b"key").await.unwrap_err();
        assert!(matches!(err, ServeError::Format(_)));
    }
}
