use std::io::Read;

use base64::{engine::general_purpose::STANDARD as B64, Engine};

use crate::error::LoaderError;

// ---------------------------------------------------------------------------
// SourceMetadata
// ---------------------------------------------------------------------------

/// Optional metadata reported by a source before streaming begins.
///
/// Both fields come from the source's own knowledge (e.g. BigQuery reports
/// these from `CreateReadSession`); sources that cannot know upfront leave
/// them as `None`.
#[derive(Debug, Default, Clone)]
pub struct SourceMetadata {
    /// Estimated total number of key-value pairs.
    pub estimated_rows: Option<u64>,
    /// Estimated total uncompressed byte size of all values.
    pub estimated_bytes: Option<u64>,
}

// ---------------------------------------------------------------------------
// KvBatch
// ---------------------------------------------------------------------------

/// A batch of key-value pairs yielded by a [`KvSource`].
///
/// Implementations may back this with a `Vec` allocation (CSV) or an Arrow
/// `RecordBatch` (ADBC / BigQuery Storage, zero-copy into Arrow buffers).
pub trait KvBatch {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])>;

    /// Total byte size of all keys + values in this batch.
    ///
    /// Implementations backed by columnar formats (e.g. Arrow) can return this
    /// in O(1) from the column buffers. The default falls back to iterating.
    fn total_bytes(&self) -> u64 {
        self.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum()
    }
}

// ---------------------------------------------------------------------------
// KvSource
// ---------------------------------------------------------------------------

/// Async fallible batch iterator of key-value pairs.
///
/// `next_batch` returns `Ok(Some(_))` while data is available,
/// `Ok(None)` when exhausted, or `Err(_)` on failure.
///
/// Implementations that do only sync I/O (e.g. [`CsvSource`]) return
/// immediately from `next_batch` without any actual awaiting.  Sources
/// backed by async I/O (e.g. BigQuery gRPC streams) can `await` freely.
///
/// Individual batch writes in the scatter loop are fast (BufWriter memcpy),
/// so blocking the async executor per-batch is acceptable for a loader
/// workload.  Callers that need stricter executor hygiene should wrap sync
/// sources with `tokio::task::spawn_blocking`.
pub trait KvSource {
    /// `Send + Sync` required so batches can be held across `.await` points
    /// inside `tokio::spawn` tasks (parallel source path).
    type Batch: KvBatch + Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;

    /// The `+ Send` bound on the returned future is required for
    /// `tokio::spawn` in the parallel-source path.
    fn next_batch(
        &mut self,
    ) -> impl Future<Output = Result<Option<Self::Batch>, Self::Error>> + Send;

    /// Metadata available before streaming starts.
    fn metadata(&self) -> SourceMetadata {
        SourceMetadata::default()
    }
}

use std::future::Future;

// ---------------------------------------------------------------------------
// VecBatch — simple owned batch used by CsvSource
// ---------------------------------------------------------------------------

pub struct VecBatch(pub Vec<(Vec<u8>, Vec<u8>)>);

impl KvBatch for VecBatch {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.0.iter().map(|(k, v)| (k.as_slice(), v.as_slice()))
    }
}

// ---------------------------------------------------------------------------
// CsvSource
// ---------------------------------------------------------------------------

/// Reads a two-column CSV (`key,value`) where both columns are
/// base64-encoded byte strings.
///
/// Rows are accumulated into batches of `batch_size` before being returned
/// to the scatter loop, amortising per-row overhead.
pub struct CsvSource<R: Read> {
    reader: csv::Reader<R>,
    batch_size: usize,
    record_idx: u64,
    metadata: SourceMetadata,
}

impl CsvSource<std::fs::File> {
    pub fn from_path(
        path: impl AsRef<std::path::Path>,
        batch_size: usize,
    ) -> Result<Self, LoaderError> {
        let file = std::fs::File::open(path)?;
        Ok(Self::new(file, batch_size))
    }
}

impl<R: Read> CsvSource<R> {
    pub fn new(reader: R, batch_size: usize) -> Self {
        Self {
            reader: csv::ReaderBuilder::new()
                .has_headers(false)
                .from_reader(reader),
            batch_size: batch_size.max(1),
            record_idx: 0,
            metadata: SourceMetadata::default(),
        }
    }

    fn next_batch_sync(&mut self) -> Result<Option<VecBatch>, LoaderError> {
        let mut batch = Vec::with_capacity(self.batch_size);
        let mut record = csv::StringRecord::new();

        while batch.len() < self.batch_size {
            if !self.reader.read_record(&mut record)? {
                break;
            }
            let key_b64 = record.get(0).unwrap_or("");
            let value_b64 = record.get(1).unwrap_or("");

            let key = B64.decode(key_b64).map_err(|e| LoaderError::Base64Decode {
                record: self.record_idx,
                source: e,
            })?;
            let value = B64
                .decode(value_b64)
                .map_err(|e| LoaderError::Base64Decode {
                    record: self.record_idx,
                    source: e,
                })?;

            batch.push((key, value));
            self.record_idx += 1;
        }

        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(VecBatch(batch)))
        }
    }
}

impl<R: Read + Send> KvSource for CsvSource<R> {
    type Batch = VecBatch;
    type Error = LoaderError;

    fn next_batch(&mut self) -> impl Future<Output = Result<Option<VecBatch>, LoaderError>> + Send {
        // CsvSource does only sync I/O; the future completes without yielding.
        std::future::ready(self.next_batch_sync())
    }

    fn metadata(&self) -> SourceMetadata {
        self.metadata.clone()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD as B64, Engine};

    fn make_csv(pairs: &[(&[u8], &[u8])]) -> Vec<u8> {
        let mut out = Vec::new();
        for (k, v) in pairs {
            out.extend_from_slice(B64.encode(k).as_bytes());
            out.push(b',');
            out.extend_from_slice(B64.encode(v).as_bytes());
            out.push(b'\n');
        }
        out
    }

    async fn collect_all<S: KvSource<Batch = VecBatch, Error = LoaderError>>(
        src: &mut S,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut all = Vec::new();
        while let Some(batch) = src.next_batch().await.unwrap() {
            for (k, v) in batch.iter() {
                all.push((k.to_vec(), v.to_vec()));
            }
        }
        all
    }

    #[tokio::test]
    async fn csv_roundtrip() {
        let pairs: &[(&[u8], &[u8])] = &[
            (b"hello", b"world"),
            (b"foo", b"bar baz"),
            (b"\x00\x01\x02", b"\xff\xfe"),
        ];
        let csv = make_csv(pairs);
        let mut src = CsvSource::new(csv.as_slice(), 10);
        let got = collect_all(&mut src).await;
        assert_eq!(got.len(), pairs.len());
        for (i, (k, v)) in got.iter().enumerate() {
            assert_eq!(k.as_slice(), pairs[i].0);
            assert_eq!(v.as_slice(), pairs[i].1);
        }
    }

    #[tokio::test]
    async fn csv_batch_boundaries() {
        // 5 rows, batch_size=2 → batches of [2, 2, 1]
        let pairs: Vec<(&[u8], &[u8])> = (0u8..5)
            .map(|i| {
                let k: &'static [u8] = Box::leak(vec![i].into_boxed_slice());
                let v: &'static [u8] = Box::leak(vec![i + 10].into_boxed_slice());
                (k, v)
            })
            .collect();
        let csv = make_csv(&pairs);
        let mut src = CsvSource::new(csv.as_slice(), 2);

        let b1 = src.next_batch().await.unwrap().unwrap();
        assert_eq!(b1.len(), 2);
        let b2 = src.next_batch().await.unwrap().unwrap();
        assert_eq!(b2.len(), 2);
        let b3 = src.next_batch().await.unwrap().unwrap();
        assert_eq!(b3.len(), 1);
        assert!(src.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn csv_empty() {
        let mut src = CsvSource::new(b"".as_slice(), 10);
        assert!(src.next_batch().await.unwrap().is_none());
    }

    #[test]
    fn vec_batch_is_empty() {
        let b = VecBatch(vec![]);
        assert!(b.is_empty());
        let b = VecBatch(vec![(b"k".to_vec(), b"v".to_vec())]);
        assert!(!b.is_empty());
    }

    #[test]
    fn source_metadata_default() {
        let src = CsvSource::new(b"".as_slice(), 10);
        let m = src.metadata();
        assert!(m.estimated_rows.is_none());
        assert!(m.estimated_bytes.is_none());
    }
}
