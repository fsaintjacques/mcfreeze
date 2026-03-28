use std::io::Read;

use base64::{Engine, engine::general_purpose::STANDARD as B64};

use crate::error::LoaderError;

// ---------------------------------------------------------------------------
// KvBatch
// ---------------------------------------------------------------------------

/// A batch of key-value pairs yielded by a [`KvSource`].
///
/// Implementations may back this with a `Vec` allocation (CSV) or an Arrow
/// `RecordBatch` (ADBC, zero-copy into Arrow buffers).
pub trait KvBatch {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool { self.len() == 0 }
    /// Iterate over `(key, value)` byte slices in this batch.
    fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])>;
}

// ---------------------------------------------------------------------------
// KvSource
// ---------------------------------------------------------------------------

/// Fallible batch iterator of key-value pairs.
///
/// `next_batch` returns `Ok(Some(_))` while data is available,
/// `Ok(None)` when exhausted, or `Err(_)` on failure.
pub trait KvSource {
    type Batch: KvBatch;
    type Error: std::error::Error + Send + Sync + 'static;

    fn next_batch(&mut self) -> Result<Option<Self::Batch>, Self::Error>;

    /// Optional total key count hint for progress reporting.
    fn size_hint(&self) -> Option<u64> { None }
}

// ---------------------------------------------------------------------------
// VecBatch — simple owned batch used by CsvSource
// ---------------------------------------------------------------------------

pub struct VecBatch(pub Vec<(Vec<u8>, Vec<u8>)>);

impl KvBatch for VecBatch {
    fn len(&self) -> usize { self.0.len() }

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
    reader:     csv::Reader<R>,
    batch_size: usize,
    record_idx: u64,
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
            reader:     csv::ReaderBuilder::new().has_headers(false).from_reader(reader),
            batch_size: batch_size.max(1),
            record_idx: 0,
        }
    }
}

impl<R: Read> KvSource for CsvSource<R> {
    type Batch = VecBatch;
    type Error = LoaderError;

    fn next_batch(&mut self) -> Result<Option<VecBatch>, LoaderError> {
        let mut batch = Vec::with_capacity(self.batch_size);
        let mut record = csv::StringRecord::new();

        while batch.len() < self.batch_size {
            if !self.reader.read_record(&mut record)? {
                break;
            }
            let key_b64   = record.get(0).unwrap_or("");
            let value_b64 = record.get(1).unwrap_or("");

            let key = B64.decode(key_b64).map_err(|e| LoaderError::Base64Decode {
                record: self.record_idx,
                source: e,
            })?;
            let value = B64.decode(value_b64).map_err(|e| LoaderError::Base64Decode {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine, engine::general_purpose::STANDARD as B64};

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

    fn collect_all(src: &mut impl KvSource<Batch = VecBatch, Error = LoaderError>)
        -> Vec<(Vec<u8>, Vec<u8>)>
    {
        let mut all = Vec::new();
        while let Some(batch) = src.next_batch().unwrap() {
            for (k, v) in batch.iter() {
                all.push((k.to_vec(), v.to_vec()));
            }
        }
        all
    }

    #[test]
    fn csv_roundtrip() {
        let pairs: &[(&[u8], &[u8])] = &[
            (b"hello", b"world"),
            (b"foo",   b"bar baz"),
            (b"\x00\x01\x02", b"\xff\xfe"),
        ];
        let csv  = make_csv(pairs);
        let mut src = CsvSource::new(csv.as_slice(), 10);
        let got  = collect_all(&mut src);
        assert_eq!(got.len(), pairs.len());
        for (i, (k, v)) in got.iter().enumerate() {
            assert_eq!(k.as_slice(), pairs[i].0);
            assert_eq!(v.as_slice(), pairs[i].1);
        }
    }

    #[test]
    fn csv_batch_boundaries() {
        // 5 rows, batch_size=2 → batches of [2, 2, 1]
        let pairs: Vec<(&[u8], &[u8])> = (0u8..5).map(|i| {
            let k: &'static [u8] = Box::leak(vec![i].into_boxed_slice());
            let v: &'static [u8] = Box::leak(vec![i + 10].into_boxed_slice());
            (k, v)
        }).collect();
        let csv = make_csv(&pairs);
        let mut src = CsvSource::new(csv.as_slice(), 2);

        let b1 = src.next_batch().unwrap().unwrap();
        assert_eq!(b1.len(), 2);
        let b2 = src.next_batch().unwrap().unwrap();
        assert_eq!(b2.len(), 2);
        let b3 = src.next_batch().unwrap().unwrap();
        assert_eq!(b3.len(), 1);
        assert!(src.next_batch().unwrap().is_none());
    }

    #[test]
    fn csv_empty() {
        let mut src = CsvSource::new(b"".as_slice(), 10);
        assert!(src.next_batch().unwrap().is_none());
    }

    #[test]
    fn vec_batch_is_empty() {
        let b = VecBatch(vec![]);
        assert!(b.is_empty());
        let b = VecBatch(vec![(b"k".to_vec(), b"v".to_vec())]);
        assert!(!b.is_empty());
    }
}
