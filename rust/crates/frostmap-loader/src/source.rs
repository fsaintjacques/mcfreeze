use std::future::Future;
use std::io::Read;
use std::pin::Pin;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_csv::reader::Format;
use arrow_csv::ReaderBuilder;
use arrow_schema::SchemaRef;

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
/// Implementations may back this with a `Vec` allocation or an Arrow
/// `RecordBatch` (zero-copy into Arrow buffers).
pub trait KvBatch {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Visit each `(key, value)` pair in the batch.
    ///
    /// Callback-based instead of iterator-based so implementations can format
    /// keys lazily into a stack-allocated buffer (e.g. integer-typed Arrow
    /// columns stringified via `itoa`) without materializing the whole column
    /// upfront. The `&[u8]` borrows are valid only for the duration of each
    /// callback invocation.
    fn for_each_kv<F: FnMut(&[u8], &[u8])>(&self, f: F);

    /// Total byte size of all keys + values in this batch.
    fn total_bytes(&self) -> u64 {
        let mut sum = 0u64;
        self.for_each_kv(|k, v| sum += (k.len() + v.len()) as u64);
        sum
    }
}

// ---------------------------------------------------------------------------
// KvSource
// ---------------------------------------------------------------------------

/// Async fallible batch iterator of key-value pairs.
///
/// `next_batch` returns `Ok(Some(_))` while data is available,
/// `Ok(None)` when exhausted, or `Err(_)` on failure.
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

// ---------------------------------------------------------------------------
// RecordBatchSource — async Arrow RecordBatch iterator
// ---------------------------------------------------------------------------

/// Async batch iterator yielding Arrow `RecordBatch`es.
///
/// This is the Arrow-level counterpart of [`KvSource`]: sources that produce
/// full record batches (BigQuery Storage, Arrow Flight, CSV, …) implement
/// this trait.  Downstream adapters (e.g. protobuf encoding) consume it and
/// produce a [`KvSource`].
pub trait RecordBatchSource: Send {
    type Error: std::error::Error + Send + Sync + 'static;

    fn next_batch(
        &mut self,
    ) -> impl Future<Output = Result<Option<RecordBatch>, Self::Error>> + Send;

    fn metadata(&self) -> SourceMetadata {
        SourceMetadata::default()
    }
}

// ---------------------------------------------------------------------------
// BoxedRecordBatchSource — type-erased RecordBatchSource
// ---------------------------------------------------------------------------

/// Type-erased [`RecordBatchSource`] using boxed futures.
///
/// Allows different concrete source types (BigQuery, CSV, …) to be stored in
/// a single `Vec<BoxedRecordBatchSource>` and processed uniformly.
pub struct BoxedRecordBatchSource(Box<dyn ErasedRecordBatchSource>);

impl BoxedRecordBatchSource {
    pub fn new<S>(source: S) -> Self
    where
        S: RecordBatchSource + 'static,
        S::Error: std::error::Error + Send + Sync + 'static,
    {
        Self(Box::new(ErasedAdapter(source)))
    }
}

impl RecordBatchSource for BoxedRecordBatchSource {
    type Error = LoaderError;

    fn next_batch(
        &mut self,
    ) -> impl Future<Output = Result<Option<RecordBatch>, LoaderError>> + Send {
        self.0.next_batch_boxed()
    }

    fn metadata(&self) -> SourceMetadata {
        self.0.metadata()
    }
}

/// Object-safe helper trait for type erasure.
trait ErasedRecordBatchSource: Send {
    fn next_batch_boxed(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<RecordBatch>, LoaderError>> + Send + '_>>;

    fn metadata(&self) -> SourceMetadata;
}

/// Adapter that boxes the future returned by a concrete `RecordBatchSource`.
struct ErasedAdapter<S>(S);

impl<S> ErasedRecordBatchSource for ErasedAdapter<S>
where
    S: RecordBatchSource + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    fn next_batch_boxed(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<RecordBatch>, LoaderError>> + Send + '_>> {
        Box::pin(async {
            self.0
                .next_batch()
                .await
                .map_err(|e| LoaderError::Source(Box::new(e)))
        })
    }

    fn metadata(&self) -> SourceMetadata {
        self.0.metadata()
    }
}

// ---------------------------------------------------------------------------
// VecBatch — simple owned batch
// ---------------------------------------------------------------------------

pub struct VecBatch(pub Vec<(Vec<u8>, Vec<u8>)>);

impl KvBatch for VecBatch {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn for_each_kv<F: FnMut(&[u8], &[u8])>(&self, mut f: F) {
        for (k, v) in &self.0 {
            f(k, v);
        }
    }
}

// ---------------------------------------------------------------------------
// ArrowKvBatch — zero-copy KvBatch backed by two Arrow byte columns
// ---------------------------------------------------------------------------

/// A batch of key-value pairs backed by two Arrow byte columns.
///
/// Both columns are borrowed from Arrow buffers — no per-row allocation.
/// Supports Binary, LargeBinary, Utf8, and LargeUtf8 column types.
#[derive(Debug)]
pub struct ArrowKvBatch {
    keys: ArrayRef,
    values: ArrayRef,
}

impl ArrowKvBatch {
    /// Build from a `RecordBatch` by extracting two columns by index.
    pub fn from_record_batch(
        batch: &RecordBatch,
        key_col_idx: usize,
        val_col_idx: usize,
    ) -> Result<Self, LoaderError> {
        let keys = batch.column(key_col_idx).clone();
        let values = batch.column(val_col_idx).clone();
        validate_key_column(&keys, batch.schema().field(key_col_idx).name())?;
        validate_byte_column(&values, batch.schema().field(val_col_idx).name())?;
        Ok(Self { keys, values })
    }
}

impl KvBatch for ArrowKvBatch {
    fn len(&self) -> usize {
        self.keys.len()
    }

    fn for_each_kv<F: FnMut(&[u8], &[u8])>(&self, mut f: F) {
        let values = self.values.as_ref();
        for_each_key(self.keys.as_ref(), |i, key| {
            f(key, byte_value(values, i));
        });
    }

    fn total_bytes(&self) -> u64 {
        key_column_size(self.keys.as_ref()) + byte_value_buffer_size(&self.values)
    }
}

/// O(1) byte size of a key column. For byte columns this is the values
/// buffer length; for integer columns it is the raw element width times
/// row count (matches the on-the-wire Arrow buffer, not the stringified
/// representation — appropriate for benchmark/ingest accounting).
pub(crate) fn key_column_size(col: &dyn arrow_array::Array) -> u64 {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{BinaryType, LargeBinaryType};
    use arrow_schema::DataType;
    match col.data_type() {
        DataType::Binary => col.as_bytes::<BinaryType>().values().len() as u64,
        DataType::LargeBinary => col.as_bytes::<LargeBinaryType>().values().len() as u64,
        DataType::Utf8 => col.as_string::<i32>().values().len() as u64,
        DataType::LargeUtf8 => col.as_string::<i64>().values().len() as u64,
        DataType::Int32 | DataType::UInt32 => col.len() as u64 * 4,
        DataType::Int64 | DataType::UInt64 => col.len() as u64 * 8,
        _ => unreachable!("key column type validated in construction"),
    }
}

fn byte_value_buffer_size(col: &ArrayRef) -> u64 {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{BinaryType, LargeBinaryType};
    use arrow_schema::DataType;
    match col.data_type() {
        DataType::Binary => col.as_bytes::<BinaryType>().values().len() as u64,
        DataType::LargeBinary => col.as_bytes::<LargeBinaryType>().values().len() as u64,
        DataType::Utf8 => col.as_string::<i32>().values().len() as u64,
        DataType::LargeUtf8 => col.as_string::<i64>().values().len() as u64,
        _ => unreachable!("value column type validated in ArrowKvBatch construction"),
    }
}

/// Validate that a column is a supported key type.
fn validate_key_column(col: &ArrayRef, name: &str) -> Result<(), LoaderError> {
    use arrow_schema::DataType;
    match col.data_type() {
        DataType::Binary
        | DataType::LargeBinary
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt32
        | DataType::UInt64 => Ok(()),
        dt => Err(LoaderError::Arrow(arrow_schema::ArrowError::SchemaError(
            format!(
                "key column {name:?} has type {dt}; expected Binary, LargeBinary, \
                 Utf8, LargeUtf8, Int32, Int64, UInt32, or UInt64"
            ),
        ))),
    }
}

/// Visit each row of a key column. Byte columns are borrowed zero-copy;
/// integer columns are formatted lazily into a stack-allocated `itoa::Buffer`,
/// so the `&[u8]` lives only for the duration of the callback.
pub(crate) fn for_each_key<F: FnMut(usize, &[u8])>(col: &dyn arrow_array::Array, mut f: F) {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{BinaryType, Int32Type, Int64Type, LargeBinaryType, UInt32Type, UInt64Type};
    use arrow_array::Array;
    use arrow_schema::DataType;
    match col.data_type() {
        DataType::Binary => {
            let a = col.as_bytes::<BinaryType>();
            for i in 0..a.len() {
                f(i, a.value(i));
            }
        }
        DataType::LargeBinary => {
            let a = col.as_bytes::<LargeBinaryType>();
            for i in 0..a.len() {
                f(i, a.value(i));
            }
        }
        DataType::Utf8 => {
            let a = col.as_string::<i32>();
            for i in 0..a.len() {
                f(i, a.value(i).as_bytes());
            }
        }
        DataType::LargeUtf8 => {
            let a = col.as_string::<i64>();
            for i in 0..a.len() {
                f(i, a.value(i).as_bytes());
            }
        }
        DataType::Int32 => {
            let a = col.as_primitive::<Int32Type>();
            let mut buf = itoa::Buffer::new();
            for (i, v) in a.values().iter().enumerate() {
                f(i, buf.format(*v).as_bytes());
            }
        }
        DataType::Int64 => {
            let a = col.as_primitive::<Int64Type>();
            let mut buf = itoa::Buffer::new();
            for (i, v) in a.values().iter().enumerate() {
                f(i, buf.format(*v).as_bytes());
            }
        }
        DataType::UInt32 => {
            let a = col.as_primitive::<UInt32Type>();
            let mut buf = itoa::Buffer::new();
            for (i, v) in a.values().iter().enumerate() {
                f(i, buf.format(*v).as_bytes());
            }
        }
        DataType::UInt64 => {
            let a = col.as_primitive::<UInt64Type>();
            let mut buf = itoa::Buffer::new();
            for (i, v) in a.values().iter().enumerate() {
                f(i, buf.format(*v).as_bytes());
            }
        }
        _ => unreachable!("key column type validated in ArrowKvBatch construction"),
    }
}

/// Validate that a column is a supported byte type.
fn validate_byte_column(col: &ArrayRef, name: &str) -> Result<(), LoaderError> {
    use arrow_schema::DataType;
    match col.data_type() {
        DataType::Binary | DataType::LargeBinary | DataType::Utf8 | DataType::LargeUtf8 => Ok(()),
        dt => Err(LoaderError::Arrow(arrow_schema::ArrowError::SchemaError(
            format!(
                "column {name:?} has type {dt}; expected Binary, LargeBinary, Utf8, or LargeUtf8"
            ),
        ))),
    }
}

/// Zero-copy byte access at row `i`. Caller must validate type first.
fn byte_value(col: &dyn arrow_array::Array, i: usize) -> &[u8] {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{BinaryType, LargeBinaryType};
    use arrow_schema::DataType;
    match col.data_type() {
        DataType::Binary => col.as_bytes::<BinaryType>().value(i),
        DataType::LargeBinary => col.as_bytes::<LargeBinaryType>().value(i),
        DataType::Utf8 => col.as_string::<i32>().value(i).as_bytes(),
        DataType::LargeUtf8 => col.as_string::<i64>().value(i).as_bytes(),
        _ => unreachable!("column type validated in ArrowKvBatch construction"),
    }
}

// ---------------------------------------------------------------------------
// RawEncodingSource — extracts key+value columns from RecordBatches
// ---------------------------------------------------------------------------

/// Wraps a [`RecordBatchSource`] and extracts key and value columns as a
/// [`KvSource`]. This is the raw (no transcoding) counterpart to
/// `ProtobufEncodingSource`.
pub struct RawEncodingSource<S> {
    inner: S,
    key_col_idx: usize,
    val_col_idx: usize,
}

impl<S> RawEncodingSource<S> {
    pub fn new(inner: S, key_col_idx: usize, val_col_idx: usize) -> Self {
        Self {
            inner,
            key_col_idx,
            val_col_idx,
        }
    }
}

impl<S> KvSource for RawEncodingSource<S>
where
    S: RecordBatchSource,
{
    type Batch = ArrowKvBatch;
    type Error = LoaderError;

    async fn next_batch(&mut self) -> Result<Option<ArrowKvBatch>, LoaderError> {
        let batch = match self.inner.next_batch().await {
            Ok(Some(b)) => b,
            Ok(None) => return Ok(None),
            Err(e) => return Err(LoaderError::Source(Box::new(e))),
        };

        ArrowKvBatch::from_record_batch(&batch, self.key_col_idx, self.val_col_idx).map(Some)
    }

    fn metadata(&self) -> SourceMetadata {
        self.inner.metadata()
    }
}

// ---------------------------------------------------------------------------
// CsvSource — Arrow-based CSV reader implementing RecordBatchSource
// ---------------------------------------------------------------------------

/// Reads a CSV file into Arrow `RecordBatch`es.
///
/// The schema is inferred from the file header and first rows. The source
/// implements [`RecordBatchSource`] so it feeds into the same pipeline as
/// BigQuery and other Arrow sources.
pub struct CsvSource<R: Read> {
    reader: arrow_csv::reader::BufReader<std::io::BufReader<R>>,
    schema: SchemaRef,
}

impl CsvSource<std::fs::File> {
    /// Open a CSV file with headers and infer the schema.
    pub fn from_path(
        path: impl AsRef<std::path::Path>,
        batch_size: usize,
    ) -> Result<Self, LoaderError> {
        use std::io::Seek;
        let mut file = std::fs::File::open(path.as_ref())?;
        let (schema, _) = Format::default()
            .with_header(true)
            .infer_schema(&mut file, Some(100))
            .map_err(LoaderError::Arrow)?;
        file.rewind()?;
        Self::with_schema(file, Arc::new(schema), batch_size)
    }
}

impl CsvSource<std::io::Cursor<Vec<u8>>> {
    /// Read all data from a reader, infer the schema, and build a CSV source.
    ///
    /// Useful for non-seekable inputs like stdin.
    pub fn from_reader(mut reader: impl Read, batch_size: usize) -> Result<Self, LoaderError> {
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        let (schema, _) = Format::default()
            .with_header(true)
            .infer_schema(std::io::Cursor::new(&buf), Some(100))
            .map_err(LoaderError::Arrow)?;
        Self::with_schema(std::io::Cursor::new(buf), Arc::new(schema), batch_size)
    }
}

impl<R: Read> CsvSource<R> {
    /// Create a CSV source with an explicit schema.
    pub fn with_schema(
        reader: R,
        schema: SchemaRef,
        batch_size: usize,
    ) -> Result<Self, LoaderError> {
        let csv_schema = schema.clone();
        let reader = ReaderBuilder::new(schema)
            .with_header(true)
            .with_batch_size(batch_size)
            .build(reader)
            .map_err(LoaderError::Arrow)?;
        Ok(Self {
            reader,
            schema: csv_schema,
        })
    }

    /// Returns the Arrow schema of the CSV data.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl<R: Read + Send> RecordBatchSource for CsvSource<R> {
    type Error = LoaderError;

    fn next_batch(
        &mut self,
    ) -> impl Future<Output = Result<Option<RecordBatch>, LoaderError>> + Send {
        let result = match self.reader.next() {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(LoaderError::Arrow(e)),
            None => Ok(None),
        };
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn csv_source_with_schema() {
        let csv = b"name,age,score\nalice,30,95.5\nbob,25,87.3\n";
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
        ]));

        let mut src = CsvSource::with_schema(csv.as_slice(), schema, 1024).unwrap();

        let batch = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(src.next_batch())
            .unwrap()
            .unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        let names = batch.column(0).as_string::<i32>();
        assert_eq!(names.value(0), "alice");
        assert_eq!(names.value(1), "bob");
    }

    #[test]
    fn csv_source_exhausted() {
        let csv = b"x\n1\n";
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));

        let mut src = CsvSource::with_schema(csv.as_slice(), schema, 1024).unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let b1 = rt.block_on(src.next_batch()).unwrap();
        assert!(b1.is_some());
        let b2 = rt.block_on(src.next_batch()).unwrap();
        assert!(b2.is_none());
    }

    #[test]
    fn csv_source_batch_boundaries() {
        let csv = b"x\n1\n2\n3\n4\n5\n";
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));

        let mut src = CsvSource::with_schema(csv.as_slice(), schema, 2).unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let b1 = rt.block_on(src.next_batch()).unwrap().unwrap();
        assert_eq!(b1.num_rows(), 2);
        let b2 = rt.block_on(src.next_batch()).unwrap().unwrap();
        assert_eq!(b2.num_rows(), 2);
        let b3 = rt.block_on(src.next_batch()).unwrap().unwrap();
        assert_eq!(b3.num_rows(), 1);
        assert!(rt.block_on(src.next_batch()).unwrap().is_none());
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
        let m = SourceMetadata::default();
        assert!(m.estimated_rows.is_none());
        assert!(m.estimated_bytes.is_none());
    }

    #[test]
    fn csv_source_empty_no_data_rows() {
        let csv = b"key,value\n";
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let mut src = CsvSource::with_schema(csv.as_slice(), schema, 1024).unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        assert!(rt.block_on(src.next_batch()).unwrap().is_none());
    }

    #[test]
    fn arrow_kv_batch_utf8_columns() {
        use arrow_array::StringArray;

        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["alice", "bob"])),
                Arc::new(StringArray::from(vec!["val_a", "val_b"])),
            ],
        )
        .unwrap();

        let kv = ArrowKvBatch::from_record_batch(&batch, 0, 1).unwrap();
        assert_eq!(kv.len(), 2);
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        kv.for_each_kv(|k, v| pairs.push((k.to_vec(), v.to_vec())));
        assert_eq!(pairs[0].0, b"alice");
        assert_eq!(pairs[0].1, b"val_a");
        assert_eq!(pairs[1].0, b"bob");
        assert_eq!(pairs[1].1, b"val_b");
        assert!(kv.total_bytes() > 0);
    }

    #[test]
    fn arrow_kv_batch_rejects_non_byte_column() {
        use arrow_array::Int32Array;

        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::StringArray::from(vec!["a"])),
                Arc::new(Int32Array::from(vec![42])),
            ],
        )
        .unwrap();

        let result = ArrowKvBatch::from_record_batch(&batch, 0, 1);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Int32"));
    }

    #[test]
    fn arrow_kv_batch_int64_key_stringified() {
        use arrow_array::{Int64Array, StringArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, -42, 1234567890])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let kv = ArrowKvBatch::from_record_batch(&batch, 0, 1).unwrap();
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        kv.for_each_kv(|k, v| pairs.push((k.to_vec(), v.to_vec())));
        assert_eq!(pairs[0].0, b"1");
        assert_eq!(pairs[1].0, b"-42");
        assert_eq!(pairs[2].0, b"1234567890");
        assert_eq!(pairs[2].1, b"c");
    }

    #[test]
    fn raw_encoding_source_maps_batches() {
        use arrow_array::StringArray;

        struct OneBatch(Option<RecordBatch>);
        impl RecordBatchSource for OneBatch {
            type Error = arrow_schema::ArrowError;
            fn next_batch(
                &mut self,
            ) -> impl std::future::Future<Output = Result<Option<RecordBatch>, Self::Error>> + Send
            {
                std::future::ready(Ok(self.0.take()))
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["k1"])),
                Arc::new(StringArray::from(vec!["v1"])),
            ],
        )
        .unwrap();

        let mut src = RawEncodingSource::new(OneBatch(Some(batch)), 0, 1);
        let rt = tokio::runtime::Runtime::new().unwrap();

        let kv = rt.block_on(src.next_batch()).unwrap().unwrap();
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        kv.for_each_kv(|k, v| pairs.push((k.to_vec(), v.to_vec())));
        assert_eq!(pairs[0].0, b"k1");
        assert_eq!(pairs[0].1, b"v1");

        assert!(rt.block_on(src.next_batch()).unwrap().is_none());
    }
}
