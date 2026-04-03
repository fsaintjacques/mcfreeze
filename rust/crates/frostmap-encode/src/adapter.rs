use std::future::Future;
use std::sync::Arc;

use arrow_array::{Array, BinaryArray, RecordBatch};
use arrow_array::cast::AsArray;
use arrow_array::types::{BinaryType, LargeBinaryType};
use arrow_schema::DataType;

use apb_core::transcode::Transcoder;
use frostmap_loader::{KvSource, SourceMetadata, VecBatch};

use crate::error::EncodeError;

// ---------------------------------------------------------------------------
// RecordBatchSource — trait for sources that yield full RecordBatches
// ---------------------------------------------------------------------------

/// Async batch iterator yielding Arrow `RecordBatch`es.
///
/// This is the input side of [`ProtobufEncodingSource`]: any source that can
/// produce full record batches (e.g. `BqRecordBatchSource`) implements this.
pub trait RecordBatchSource: Send {
    type Error: std::error::Error + Send + Sync + 'static;

    fn next_batch(&mut self) -> impl Future<Output = Result<Option<RecordBatch>, Self::Error>> + Send;

    fn metadata(&self) -> SourceMetadata { SourceMetadata::default() }
}

// ---------------------------------------------------------------------------
// RecordBatchSource impl for BqRecordBatchSource
// ---------------------------------------------------------------------------

impl RecordBatchSource for frostmap_bq::BqRecordBatchSource {
    type Error = frostmap_bq::BqError;

    fn next_batch(&mut self) -> impl Future<Output = Result<Option<RecordBatch>, Self::Error>> + Send {
        self.next_batch()
    }
}

// ---------------------------------------------------------------------------
// ProtobufEncodingSource
// ---------------------------------------------------------------------------

/// Wraps a [`RecordBatchSource`] and transcodes each batch into key-value
/// pairs where the value is a serialized protobuf message.
///
/// The key column is extracted by index, and all remaining columns are
/// transcoded via the apb [`Transcoder`].
pub struct ProtobufEncodingSource<S> {
    inner: S,
    key_col_idx: usize,
    value_col_indices: Vec<usize>,
    transcoder: Arc<Transcoder>,
}

impl<S> ProtobufEncodingSource<S> {
    /// Create a new encoding source.
    ///
    /// - `inner`: source producing Arrow `RecordBatch`es
    /// - `key_col_idx`: index of the column to extract as the KV key
    /// - `transcoder`: pre-built apb `Transcoder`, shared via `Arc` (it is `Sync`)
    ///
    /// `value_col_indices` is auto-computed as all columns except the key.
    pub fn new(inner: S, key_col_idx: usize, n_columns: usize, transcoder: Arc<Transcoder>) -> Self {
        let value_col_indices: Vec<usize> = (0..n_columns)
            .filter(|&i| i != key_col_idx)
            .collect();
        Self {
            inner,
            key_col_idx,
            value_col_indices,
            transcoder,
        }
    }
}

// SAFETY: Transcoder is Sync (documented in apb), and all other fields are Send.
unsafe impl<S: Send> Send for ProtobufEncodingSource<S> {}

impl<S> KvSource for ProtobufEncodingSource<S>
where
    S: RecordBatchSource,
{
    type Batch = VecBatch;
    type Error = EncodeError;

    fn next_batch(&mut self) -> impl Future<Output = Result<Option<VecBatch>, EncodeError>> + Send {
        async move {
            let batch = match self.inner.next_batch().await {
                Ok(Some(b)) => b,
                Ok(None) => return Ok(None),
                Err(e) => return Err(EncodeError::Source(Box::new(e))),
            };

            let num_rows = batch.num_rows();
            let key_col = batch.column(self.key_col_idx);

            // Project value columns for transcoding.
            let value_batch = batch.project(&self.value_col_indices)
                .map_err(|e| EncodeError::Arrow(e))?;

            // Transcode value columns → protobuf BinaryArray.
            let proto_values: BinaryArray = self.transcoder.transcode_arrow(&value_batch)?;

            // Build (key, value) pairs.
            let mut pairs = Vec::with_capacity(num_rows);
            for i in 0..num_rows {
                let key = extract_key_bytes(key_col.as_ref(), i)?;
                let value = proto_values.value(i);
                pairs.push((key.to_vec(), value.to_vec()));
            }

            Ok(Some(VecBatch(pairs)))
        }
    }

    fn metadata(&self) -> SourceMetadata {
        self.inner.metadata()
    }
}

/// Extract key bytes from an Arrow column at the given row index.
fn extract_key_bytes(col: &dyn Array, row: usize) -> Result<&[u8], EncodeError> {
    match col.data_type() {
        DataType::Binary => Ok(col.as_bytes::<BinaryType>().value(row)),
        DataType::LargeBinary => Ok(col.as_bytes::<LargeBinaryType>().value(row)),
        DataType::Utf8 => Ok(col.as_string::<i32>().value(row).as_bytes()),
        DataType::LargeUtf8 => Ok(col.as_string::<i64>().value(row).as_bytes()),
        dt => Err(EncodeError::Config(format!(
            "key column has unsupported type {dt}; expected Binary, LargeBinary, Utf8, or LargeUtf8"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Int32Array, StringArray, Float64Array};
    use arrow_schema::{Field, Schema};

    use apb_core::generate::generate_file_descriptor;
    use apb_core::descriptor::ProtoSchema;
    use apb_core::mapping::{InferOptions, infer_mapping};
    use prost_reflect::prost::Message;
    use prost_reflect::prost_types::FileDescriptorSet;

    /// A test source that yields a fixed list of RecordBatches.
    struct VecRecordBatchSource {
        batches: Vec<RecordBatch>,
        idx: usize,
    }

    impl VecRecordBatchSource {
        fn new(batches: Vec<RecordBatch>) -> Self {
            Self { batches, idx: 0 }
        }
    }

    impl RecordBatchSource for VecRecordBatchSource {
        type Error = arrow_schema::ArrowError;

        fn next_batch(&mut self) -> impl Future<Output = Result<Option<RecordBatch>, Self::Error>> + Send {
            async move {
                if self.idx < self.batches.len() {
                    let batch = self.batches[self.idx].clone();
                    self.idx += 1;
                    Ok(Some(batch))
                } else {
                    Ok(None)
                }
            }
        }
    }

    #[tokio::test]
    async fn test_protobuf_encoding_source_auto_generate() {
        // Build a RecordBatch: key (Utf8), age (Int32), score (Float64)
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
            Field::new("score", DataType::Float64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["alice", "bob"])),
                Arc::new(Int32Array::from(vec![30, 25])),
                Arc::new(Float64Array::from(vec![95.5, 87.3])),
            ],
        ).unwrap();

        // Build transcoder from value schema (columns 1, 2 — everything except key)
        let value_schema = Schema::new(vec![
            Field::new("age", DataType::Int32, false),
            Field::new("score", DataType::Float64, false),
        ]);
        let fd = generate_file_descriptor(&value_schema, "test", "Row").unwrap();
        let fds = FileDescriptorSet { file: vec![fd] };
        let desc_bytes = fds.encode_to_vec();
        let proto_schema = ProtoSchema::from_bytes(&desc_bytes).unwrap();
        let msg = proto_schema.message("test.Row").unwrap();
        let mapping = infer_mapping(&value_schema, &msg, &InferOptions::default()).unwrap();
        let transcoder = Transcoder::new(&mapping).unwrap();

        let source = VecRecordBatchSource::new(vec![batch]);
        let mut enc_source = ProtobufEncodingSource::new(source, 0, 3, Arc::new(transcoder));

        // First call should yield 2 rows.
        let batch = enc_source.next_batch().await.unwrap().unwrap();
        assert_eq!(frostmap_loader::KvBatch::len(&batch), 2);

        let pairs: Vec<_> = frostmap_loader::KvBatch::iter(&batch).collect();
        assert_eq!(pairs[0].0, b"alice");
        assert_eq!(pairs[1].0, b"bob");

        // Values should be non-empty protobuf bytes.
        assert!(!pairs[0].1.is_empty());
        assert!(!pairs[1].1.is_empty());

        // Second call should yield None.
        assert!(enc_source.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_build_transcoder_auto_generate() {
        use crate::config::ProtobufEncoding;
        use crate::builder::build_transcoder;

        let value_schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("count", DataType::Int32, false),
        ]);

        let config = ProtobufEncoding {
            descriptor: None,
            message_name: None,
        };

        let transcoder = build_transcoder(&config, &value_schema).unwrap();

        // Verify it can transcode a batch.
        let batch = RecordBatch::try_new(
            Arc::new(value_schema),
            vec![
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(Int32Array::from(vec![42])),
            ],
        ).unwrap();

        let result = transcoder.transcode_arrow(&batch).unwrap();
        assert_eq!(result.len(), 1);
        assert!(!result.value(0).is_empty());
    }
}
