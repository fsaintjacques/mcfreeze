use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{BinaryType, LargeBinaryType};
use arrow_array::{Array, ArrayRef, BinaryArray, RecordBatch};
use arrow_schema::DataType;

use apb_core::transcode::Transcoder;
use frostmap_loader::{KvBatch, KvSource, RecordBatchSource, SourceMetadata};

use crate::error::EncodeError;

// ---------------------------------------------------------------------------
// EncodedBatch — zero-copy KvBatch backed by Arrow arrays
// ---------------------------------------------------------------------------

/// A batch of key-value pairs where keys come from an Arrow column and values
/// are protobuf-encoded bytes from apb's `transcode_arrow`.
///
/// Both columns are borrowed from Arrow buffers — no per-row allocation.
pub struct EncodedBatch {
    keys: ArrayRef,
    values: BinaryArray,
}

impl KvBatch for EncodedBatch {
    fn len(&self) -> usize {
        self.keys.len()
    }

    fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        (0..self.keys.len()).map(|i| {
            let key = key_bytes_at(self.keys.as_ref(), i);
            let value = self.values.value(i);
            (key, value)
        })
    }

    fn total_bytes(&self) -> u64 {
        let key_bytes: u64 = match self.keys.data_type() {
            DataType::Binary => self.keys.as_bytes::<BinaryType>().values().len() as u64,
            DataType::LargeBinary => self.keys.as_bytes::<LargeBinaryType>().values().len() as u64,
            DataType::Utf8 => self.keys.as_string::<i32>().values().len() as u64,
            DataType::LargeUtf8 => self.keys.as_string::<i64>().values().len() as u64,
            _ => unreachable!("key column type validated in encode_batch"),
        };
        key_bytes + self.values.values().len() as u64
    }
}

/// Zero-copy key access by row index. Panics on unsupported type (caller
/// must validate via `validate_key_column` first).
fn key_bytes_at(col: &dyn Array, row: usize) -> &[u8] {
    match col.data_type() {
        DataType::Binary => col.as_bytes::<BinaryType>().value(row),
        DataType::LargeBinary => col.as_bytes::<LargeBinaryType>().value(row),
        DataType::Utf8 => col.as_string::<i32>().value(row).as_bytes(),
        DataType::LargeUtf8 => col.as_string::<i64>().value(row).as_bytes(),
        _ => unreachable!("key column type validated in encode_batch"),
    }
}

// ---------------------------------------------------------------------------
// encode_batch
// ---------------------------------------------------------------------------

/// Validate that the key column type is supported.
fn validate_key_column(col: &dyn Array) -> Result<(), EncodeError> {
    match col.data_type() {
        DataType::Binary | DataType::LargeBinary | DataType::Utf8 | DataType::LargeUtf8 => Ok(()),
        dt => Err(EncodeError::Config(format!(
            "key column has unsupported type {dt}; expected Binary, LargeBinary, Utf8, or LargeUtf8"
        ))),
    }
}

/// Encode a `RecordBatch` into an [`EncodedBatch`] of key-value pairs where
/// each value is a serialized protobuf message.
///
/// Zero per-row allocation: the key column is borrowed from the input batch
/// and the value column is the `BinaryArray` produced by apb's transcoder.
pub fn encode_batch(
    batch: &RecordBatch,
    key_col_idx: usize,
    transcoder: &Transcoder,
) -> Result<EncodedBatch, EncodeError> {
    let key_col = batch.column(key_col_idx).clone();
    validate_key_column(key_col.as_ref())?;

    // Check for null keys.
    if key_col.null_count() > 0 {
        return Err(EncodeError::Config(
            "key column contains null values".into(),
        ));
    }

    // Project value columns (everything except key).
    let value_col_indices: Vec<usize> = (0..batch.num_columns())
        .filter(|&i| i != key_col_idx)
        .collect();
    let value_batch = batch
        .project(&value_col_indices)
        .map_err(EncodeError::Arrow)?;

    // Transcode value columns → protobuf BinaryArray.
    let values: BinaryArray = transcoder.transcode_arrow(&value_batch)?;

    Ok(EncodedBatch {
        keys: key_col,
        values,
    })
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
    transcoder: Arc<Transcoder>,
}

impl<S> ProtobufEncodingSource<S> {
    /// Create a new encoding source.
    ///
    /// - `inner`: source producing Arrow `RecordBatch`es
    /// - `key_col_idx`: index of the column to extract as the KV key
    /// - `transcoder`: pre-built apb `Transcoder`, shared via `Arc` (it is `Sync`)
    pub fn new(inner: S, key_col_idx: usize, transcoder: Arc<Transcoder>) -> Self {
        Self {
            inner,
            key_col_idx,
            transcoder,
        }
    }
}

impl<S> KvSource for ProtobufEncodingSource<S>
where
    S: RecordBatchSource,
{
    type Batch = EncodedBatch;
    type Error = EncodeError;

    async fn next_batch(&mut self) -> Result<Option<EncodedBatch>, EncodeError> {
        let batch = match self.inner.next_batch().await {
            Ok(Some(b)) => b,
            Ok(None) => return Ok(None),
            Err(e) => return Err(EncodeError::Source(Box::new(e))),
        };

        encode_batch(&batch, self.key_col_idx, &self.transcoder).map(Some)
    }

    fn metadata(&self) -> SourceMetadata {
        self.inner.metadata()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int32Array, StringArray};
    use arrow_schema::{Field, Schema};

    use apb_core::descriptor::ProtoSchema;
    use apb_core::generate::generate_file_descriptor;
    use apb_core::mapping::{infer_mapping, InferOptions};
    use prost_reflect::prost::Message;
    use prost_reflect::prost_types::FileDescriptorSet;

    #[test]
    fn test_encode_batch_zero_copy() {
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
        )
        .unwrap();

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

        let encoded = encode_batch(&batch, 0, &transcoder).unwrap();
        assert_eq!(encoded.len(), 2);

        let pairs: Vec<_> = encoded.iter().collect();
        assert_eq!(pairs[0].0, b"alice");
        assert_eq!(pairs[1].0, b"bob");
        assert!(!pairs[0].1.is_empty());
        assert!(!pairs[1].1.is_empty());
        assert!(encoded.total_bytes() > 0);
    }

    #[test]
    fn test_build_transcoder_auto_generate() {
        use crate::builder::build_transcoder;
        use crate::config::ProtobufEncoding;

        let value_schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("count", DataType::Int32, false),
        ]);

        let config = ProtobufEncoding {
            descriptor: None,
            descriptor_uri: None,
            package: Some("test".into()),
            message_name: "Row".into(),
        };

        let transcoder = build_transcoder(&config, &value_schema).unwrap();

        let batch = RecordBatch::try_new(
            Arc::new(value_schema),
            vec![
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(Int32Array::from(vec![42])),
            ],
        )
        .unwrap();

        let result = transcoder.transcode_arrow(&batch).unwrap();
        assert_eq!(result.len(), 1);
        assert!(!result.value(0).is_empty());
    }

    #[test]
    fn test_build_transcoder_inline_descriptor() {
        use crate::builder::build_transcoder;
        use crate::config::ProtobufEncoding;
        use prost_reflect::prost::Message;
        use prost_reflect::prost_types::FileDescriptorSet;

        let value_schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("count", DataType::Int32, false),
        ]);

        let fd = apb_core::generate::generate_file_descriptor(&value_schema, "pkg", "Msg").unwrap();
        let fds = FileDescriptorSet { file: vec![fd] };
        let desc_bytes = fds.encode_to_vec();
        let desc_b64 =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &desc_bytes);

        let config = ProtobufEncoding {
            descriptor: Some(desc_b64),
            descriptor_uri: None,
            package: None,
            message_name: "pkg.Msg".into(),
        };

        let transcoder = build_transcoder(&config, &value_schema).unwrap();

        let batch = RecordBatch::try_new(
            Arc::new(value_schema),
            vec![
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(Int32Array::from(vec![42])),
            ],
        )
        .unwrap();

        let result = transcoder.transcode_arrow(&batch).unwrap();
        assert_eq!(result.len(), 1);
        assert!(!result.value(0).is_empty());
    }

    #[test]
    fn test_build_transcoder_mutual_exclusion_error() {
        use crate::builder::build_transcoder;
        use crate::config::ProtobufEncoding;

        let value_schema = Schema::new(vec![Field::new("x", DataType::Int32, false)]);

        let config = ProtobufEncoding {
            descriptor: Some("abc".into()),
            descriptor_uri: Some("gs://bucket/file".into()),
            package: None,
            message_name: "Msg".into(),
        };

        let result = build_transcoder(&config, &value_schema);
        assert!(result.is_err());
        assert!(result
            .err()
            .unwrap()
            .to_string()
            .contains("mutually exclusive"));
    }

    #[test]
    fn test_build_transcoder_missing_package_error() {
        use crate::builder::build_transcoder;
        use crate::config::ProtobufEncoding;

        let value_schema = Schema::new(vec![Field::new("x", DataType::Int32, false)]);

        let config = ProtobufEncoding {
            descriptor: None,
            descriptor_uri: None,
            package: None,
            message_name: "Msg".into(),
        };

        let result = build_transcoder(&config, &value_schema);
        assert!(result.is_err());
        assert!(result
            .err()
            .unwrap()
            .to_string()
            .contains("package is required"));
    }
}
