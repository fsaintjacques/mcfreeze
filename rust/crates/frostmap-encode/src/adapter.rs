use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{BinaryType, LargeBinaryType};
use arrow_array::{Array, BinaryArray, RecordBatch};
use arrow_schema::DataType;

use apb_core::transcode::Transcoder;
use frostmap_loader::{KvSource, RecordBatchSource, SourceMetadata, VecBatch};

use crate::error::EncodeError;

/// A key-value pair of owned byte vectors.
pub type KvPair = (Vec<u8>, Vec<u8>);

/// Encode a `RecordBatch` into key-value pairs where the value is serialized
/// protobuf bytes.
///
/// - `key_col_idx`: index of the column to extract as the key
/// - `transcoder`: pre-built apb `Transcoder`
///
/// All columns except the key column are transcoded into a single protobuf
/// message per row.
pub fn encode_batch(
    batch: &RecordBatch,
    key_col_idx: usize,
    transcoder: &Transcoder,
) -> Result<Vec<KvPair>, EncodeError> {
    let num_rows = batch.num_rows();
    let key_col = batch.column(key_col_idx);

    // Project value columns (everything except key).
    let value_col_indices: Vec<usize> = (0..batch.num_columns())
        .filter(|&i| i != key_col_idx)
        .collect();
    let value_batch = batch
        .project(&value_col_indices)
        .map_err(EncodeError::Arrow)?;

    // Transcode value columns → protobuf BinaryArray.
    let proto_values: BinaryArray = transcoder.transcode_arrow(&value_batch)?;

    // Build (key, value) pairs.
    let mut pairs = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        let key = extract_key_bytes(key_col.as_ref(), i)?;
        let value = proto_values.value(i);
        pairs.push((key.to_vec(), value.to_vec()));
    }

    Ok(pairs)
}

/// Extract key bytes from an Arrow column at the given row index.
fn extract_key_bytes(col: &dyn Array, row: usize) -> Result<&[u8], EncodeError> {
    if col.is_null(row) {
        return Err(EncodeError::Config(format!(
            "key column is null at row {row}"
        )));
    }
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
    type Batch = VecBatch;
    type Error = EncodeError;

    async fn next_batch(&mut self) -> Result<Option<VecBatch>, EncodeError> {
        let batch = match self.inner.next_batch().await {
            Ok(Some(b)) => b,
            Ok(None) => return Ok(None),
            Err(e) => return Err(EncodeError::Source(Box::new(e))),
        };

        let pairs = encode_batch(&batch, self.key_col_idx, &self.transcoder)?;
        Ok(Some(VecBatch(pairs)))
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
    fn test_encode_batch_auto_generate() {
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
        )
        .unwrap();

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

        let pairs = encode_batch(&batch, 0, &transcoder).unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, b"alice");
        assert_eq!(pairs[1].0, b"bob");
        assert!(!pairs[0].1.is_empty());
        assert!(!pairs[1].1.is_empty());
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

        // Verify it can transcode a batch.
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

        // Generate a descriptor, then base64-encode it to simulate inline.
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
