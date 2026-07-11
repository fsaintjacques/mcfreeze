// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, RecordBatch};
use arrow_schema::DataType;

use apb_core::transcode::Transcoder;
use mcfreeze_loader::{
    for_each_key, key_column_size, KvBatch, KvSource, RecordBatchSource, SourceMetadata,
};

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

    fn for_each_kv<F: FnMut(&[u8], &[u8])>(&self, mut f: F) {
        let values = &self.values;
        for_each_key(self.keys.as_ref(), |i, key| {
            f(key, values.value(i));
        });
    }

    fn total_bytes(&self) -> u64 {
        key_column_size(self.keys.as_ref()) + self.values.values().len() as u64
    }
}

/// Validate that a key column type is supported.
fn validate_key_column(col: &dyn Array) -> Result<(), EncodeError> {
    match col.data_type() {
        DataType::Binary
        | DataType::LargeBinary
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt32
        | DataType::UInt64 => Ok(()),
        dt => Err(EncodeError::Config(format!(
            "key column has unsupported type {dt}; expected Binary, LargeBinary, \
             Utf8, LargeUtf8, Int32, Int64, UInt32, or UInt64"
        ))),
    }
}

// ---------------------------------------------------------------------------
// encode_batch
// ---------------------------------------------------------------------------

/// Encode a `RecordBatch` into an [`EncodedBatch`] of key-value pairs where
/// each value is a serialized protobuf message.
///
/// Byte key columns are borrowed zero-copy from the input batch; integer
/// key columns are stringified into a packed buffer. The value column is
/// the `BinaryArray` produced by apb's transcoder.
pub fn encode_batch(
    batch: &RecordBatch,
    key_col_idx: usize,
    include_key_in_value: bool,
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

    // Project value columns.
    let value_col_indices: Vec<usize> = (0..batch.num_columns())
        .filter(|&i| include_key_in_value || i != key_col_idx)
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
    include_key_in_value: bool,
    transcoder: Arc<Transcoder>,
}

impl<S> ProtobufEncodingSource<S> {
    /// Create a new encoding source.
    ///
    /// - `inner`: source producing Arrow `RecordBatch`es
    /// - `key_col_idx`: index of the column to extract as the KV key
    /// - `include_key_in_value`: whether to include the key column in the encoded value
    /// - `transcoder`: pre-built apb `Transcoder`, shared via `Arc` (it is `Sync`)
    pub fn new(
        inner: S,
        key_col_idx: usize,
        include_key_in_value: bool,
        transcoder: Arc<Transcoder>,
    ) -> Self {
        Self {
            inner,
            key_col_idx,
            include_key_in_value,
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

        encode_batch(
            &batch,
            self.key_col_idx,
            self.include_key_in_value,
            &self.transcoder,
        )
        .map(Some)
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

        let encoded = encode_batch(&batch, 0, false, &transcoder).unwrap();
        assert_eq!(encoded.len(), 2);

        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        encoded.for_each_kv(|k, v| pairs.push((k.to_vec(), v.to_vec())));
        assert_eq!(pairs[0].0, b"alice");
        assert_eq!(pairs[1].0, b"bob");
        assert!(!pairs[0].1.is_empty());
        assert!(!pairs[1].1.is_empty());
        assert!(encoded.total_bytes() > 0);
    }

    #[test]
    fn test_encode_batch_include_key_in_value() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["alice"])),
                Arc::new(Int32Array::from(vec![30])),
            ],
        )
        .unwrap();

        // Value schema includes all columns (key + age).
        let value_schema = Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
        ]);
        let fd = generate_file_descriptor(&value_schema, "test", "Row").unwrap();
        let fds = FileDescriptorSet { file: vec![fd] };
        let desc_bytes = fds.encode_to_vec();
        let proto_schema = ProtoSchema::from_bytes(&desc_bytes).unwrap();
        let msg = proto_schema.message("test.Row").unwrap();
        let mapping = infer_mapping(&value_schema, &msg, &InferOptions::default()).unwrap();
        let transcoder = Transcoder::new(&mapping).unwrap();

        let encoded = encode_batch(&batch, 0, true, &transcoder).unwrap();
        assert_eq!(encoded.len(), 1);

        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        encoded.for_each_kv(|k, v| pairs.push((k.to_vec(), v.to_vec())));
        assert_eq!(pairs[0].0, b"alice");
        // Decode the protobuf value and verify the key field is present.
        let dynamic_msg = prost_reflect::DynamicMessage::decode(msg, &*pairs[0].1).unwrap();
        assert_eq!(
            dynamic_msg.get_field_by_name("key").unwrap().as_str(),
            Some("alice")
        );
        assert_eq!(
            dynamic_msg
                .get_field_by_name("age")
                .unwrap()
                .as_i32()
                .unwrap(),
            30
        );
    }

    #[test]
    fn test_encode_batch_int64_key() {
        use arrow_array::Int64Array;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, -42, 9999999999])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap();

        let value_schema = Schema::new(vec![Field::new("score", DataType::Float64, false)]);
        let fd = generate_file_descriptor(&value_schema, "test", "Row").unwrap();
        let fds = FileDescriptorSet { file: vec![fd] };
        let desc_bytes = fds.encode_to_vec();
        let proto_schema = ProtoSchema::from_bytes(&desc_bytes).unwrap();
        let msg = proto_schema.message("test.Row").unwrap();
        let mapping = infer_mapping(&value_schema, &msg, &InferOptions::default()).unwrap();
        let transcoder = Transcoder::new(&mapping).unwrap();

        let encoded = encode_batch(&batch, 0, false, &transcoder).unwrap();
        assert_eq!(encoded.len(), 3);

        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        encoded.for_each_kv(|k, v| pairs.push((k.to_vec(), v.to_vec())));
        assert_eq!(pairs[0].0, b"1");
        assert_eq!(pairs[1].0, b"-42");
        assert_eq!(pairs[2].0, b"9999999999");
        assert!(!pairs[0].1.is_empty());
        assert!(encoded.total_bytes() > 0);
    }

    #[tokio::test]
    async fn test_build_transcoder_auto_generate() {
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

        let output = build_transcoder(&config, &value_schema).await.unwrap();
        assert_eq!(output.message_fqn, "test.Row");
        assert!(!output.descriptor_bytes.is_empty());

        let batch = RecordBatch::try_new(
            Arc::new(value_schema),
            vec![
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(Int32Array::from(vec![42])),
            ],
        )
        .unwrap();

        let result = output.transcoder.transcode_arrow(&batch).unwrap();
        assert_eq!(result.len(), 1);
        assert!(!result.value(0).is_empty());
    }

    #[tokio::test]
    async fn test_build_transcoder_inline_descriptor() {
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

        let output = build_transcoder(&config, &value_schema).await.unwrap();
        assert_eq!(output.message_fqn, "pkg.Msg");

        let batch = RecordBatch::try_new(
            Arc::new(value_schema),
            vec![
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(Int32Array::from(vec![42])),
            ],
        )
        .unwrap();

        let result = output.transcoder.transcode_arrow(&batch).unwrap();
        assert_eq!(result.len(), 1);
        assert!(!result.value(0).is_empty());
    }

    #[tokio::test]
    async fn test_build_transcoder_mutual_exclusion_error() {
        use crate::builder::build_transcoder;
        use crate::config::ProtobufEncoding;

        let value_schema = Schema::new(vec![Field::new("x", DataType::Int32, false)]);

        let config = ProtobufEncoding {
            descriptor: Some("abc".into()),
            descriptor_uri: Some("gs://bucket/file".into()),
            package: None,
            message_name: "Msg".into(),
        };

        let result = build_transcoder(&config, &value_schema).await;
        assert!(result.is_err());
        assert!(result
            .err()
            .unwrap()
            .to_string()
            .contains("mutually exclusive"));
    }

    #[tokio::test]
    async fn test_build_transcoder_missing_package_error() {
        use crate::builder::build_transcoder;
        use crate::config::ProtobufEncoding;

        let value_schema = Schema::new(vec![Field::new("x", DataType::Int32, false)]);

        let config = ProtobufEncoding {
            descriptor: None,
            descriptor_uri: None,
            package: None,
            message_name: "Msg".into(),
        };

        let result = build_transcoder(&config, &value_schema).await;
        assert!(result.is_err());
        assert!(result
            .err()
            .unwrap()
            .to_string()
            .contains("package is required"));
    }
}
