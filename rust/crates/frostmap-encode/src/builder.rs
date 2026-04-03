use arrow_schema::Schema;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use prost_reflect::prost::Message;
use prost_reflect::prost_types::FileDescriptorSet;

use apb_core::descriptor::ProtoSchema;
use apb_core::generate::generate_file_descriptor;
use apb_core::mapping::{InferOptions, infer_mapping};
use apb_core::transcode::Transcoder;

use crate::config::ProtobufEncoding;
use crate::error::EncodeError;

/// Build a [`Transcoder`] from encoding config and the Arrow schema of the
/// value columns (key column already removed).
///
/// When `config.descriptor` is set, it is decoded from base64 and used directly.
/// When absent, a descriptor is auto-generated from the Arrow schema.
pub fn build_transcoder(
    config: &ProtobufEncoding,
    value_schema: &Schema,
) -> Result<Transcoder, EncodeError> {
    let (descriptor_bytes, message_name) = match (&config.descriptor, &config.message_name) {
        (Some(desc_b64), Some(name)) => {
            let bytes = STANDARD.decode(desc_b64)?;
            (bytes, name.clone())
        }
        (None, _) => {
            let fd = generate_file_descriptor(value_schema, "frostmap", "Value")?;
            let fds = FileDescriptorSet { file: vec![fd] };
            let bytes = fds.encode_to_vec();
            (bytes, "frostmap.Value".to_string())
        }
        (Some(_), None) => {
            return Err(EncodeError::Config(
                "protobuf.message_name is required when protobuf.descriptor is set".into(),
            ));
        }
    };

    let schema = ProtoSchema::from_bytes(&descriptor_bytes)?;
    let msg = schema.message(&message_name)?;
    let mapping = infer_mapping(value_schema, &msg, &InferOptions::default())?;
    let transcoder = Transcoder::new(&mapping)?;
    Ok(transcoder)
}
