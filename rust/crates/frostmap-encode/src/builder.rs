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
/// Descriptor resolution order:
/// 1. `descriptor` (inline base64) — decode and use directly
/// 2. `descriptor_uri` (GCS URI) — download and use (not yet implemented)
/// 3. Neither — auto-generate from the Arrow schema using `package` + `message_name`
pub fn build_transcoder(
    config: &ProtobufEncoding,
    value_schema: &Schema,
) -> Result<Transcoder, EncodeError> {
    let descriptor_bytes = resolve_descriptor(config, value_schema)?;
    let fqn = fully_qualified_name(config);

    let schema = ProtoSchema::from_bytes(&descriptor_bytes)?;
    let msg = schema.message(&fqn)?;
    let mapping = infer_mapping(value_schema, &msg, &InferOptions::default())?;
    let transcoder = Transcoder::new(&mapping)?;
    Ok(transcoder)
}

/// Resolve descriptor bytes from config.
fn resolve_descriptor(
    config: &ProtobufEncoding,
    value_schema: &Schema,
) -> Result<Vec<u8>, EncodeError> {
    match (&config.descriptor, &config.descriptor_uri) {
        (Some(_), Some(_)) => {
            Err(EncodeError::Config(
                "descriptor and descriptor_uri are mutually exclusive".into(),
            ))
        }
        (Some(desc_b64), None) => {
            Ok(STANDARD.decode(desc_b64)?)
        }
        (None, Some(_uri)) => {
            // TODO: download from GCS
            Err(EncodeError::Config(
                "descriptor_uri (GCS download) is not yet implemented".into(),
            ))
        }
        (None, None) => {
            // Auto-generate from Arrow schema.
            let package = config.package.as_deref().ok_or_else(|| {
                EncodeError::Config(
                    "protobuf.package is required when no descriptor is provided".into(),
                )
            })?;
            let fd = generate_file_descriptor(value_schema, package, &config.message_name)?;
            let fds = FileDescriptorSet { file: vec![fd] };
            Ok(fds.encode_to_vec())
        }
    }
}

/// Build the fully-qualified message name from config.
fn fully_qualified_name(config: &ProtobufEncoding) -> String {
    match &config.package {
        Some(pkg) if !pkg.is_empty() => format!("{}.{}", pkg, config.message_name),
        _ => config.message_name.clone(),
    }
}
