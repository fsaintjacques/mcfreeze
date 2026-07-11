// SPDX-License-Identifier: Apache-2.0

use arrow_schema::Schema;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use prost_reflect::prost::Message;
use prost_reflect::prost_types::FileDescriptorSet;

use apb_core::descriptor::ProtoSchema;
use apb_core::generate::generate_file_descriptor;
use apb_core::mapping::{infer_mapping, InferOptions};
use apb_core::transcode::Transcoder;

use crate::config::ProtobufEncoding;
use crate::error::EncodeError;

/// Result of building a protobuf transcoder, including the resolved descriptor
/// bytes for downstream persistence (e.g. embedding in `meta.json`).
pub struct TranscoderOutput {
    pub transcoder: Transcoder,
    /// Serialized `FileDescriptorSet` (protobuf binary).
    pub descriptor_bytes: Vec<u8>,
    /// Fully-qualified protobuf message name (e.g. `"mypackage.MyMessage"`).
    pub message_fqn: String,
}

/// Build a [`Transcoder`] from encoding config and the Arrow schema of the
/// value columns (key column already removed).
///
/// Descriptor resolution order:
/// 1. `descriptor` (inline base64) — decode and use directly
/// 2. `descriptor_uri` (GCS URI) — download via authenticated HTTP GET
/// 3. Neither — auto-generate from the Arrow schema using `package` + `message_name`
pub async fn build_transcoder(
    config: &ProtobufEncoding,
    value_schema: &Schema,
) -> Result<TranscoderOutput, EncodeError> {
    let auto_generated = config.descriptor.is_none() && config.descriptor_uri.is_none();
    let descriptor_bytes = resolve_descriptor(config, value_schema).await?;
    let fqn = if auto_generated {
        // Auto-generated: FQN is package.message_name
        fully_qualified_name(config)
    } else {
        // Explicit descriptor: message_name is already the FQN
        config.message_name.clone()
    };

    let schema = ProtoSchema::from_bytes(&descriptor_bytes)?;
    let msg = schema.message(&fqn)?;
    let mapping = infer_mapping(value_schema, &msg, &InferOptions::default())?;
    let transcoder = Transcoder::new(&mapping)?;
    Ok(TranscoderOutput {
        transcoder,
        descriptor_bytes,
        message_fqn: fqn,
    })
}

/// Resolve descriptor bytes from config.
async fn resolve_descriptor(
    config: &ProtobufEncoding,
    value_schema: &Schema,
) -> Result<Vec<u8>, EncodeError> {
    match (&config.descriptor, &config.descriptor_uri) {
        (Some(_), Some(_)) => Err(EncodeError::Config(
            "descriptor and descriptor_uri are mutually exclusive".into(),
        )),
        (Some(desc_b64), None) => Ok(STANDARD.decode(desc_b64)?),
        (None, Some(uri)) => download_gcs_descriptor(uri).await,
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

/// Parse a `gs://bucket/object` URI into (bucket, object).
fn parse_gcs_uri(uri: &str) -> Result<(&str, &str), EncodeError> {
    let path = uri.strip_prefix("gs://").ok_or_else(|| {
        EncodeError::Config(format!("descriptor_uri must start with gs://, got {uri:?}"))
    })?;
    let (bucket, object) = path.split_once('/').ok_or_else(|| {
        EncodeError::Config(format!("descriptor_uri missing object path: {uri:?}"))
    })?;
    if bucket.is_empty() || object.is_empty() {
        return Err(EncodeError::Config(format!(
            "descriptor_uri has empty bucket or object: {uri:?}"
        )));
    }
    Ok((bucket, object))
}

/// Download a FileDescriptorSet from a `gs://` URI.
///
/// Uses the GCS JSON API (`storage/v1`) with ADC for authentication
/// (Workload Identity, gcloud CLI, or `GOOGLE_APPLICATION_CREDENTIALS`).
async fn download_gcs_descriptor(uri: &str) -> Result<Vec<u8>, EncodeError> {
    let (bucket, object) = parse_gcs_uri(uri)?;

    let api = gcloud_sdk::GoogleRestApi::new()
        .await
        .map_err(|e| EncodeError::Source(format!("GCS auth init: {e}").into()))?;

    let encoded_object = utf8_percent_encode(object, NON_ALPHANUMERIC).to_string();
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b/{bucket}/o/{encoded_object}?alt=media",
    );

    let resp = api
        .get(&url)
        .await
        .map_err(|e| EncodeError::Source(format!("GCS auth token: {e}").into()))?
        .send()
        .await
        .map_err(|e| EncodeError::Source(format!("GCS GET {uri}: {e}").into()))?;

    if !resp.status().is_success() {
        return Err(EncodeError::Source(
            format!("GCS GET {uri}: HTTP {}", resp.status()).into(),
        ));
    }

    let bytes = resp
        .bytes()
        .await
        .map_err(|e| EncodeError::Source(format!("GCS read body {uri}: {e}").into()))?;

    if bytes.is_empty() {
        return Err(EncodeError::Config(format!("GCS object is empty: {uri}")));
    }

    Ok(bytes.to_vec())
}

/// Build the fully-qualified message name from config.
fn fully_qualified_name(config: &ProtobufEncoding) -> String {
    match &config.package {
        Some(pkg) if !pkg.is_empty() => format!("{}.{}", pkg, config.message_name),
        _ => config.message_name.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_gcs_uri_ok() {
        let (b, o) = parse_gcs_uri("gs://my-bucket/path/to/file.desc").unwrap();
        assert_eq!(b, "my-bucket");
        assert_eq!(o, "path/to/file.desc");
    }

    #[test]
    fn test_parse_gcs_uri_single_object() {
        let (b, o) = parse_gcs_uri("gs://bucket/file.desc").unwrap();
        assert_eq!(b, "bucket");
        assert_eq!(o, "file.desc");
    }

    #[test]
    fn test_parse_gcs_uri_missing_prefix() {
        let err = parse_gcs_uri("s3://bucket/file").unwrap_err();
        assert!(err.to_string().contains("must start with gs://"));
    }

    #[test]
    fn test_parse_gcs_uri_no_object() {
        let err = parse_gcs_uri("gs://bucket-only").unwrap_err();
        assert!(err.to_string().contains("missing object path"));
    }

    #[test]
    fn test_parse_gcs_uri_empty_bucket() {
        let err = parse_gcs_uri("gs:///object").unwrap_err();
        assert!(err.to_string().contains("empty bucket or object"));
    }

    #[test]
    fn test_parse_gcs_uri_empty_object() {
        let err = parse_gcs_uri("gs://bucket/").unwrap_err();
        assert!(err.to_string().contains("empty bucket or object"));
    }
}
