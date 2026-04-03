//! Serde config types, wire-compatible with the Go `api.DatasetSpec` JSON.

use std::path::PathBuf;

use serde::Deserialize;

/// Top-level worker config read from a JSON file.
#[derive(Debug, Deserialize)]
pub struct WorkerConfig {
    pub source: SourceSpec,
    #[serde(default)]
    pub encoding: Option<EncodingSpec>,
    pub output: PathBuf,
    #[serde(default = "default_partitions")]
    pub partitions: u32,
    #[serde(default = "default_index_parallelism")]
    pub index_parallelism: usize,
}

fn default_partitions() -> u32 { 64 }
fn default_index_parallelism() -> usize { 2 }

/// Discriminated union: exactly one field is set.
#[derive(Debug, Deserialize)]
pub struct SourceSpec {
    pub bigquery: Option<BigQuerySource>,
}

#[derive(Debug, Deserialize)]
pub struct BigQuerySource {
    pub project: String,
    pub table: String,
    pub key_column: String,
    /// Required when encoding is raw (no encoding spec).
    /// Empty string is treated as absent.
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub value_column: Option<String>,
    /// Column projection — only read these columns. Empty = all columns.
    #[serde(default)]
    pub selected_fields: Vec<String>,
    /// SQL WHERE predicate pushed down to BigQuery.
    pub row_restriction: Option<String>,
    #[serde(default = "default_streams")]
    pub streams: i32,
    #[serde(default)]
    pub no_compression: bool,
}

fn default_streams() -> i32 { 8 }

/// Discriminated union for value encoding.
/// When absent, values are taken raw from a single column.
#[derive(Debug, Deserialize)]
pub struct EncodingSpec {
    pub protobuf: Option<ProtobufEncoding>,
}

/// Protobuf encoding via apb.
#[derive(Debug, Deserialize)]
pub struct ProtobufEncoding {
    /// Base64-encoded `FileDescriptorSet`.
    /// Mutually exclusive with `descriptor_uri`. When both are absent,
    /// the descriptor is auto-generated from the Arrow schema.
    pub descriptor: Option<String>,
    /// GCS URI to a `FileDescriptorSet` binary (e.g. `gs://bucket/schema.desc`).
    /// Mutually exclusive with `descriptor`.
    pub descriptor_uri: Option<String>,
    /// Protobuf package name (e.g. `"mypackage"`).
    /// Required when auto-generating (no descriptor); ignored otherwise.
    pub package: Option<String>,
    /// Protobuf message name.
    /// When auto-generating: bare name (e.g. `"MyMessage"`) — combined with
    /// `package` to form the FQN.
    /// When a descriptor is provided: fully-qualified name
    /// (e.g. `"mypackage.MyMessage"`).
    pub message_name: String,
}

/// Deserialize an empty string as `None`.
fn empty_string_as_none<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    Ok(s.filter(|s| !s.is_empty()))
}
