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
    /// Base64-encoded `FileDescriptorSet`. When absent, the descriptor is
    /// auto-generated from the Arrow schema.
    pub descriptor: Option<String>,
    /// Fully-qualified protobuf message name.
    /// Required when `descriptor` is set; auto-generated otherwise.
    pub message_name: Option<String>,
}
