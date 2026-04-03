mod adapter;
mod builder;
pub mod config;
mod error;

pub use adapter::{ProtobufEncodingSource, RecordBatchSource};
pub use builder::build_transcoder;
pub use error::EncodeError;
