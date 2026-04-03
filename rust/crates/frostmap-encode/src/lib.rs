mod adapter;
mod builder;
pub mod config;
mod error;

pub use adapter::{encode_batch, EncodedBatch, ProtobufEncodingSource};
pub use builder::build_transcoder;
pub use error::EncodeError;
