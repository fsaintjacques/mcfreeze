// SPDX-License-Identifier: Apache-2.0

mod adapter;
mod builder;
pub mod config;
mod error;

pub use adapter::{encode_batch, EncodedBatch, ProtobufEncodingSource};
pub use builder::{build_transcoder, TranscoderOutput};
pub use error::EncodeError;
