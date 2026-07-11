// SPDX-License-Identifier: Apache-2.0

use thiserror::Error;

#[derive(Debug, Error)]
pub enum EncodeError {
    #[error("apb transcode error: {0}")]
    Transcode(#[from] apb_core::transcode::TranscodeError),

    #[error("apb mapping error: {0}")]
    Mapping(#[from] apb_core::mapping::MappingError),

    #[error("apb descriptor error: {0}")]
    Descriptor(#[from] apb_core::descriptor::DescriptorError),

    #[error("apb generate error: {0}")]
    Generate(#[from] apb_core::generate::GenerateError),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("base64 decode error: {0}")]
    Base64(#[from] base64::DecodeError),

    #[error("config error: {0}")]
    Config(String),

    #[error("source error: {0}")]
    Source(Box<dyn std::error::Error + Send + Sync>),
}
