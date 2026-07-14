// SPDX-License-Identifier: Apache-2.0

pub mod data;
pub mod desc;
pub mod meta;
pub mod v4;

pub use desc::{FormatId, SnapshotDesc};

// Path compatibility while consumers migrate to the `Snapshot` facade
// (doc/plan/FORMAT_INTERFACE.md): `mcfreeze_format::reader::...` etc.
// keep resolving to the V4 implementation.
pub use v4::{index, reader, spill, writer};

mod error;
pub use error::Error;

pub type Result<T> = std::result::Result<T, Error>;
