// SPDX-License-Identifier: Apache-2.0

pub mod builder;
pub mod data;
pub mod desc;
pub mod meta;
pub mod snapshot;
pub mod v4;

pub use builder::{builder_for, BuildDone, BuilderConfig, FormatBuilder, PartitionAppender};
pub use desc::{FormatId, SnapshotDesc};
pub use snapshot::{GetOutcome, OpenOptions, Snapshot};

// Path compatibility for the loader's write path until it migrates to
// `FormatBuilder` (doc/plan/FORMAT_INTERFACE.md). The reader is only
// reachable through the `Snapshot` facade.
pub use v4::{index, spill, writer};

mod error;
pub use error::Error;

pub type Result<T> = std::result::Result<T, Error>;
