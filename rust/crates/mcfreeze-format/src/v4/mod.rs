// SPDX-License-Identifier: Apache-2.0

//! Format V4: Robin Hood hash index (`index.all`) + 64-byte-aligned
//! values read via `pread` (`data.bin`). See `doc/format.md`.

pub mod index;
pub mod reader;
pub mod spill;
pub mod writer;
