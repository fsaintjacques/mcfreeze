// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

// ---------------------------------------------------------------------------
// Format-wide constants
// ---------------------------------------------------------------------------

pub const FORMAT_VERSION: u32 = 4;

/// Default seed for the value-header verification fingerprint.
/// Must be != 0 so it is independent of the index fingerprint (seed 0).
pub const DEFAULT_VERIFY_SEED: u64 = 0x517cc1b727220a95; // xxhash64("mcfreeze-verify")
pub const HASH_ALGORITHM: &str = "xxhash64";

/// Value alignment in bytes. Every value in `data.bin` starts at a multiple of this.
pub const VALUE_ALIGNMENT: u64 = 64;

/// Target hash-table fill rate used during index construction.
/// 0.95 keeps the table compact; `build()` retries with a 1.5× larger table
/// on PSL overflow, so the fill rate only affects the common-case table size.
pub const FILL_RATE: f64 = 0.95;

/// Alignment of each partition's bucket array within `index.all` (2 MiB).
/// Matches the huge page size on x86-64 Linux for `MADV_HUGEPAGE`.
pub const INDEX_ALIGNMENT: u64 = 2 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Layout
// ---------------------------------------------------------------------------

/// Partition-count-dependent parameters derived from `n_partitions`.
///
/// Each partition uses compact 8-byte buckets (u32 fingerprint + u32 offset).
/// `Layout` provides the derived masks and the routing helper.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Layout {
    pub n_partitions: u32,
    /// `n_partitions - 1`; used for fast modulo on the power-of-two count.
    partition_mask: u64,
}

impl Layout {
    /// Construct a `Layout` for the given partition count.
    ///
    /// # Errors
    /// Returns [`Error::InvalidPartitionCount`] when `n_partitions` is zero
    /// or not a power of two.
    pub fn new(n_partitions: u32) -> Result<Self> {
        if n_partitions == 0 || !n_partitions.is_power_of_two() {
            return Err(Error::InvalidPartitionCount(n_partitions));
        }
        Ok(Self {
            n_partitions,
            partition_mask: n_partitions as u64 - 1,
        })
    }

    /// Map a fingerprint to its partition index.
    #[inline]
    pub fn partition_of(&self, fingerprint: u64) -> usize {
        (fingerprint & self.partition_mask) as usize
    }
}

// ---------------------------------------------------------------------------
// Directory helpers
// ---------------------------------------------------------------------------

/// Returns the `data/` subdirectory of a snapshot root.
/// All partition directories live under this path.
pub fn data_dir(root: &std::path::Path) -> std::path::PathBuf {
    root.join("data")
}

/// Path to the unified index file: `<root>/index.all`.
pub fn index_path(root: &std::path::Path) -> std::path::PathBuf {
    root.join("index.all")
}

/// Zero-padded partition directory path (e.g. `<root>/data/part-07` for N=64, i=7).
pub fn partition_dir(root: &std::path::Path, n_partitions: u32, i: usize) -> std::path::PathBuf {
    let width = format!("{}", n_partitions - 1).len();
    data_dir(root).join(format!("part-{:0>width$}", i, width = width))
}

// ---------------------------------------------------------------------------
// Meta
// ---------------------------------------------------------------------------

/// Size of the value header: 8-byte verify fingerprint + 4-byte length.
pub const VALUE_HEADER_SIZE: usize = 12;

/// Per-partition control data needed by the reader.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMeta {
    /// Byte offset of this partition's bucket array within `index.all`.
    /// Each offset is 2 MiB-aligned for huge page backing.
    pub index_offset: u64,
    /// Number of logical buckets in this partition.
    pub index_n_buckets: u64,
}

/// Build-time statistics embedded in `meta.json` for diagnostics.
/// Opaque to the reader — never needed to open or query a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stats {
    pub n_keys: u64,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scatter: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<serde_json::Value>,
}

/// Contents of `meta.json`, written last as the snapshot completion signal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    pub format_version: u32,
    pub hash_algorithm: String,
    /// Seed for the verification fingerprint stored in each value header.
    /// Must be non-zero in V3 snapshots.
    pub verify_seed: u64,
    /// Per-partition control data; length determines the partition count.
    pub partitions: Vec<PartitionMeta>,
    /// Build-time statistics (opaque to the reader).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<Stats>,
    /// Encoding metadata (e.g. protobuf descriptor), patched by the CLI.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<serde_json::Value>,
}

impl Meta {
    /// Validate the format version, hash algorithm, and partition count,
    /// then return the derived [`Layout`].
    pub fn layout(&self) -> Result<Layout> {
        if self.format_version != FORMAT_VERSION {
            return Err(Error::VersionMismatch {
                expected: FORMAT_VERSION,
                got: self.format_version,
            });
        }
        if self.hash_algorithm != HASH_ALGORITHM {
            return Err(Error::UnsupportedHashAlgorithm(self.hash_algorithm.clone()));
        }
        Layout::new(self.partitions.len() as u32)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_ok() {
        for log2 in 0u32..=31 {
            Layout::new(1 << log2).unwrap();
        }
    }

    #[test]
    fn layout_n1_partition_routing() {
        let l = Layout::new(1).unwrap();
        assert_eq!(l.partition_of(0), 0);
        assert_eq!(l.partition_of(u64::MAX), 0);
    }

    #[test]
    fn layout_n64_partition_routing() {
        let l = Layout::new(64).unwrap();
        assert_eq!(l.partition_of(0), 0);
        assert_eq!(l.partition_of(64), 0);
        assert_eq!(l.partition_of(63), 63);
        assert_eq!(l.partition_of(65), 1);
    }

    #[test]
    fn layout_invalid() {
        assert!(Layout::new(0).is_err());
        assert!(Layout::new(3).is_err());
        assert!(Layout::new(7).is_err());
    }

    fn test_meta(n_partitions: u32) -> Meta {
        Meta {
            format_version: FORMAT_VERSION,
            hash_algorithm: HASH_ALGORITHM.to_string(),
            verify_seed: DEFAULT_VERIFY_SEED,
            partitions: (0..n_partitions)
                .map(|_| PartitionMeta {
                    index_offset: 0,
                    index_n_buckets: 0,
                })
                .collect(),
            stats: None,
            encoding: None,
        }
    }

    #[test]
    fn meta_layout_roundtrip() {
        let meta = test_meta(64);
        let layout = meta.layout().unwrap();
        assert_eq!(layout.n_partitions, 64);
    }

    #[test]
    fn meta_rejects_old_format_version() {
        let mut meta = test_meta(64);
        meta.format_version = 2;
        assert!(meta.layout().is_err());
    }

    #[test]
    fn meta_rejects_bad_hash_algorithm() {
        let mut meta = test_meta(4);
        meta.hash_algorithm = "sha256".to_string();
        assert!(meta.layout().is_err());
    }
}
