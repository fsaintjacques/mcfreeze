// SPDX-License-Identifier: Apache-2.0

//! V5 `meta.json` payload. Written last by the builder as the snapshot
//! completeness sentinel; parsed by `SnapshotDesc::load` once the
//! version probe commits to V5.

use serde::{Deserialize, Serialize};

use crate::{
    meta::{Layout, Stats, HASH_ALGORITHM},
    Error, Result,
};

pub const FORMAT_VERSION: u32 = 5;

/// Smallest legal `block_size`: the physical I/O floor (page cache
/// granularity, NVMe sector). The auto-tune clamp's lower edge.
pub const MIN_BLOCK_SIZE: u32 = 4096;
/// Auto-tune clamp's upper edge. `--block-size` may exceed it.
pub const MAX_AUTO_BLOCK_SIZE: u32 = 64 * 1024;

/// Optional per-partition filter over key fingerprints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SketchMeta {
    pub kind: String,
}

/// Per-partition control data. `n_blocks` is derivable from
/// `len(fences.bin) / 4`; kept explicit to catch truncation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMeta {
    pub n_blocks: u64,
}

/// Contents of a V5 `meta.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    pub format_version: u32,
    pub hash_algorithm: String,
    pub verify_seed: u64,
    /// Power-of-two multiple of 4 KiB. One block = one `pread`.
    pub block_size: u32,
    /// Values ≤ threshold are inline; larger ones live in `heap.bin`
    /// behind a stub. Defaults to `block_size / 2` at build time.
    pub inline_threshold: u32,
    /// `None` when the filter is disabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sketch: Option<SketchMeta>,
    pub partitions: Vec<PartitionMeta>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<Stats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<serde_json::Value>,
}

impl Meta {
    /// Validate hash algorithm, block size, and partition count; return
    /// the derived [`Layout`]. Version dispatch is not re-checked here —
    /// it has a single owner, the probe in `SnapshotDesc::load`.
    pub fn layout(&self) -> Result<Layout> {
        if self.hash_algorithm != HASH_ALGORITHM {
            return Err(Error::UnsupportedHashAlgorithm(self.hash_algorithm.clone()));
        }
        validate_block_size(self.block_size)?;
        Layout::new(self.partitions.len() as u32)
    }
}

/// `block_size` contract shared by meta validation and the `--block-size`
/// build override: a power of two, at least [`MIN_BLOCK_SIZE`].
pub fn validate_block_size(block_size: u32) -> Result<()> {
    if !block_size.is_power_of_two() || block_size < MIN_BLOCK_SIZE {
        return Err(Error::InvalidBlockSize(block_size));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::DEFAULT_VERIFY_SEED;

    fn test_meta(n_partitions: u32) -> Meta {
        Meta {
            format_version: FORMAT_VERSION,
            hash_algorithm: HASH_ALGORITHM.to_string(),
            verify_seed: DEFAULT_VERIFY_SEED,
            block_size: 4096,
            inline_threshold: 2048,
            sketch: None,
            partitions: (0..n_partitions)
                .map(|_| PartitionMeta { n_blocks: 0 })
                .collect(),
            stats: None,
            encoding: None,
        }
    }

    #[test]
    fn layout_roundtrip() {
        assert_eq!(test_meta(8).layout().unwrap().n_partitions, 8);
    }

    #[test]
    fn rejects_bad_hash_algorithm() {
        let mut m = test_meta(1);
        m.hash_algorithm = "sha256".into();
        assert!(m.layout().is_err());
    }

    #[test]
    fn rejects_bad_block_size() {
        for bad in [0u32, 512, 4095, 5000, 6144] {
            assert!(
                matches!(validate_block_size(bad), Err(Error::InvalidBlockSize(b)) if b == bad),
                "{bad} must be rejected"
            );
        }
        for good in [4096u32, 8192, 65536, 1 << 20] {
            validate_block_size(good).unwrap();
        }
    }

    #[test]
    fn sketch_field_optional_in_json() {
        // Older/sketchless snapshots omit the field entirely.
        let m = test_meta(1);
        let json = serde_json::to_string(&m).unwrap();
        assert!(!json.contains("sketch"));
        let back: Meta = serde_json::from_str(&json).unwrap();
        assert!(back.sketch.is_none());
    }
}
