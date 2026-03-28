use serde::{Deserialize, Serialize};

use crate::{Error, Result};

// ---------------------------------------------------------------------------
// Format-wide constants
// ---------------------------------------------------------------------------

pub const FORMAT_VERSION: u32   = 1;
pub const HASH_ALGORITHM: &str  = "xxhash64";

/// Bits allocated to the aligned offset in the `loc` field.
/// Max per-partition addressable space: 2^OFFSET_BITS × 64 bytes = 1 TiB.
pub const OFFSET_BITS: u8 = 34;

/// Bits allocated to value size. Max value: 2^22 - 1 = 4 MiB - 1 bytes.
pub const SIZE_BITS: u8 = 22;

/// Bits allocated to the probe sequence length. Max PSL: 255.
pub const PSL_BITS: u8 = 8;

// Sanity check: the three fields must exactly fill a u64.
const _: () = assert!(OFFSET_BITS as u32 + SIZE_BITS as u32 + PSL_BITS as u32 == 64);

/// Value alignment in bytes. Every value in `data.bin` starts at a multiple of this.
pub const VALUE_ALIGNMENT: u64 = 64;

/// Target hash-table fill rate used during index construction.
pub const FILL_RATE: f64 = 0.95;

// ---------------------------------------------------------------------------
// Layout
// ---------------------------------------------------------------------------

/// Partition-count-dependent parameters derived from `n_partitions`.
///
/// The bit widths in `loc` are fixed ([`OFFSET_BITS`], [`SIZE_BITS`],
/// [`PSL_BITS`]). `Layout` provides the derived masks and the routing helper.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Layout {
    pub n_partitions: u32,
    /// `n_partitions - 1`; used for fast modulo on the power-of-two count.
    partition_mask:   u64,
}

impl Layout {
    /// Construct a `Layout` for the given partition count.
    ///
    /// # Errors
    /// Returns [`Error::InvalidPartitionCount`] when `n_partitions` is zero,
    /// not a power of two, or so large that `log2(n) > OFFSET_BITS` (the
    /// per-partition address space would underflow).
    pub fn new(n_partitions: u32) -> Result<Self> {
        if n_partitions == 0
            || !n_partitions.is_power_of_two()
            || n_partitions.trailing_zeros() as u8 > OFFSET_BITS
        {
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
// Meta
// ---------------------------------------------------------------------------

/// Contents of `meta.json`, written last as the snapshot completion signal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    pub format_version:  u32,
    pub n_partitions:    u32,
    pub hash_algorithm:  String,
    pub offset_bits:     u8,
    pub size_bits:       u8,
    pub psl_bits:        u8,
    pub n_keys:          u64,
    pub created_at:      String,
}

impl Meta {
    /// Validate the stored bit widths against the compiled-in constants and
    /// return the derived [`Layout`].
    pub fn layout(&self) -> Result<Layout> {
        if self.offset_bits != OFFSET_BITS
            || self.size_bits != SIZE_BITS
            || self.psl_bits  != PSL_BITS
        {
            return Err(Error::LayoutMismatch {
                offset_bits: self.offset_bits,
                size_bits:   self.size_bits,
                psl_bits:    self.psl_bits,
            });
        }
        Layout::new(self.n_partitions)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bit_fields_sum_to_64() {
        assert_eq!(OFFSET_BITS as u32 + SIZE_BITS as u32 + PSL_BITS as u32, 64);
    }

    #[test]
    fn layout_ok() {
        // Any power of two that fits in u32 is valid (n_partitions: u32 caps at 2^31).
        for log2 in 0u32..=31 {
            Layout::new(1 << log2).unwrap();
        }
    }

    #[test]
    fn layout_n1_partition_routing() {
        let l = Layout::new(1).unwrap();
        // Every fingerprint routes to partition 0.
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
        // 2^(OFFSET_BITS+1) overflows the per-partition address space.
        let overflow = 1u64 << (OFFSET_BITS as u32 + 1);
        if overflow <= u32::MAX as u64 {
            assert!(Layout::new(overflow as u32).is_err());
        }
    }

    #[test]
    fn meta_layout_roundtrip() {
        let meta = Meta {
            format_version: FORMAT_VERSION,
            n_partitions:   64,
            hash_algorithm: HASH_ALGORITHM.to_string(),
            offset_bits:    OFFSET_BITS,
            size_bits:      SIZE_BITS,
            psl_bits:       PSL_BITS,
            n_keys:         0,
            created_at:     "2026-03-27T00:00:00Z".to_string(),
        };
        let layout = meta.layout().unwrap();
        assert_eq!(layout.n_partitions, 64);
    }

    #[test]
    fn meta_layout_mismatch() {
        let meta = Meta {
            format_version: FORMAT_VERSION,
            n_partitions:   64,
            hash_algorithm: HASH_ALGORITHM.to_string(),
            offset_bits:    OFFSET_BITS + 1, // wrong
            size_bits:      SIZE_BITS,
            psl_bits:       PSL_BITS,
            n_keys:         0,
            created_at:     "2026-03-27T00:00:00Z".to_string(),
        };
        assert!(meta.layout().is_err());
    }
}
