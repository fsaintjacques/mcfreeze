// SPDX-License-Identifier: Apache-2.0

//! V5 fence search (`fences.bin`).
//!
//! `fence[i]` = high 32 bits of block `i`'s first record fingerprint.
//! The array is non-decreasing, **not** strictly increasing: a run of
//! records sharing their top 32 bits can straddle block boundaries, and
//! every block starting inside such a run repeats the fence value.
//!
//! For `h = high32(fp)`, with `lo` = index of the first fence ≥ `h`, the
//! candidate blocks are:
//!
//! - every block `i ≥ lo` with `fence[i] == h` (a run continuing through
//!   those blocks), and
//! - block `lo − 1` (the run may *start* mid-block, before the first
//!   fence equal to `h`; when no fence equals `h` this is the only
//!   candidate).
//!
//! Probe order matters: [`candidate_blocks`] yields the equal-fence
//! blocks first and the backward block last. About `1/B` of present keys
//! (B = records/block) share their block head's high32 — for those,
//! `fence[lo] == h` and the key is almost surely in block `lo`, not
//! `lo − 1`; probing `lo − 1` first would waste a `pread` on every such
//! lookup. The backward block only holds the key on a true backward
//! straddle, whose rate is `≈ n_partition_keys / (2^32 × B)` — see the
//! tie-rate table in `doc/plan/FORMAT_V5_SPARSE_INDEX.md`.

/// The fence of a fingerprint: its high 32 bits. No zero bias — unlike
/// the vfp, fence values carry no sentinel semantics.
#[inline]
pub fn fence_of(fp: u64) -> u32 {
    (fp >> 32) as u32
}

/// Candidate blocks for `h = high32(fp)`, in probe order (equal-fence
/// blocks ascending, then the backward block). Empty iff no block can
/// contain a record with that high32 — a zero-I/O miss.
///
/// Precondition: `fences` is non-decreasing (sorted by construction).
pub fn candidate_blocks(fences: &[u32], h: u32) -> impl Iterator<Item = usize> + '_ {
    let lo = fences.partition_point(|&f| f < h);
    let eq = fences[lo..].iter().take_while(|&&f| f == h).count();
    let back = if lo > 0 { Some(lo - 1) } else { None };
    (lo..lo + eq).chain(back)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidates(fences: &[u32], h: u32) -> Vec<usize> {
        candidate_blocks(fences, h).collect()
    }

    #[test]
    fn empty_partition_has_no_candidates() {
        assert!(candidates(&[], 42).is_empty());
    }

    #[test]
    fn below_first_fence_is_free_miss() {
        assert!(candidates(&[10, 20, 30], 5).is_empty());
    }

    #[test]
    fn strictly_between_fences_is_single_block() {
        assert_eq!(candidates(&[10, 20, 30], 15), vec![0]);
        assert_eq!(candidates(&[10, 20, 30], 25), vec![1]);
    }

    #[test]
    fn above_last_fence_is_last_block() {
        assert_eq!(candidates(&[10, 20, 30], 99), vec![2]);
    }

    #[test]
    fn equal_to_fence_probes_that_block_first_then_backward() {
        // h == fence[1]: the key is almost surely in block 1 (it shares
        // high32 with block 1's head); block 0 is the rare backward
        // straddle, probed last.
        assert_eq!(candidates(&[10, 20, 30], 20), vec![1, 0]);
    }

    #[test]
    fn equal_to_first_fence_has_no_backward_block() {
        assert_eq!(candidates(&[10, 20, 30], 10), vec![0]);
    }

    #[test]
    fn fence_run_probes_all_equal_blocks_then_backward() {
        // Blocks 1..=3 all start inside a run of high32 == 20.
        assert_eq!(candidates(&[10, 20, 20, 20, 30], 20), vec![1, 2, 3, 0]);
    }

    #[test]
    fn run_at_array_end() {
        assert_eq!(candidates(&[10, 20, 20], 20), vec![1, 2, 0]);
    }

    #[test]
    fn fence_of_is_high32_unbiased() {
        assert_eq!(fence_of(0xABCD_1234_0000_5678), 0xABCD_1234);
        assert_eq!(fence_of(0x0000_0000_FFFF_FFFF), 0);
    }
}
