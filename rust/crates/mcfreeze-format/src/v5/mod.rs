// SPDX-License-Identifier: Apache-2.0

//! Format V5: fingerprint-sorted fixed-size blocks + per-block fences.
//! See `doc/plan/FORMAT_V5_SPARSE_INDEX.md`.
//!
//! In-memory primitives — record and block encode/scan ([`block`]),
//! fence build and candidate search ([`fence`]) — plus the
//! [`FormatBuilder`](crate::FormatBuilder) implementation ([`builder`]),
//! the V5 `meta.json` payload ([`meta`]), and the read path behind the
//! `Snapshot` facade ([`reader`]).

pub mod block;
pub mod builder;
pub mod compress;
pub mod fence;
pub mod meta;
pub(crate) mod reader;
pub mod sketch;

use xxhash_rust::xxh64::xxh64;

/// Verify fingerprint for V5 record headers: `xxhash64(key, seed)`,
/// biased 0 → 1 so that 0 remains the block-padding sentinel the block
/// scanner stops at.
#[inline]
pub fn verify_fingerprint(key: &[u8], seed: u64) -> u64 {
    match xxh64(key, seed) {
        0 => 1,
        h => h,
    }
}

#[cfg(test)]
mod tests {
    use super::block::{find, BlockAssembler, Record, Stub};
    use super::fence::{candidate_blocks, fence_of};

    /// End-to-end in-memory lookup: fences route to candidate blocks,
    /// each candidate is checksum-verified and scanned for the vfp.
    fn lookup<'a>(blocks: &'a [Vec<u8>], fences: &[u32], fp: u64, vfp: u64) -> Option<Record<'a>> {
        for b in candidate_blocks(fences, fence_of(fp)) {
            if let Some(rec) = find(&blocks[b], vfp).unwrap() {
                return Some(rec);
            }
        }
        None
    }

    /// Build a partition from records sorted by `fp`. Returns the sealed
    /// blocks and the fence array. `vfp` is derived as `vfp_of(fp)` for test
    /// purposes (non-zero, unique for distinct test fps).
    fn build(block_size: usize, fps: &[u64]) -> (Vec<Vec<u8>>, Vec<u32>) {
        let mut blocks = Vec::new();
        let mut asm = BlockAssembler::new(block_size, |b: &[u8]| {
            blocks.push(b.to_vec());
            Ok(())
        });
        for &fp in fps {
            asm.push_inline(fp, vfp_of(fp), &fp.to_le_bytes(), false)
                .unwrap();
        }
        let fences = asm.finish().unwrap();
        (blocks, fences)
    }

    fn assert_hit(blocks: &[Vec<u8>], fences: &[u32], fp: u64) {
        match lookup(blocks, fences, fp, vfp_of(fp)) {
            Some(Record::Inline { value, .. }) => assert_eq!(value, fp.to_le_bytes()),
            other => panic!("expected hit for fp={fp:#x}, got {other:?}"),
        }
    }

    // 64-byte blocks hold exactly two 24-byte records (usable = 60);
    // every test below relies on that geometry.
    const BS: usize = 64;

    fn fp(h: u32, low: u64) -> u64 {
        ((h as u64) << 32) | low
    }

    /// Injective and non-zero for the fps used in these tests (`fp | 1`
    /// would collide adjacent even/odd fps onto one vfp).
    fn vfp_of(fp: u64) -> u64 {
        fp * 2 + 1
    }

    #[test]
    fn backward_straddle_is_found() {
        // Run of high32 == 100 starts in block 0's tail:
        //   b0: [50|1, 100|1]  b1: [100|2, 100|3]   fences = [50, 100]
        // The key 100|1 lives *before* the first fence == 100 — the case
        // the original last-≤-plus-next spec false-missed.
        let fps = [fp(50, 1), fp(100, 1), fp(100, 2), fp(100, 3)];
        let (blocks, fences) = build(BS, &fps);
        assert_eq!(fences, vec![50, 100]);
        for &f in &fps {
            assert_hit(&blocks, &fences, f);
        }
    }

    #[test]
    fn run_spanning_full_blocks_is_found() {
        // Run of high32 == 100 spans two full blocks plus both
        // neighbors' edges: b0: [50|1, 100|1] b1: [100|2, 100|3]
        // b2: [100|4, 100|5] b3: [100|6, 200|1]  fences = [50,100,100,100]
        let fps = [
            fp(50, 1),
            fp(100, 1),
            fp(100, 2),
            fp(100, 3),
            fp(100, 4),
            fp(100, 5),
            fp(100, 6),
            fp(200, 1),
        ];
        let (blocks, fences) = build(BS, &fps);
        assert_eq!(fences, vec![50, 100, 100, 100]);
        for &f in &fps {
            assert_hit(&blocks, &fences, f);
        }
    }

    #[test]
    fn absent_key_sharing_run_high32_misses_cleanly() {
        // Forces the full candidate scan (all fence == 100 blocks plus
        // the backward block) to conclude a miss without error.
        let fps = [fp(50, 1), fp(100, 1), fp(100, 2), fp(100, 3)];
        let (blocks, fences) = build(BS, &fps);
        let absent = fp(100, 999);
        assert!(lookup(&blocks, &fences, absent, vfp_of(absent)).is_none());
    }

    #[test]
    fn empty_partition_misses_without_scanning() {
        let (blocks, fences) = build(BS, &[]);
        assert!(blocks.is_empty());
        assert!(fences.is_empty());
        assert!(lookup(&blocks, &fences, fp(100, 1), 1).is_none());
    }

    #[test]
    fn stub_records_roundtrip_through_lookup() {
        let mut blocks = Vec::new();
        let mut asm = BlockAssembler::new(BS, |b: &[u8]| {
            blocks.push(b.to_vec());
            Ok(())
        });
        let stub = Stub {
            heap_offset: 0x00DE_ADBE_EF00,
            stored_len: 1 << 20,
            value_checksum: 0x1234_5678,
        };
        asm.push_inline(fp(10, 1), 11, b"inline", false).unwrap();
        asm.push_stub(fp(20, 1), 21, stub, false).unwrap();
        let fences = asm.finish().unwrap();

        match lookup(&blocks, &fences, fp(20, 1), 21) {
            Some(Record::Stub { vfp, stub: s, .. }) => {
                assert_eq!(vfp, 21);
                assert_eq!(s, stub);
            }
            other => panic!("expected stub, got {other:?}"),
        }
    }
}
