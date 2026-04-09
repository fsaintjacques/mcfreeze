use bytemuck::{Pod, Zeroable};
use xxhash_rust::xxh64::xxh64;

use crate::{
    meta::{FILL_RATE, INDEX_ALIGNMENT, VALUE_ALIGNMENT},
    Error, Result,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum aligned offset that fits in a u32. Reserved below `u32::MAX` because
/// `NO_MATCH` (u32::MAX) is used as a sentinel in `ProbeResult::offsets`.
const MAX_OFFSET: u32 = u32::MAX - 1;
const MAX_PSL: u8 = u8::MAX;

// ---------------------------------------------------------------------------
// Fingerprinting
// ---------------------------------------------------------------------------

/// Compute the xxhash64 fingerprint of a key (seed 0).
///
/// A result of `0` is biased to `1` to preserve the zero-fingerprint
/// sentinel that marks empty buckets (both the full u64 and the truncated u32).
#[inline]
pub fn fingerprint(key: &[u8]) -> u64 {
    let h = xxh64(key, 0);
    if h == 0 {
        1
    } else {
        h
    }
}

/// Derive the 32-bit bucket fingerprint from a full 64-bit fingerprint.
///
/// Uses the **high** 32 bits. This is intentional: `Layout::partition_of`
/// uses the low `log2(n_partitions)` bits of the same u64 to route records
/// to a partition, so within a single partition every record shares the
/// same low bits. If `compact_fingerprint` also used the low 32 bits, the
/// compact fps within a partition would share those same low bits, and
/// `cfp % n_buckets` would collapse onto a fraction `n_buckets /
/// gcd(2^partition_bits, n_buckets)` of the table — guaranteed PSL
/// overflow on any realistic load.
///
/// Taking the high 32 bits decouples partition routing from home-position
/// computation: within a partition, compact fps are uniformly distributed
/// across the full `u32` space.
///
/// Zero is biased to 1 to preserve the zero-fingerprint sentinel that
/// marks empty buckets.
#[inline]
pub fn compact_fingerprint(fp: u64) -> u32 {
    let t = (fp >> 32) as u32;
    if t == 0 {
        1
    } else {
        t
    }
}

/// Compute the verification fingerprint for the value header.
/// Uses a different seed than the index fingerprint so a collision
/// in one does not imply a collision in the other.
#[inline]
pub fn verify_fingerprint(key: &[u8], seed: u64) -> u64 {
    xxh64(key, seed)
}

// ---------------------------------------------------------------------------
// CompactBucket (8 bytes)
// ---------------------------------------------------------------------------

/// An 8-byte index entry: 32-bit fingerprint + 32-bit aligned offset.
///
/// 8 buckets per 64-byte cache line (2× the density of the old 16-byte bucket).
///
/// `offset` is in VALUE_ALIGNMENT (64-byte) units. `offset × 64` = byte
/// position in `data.bin`. Max addressable: 2^32 × 64 = 256 GB per partition.
///
/// A bucket with `fingerprint == 0` is empty.
#[repr(C)]
#[derive(Clone, Copy, Default, Pod, Zeroable)]
pub struct Bucket {
    pub fingerprint: u32,
    pub offset: u32,
}

impl Bucket {
    /// Create a new bucket, validating that the aligned offset fits in u32.
    ///
    /// `aligned_offset` is in `VALUE_ALIGNMENT` (64-byte) units. The maximum
    /// valid value is `u32::MAX - 1` because `u32::MAX` is reserved as the
    /// `NO_MATCH` sentinel in `ProbeResult::offsets`.
    pub fn new(fingerprint: u32, aligned_offset: u64) -> Result<Self> {
        if aligned_offset > MAX_OFFSET as u64 {
            return Err(Error::OffsetOverflow(aligned_offset));
        }
        Ok(Self {
            fingerprint,
            offset: aligned_offset as u32,
        })
    }

    #[inline]
    pub fn is_empty(self) -> bool {
        self.fingerprint == 0
    }

    /// Byte offset in `data.bin` (`offset × VALUE_ALIGNMENT`).
    #[inline]
    pub fn byte_offset(self) -> u64 {
        self.offset as u64 * VALUE_ALIGNMENT
    }
}

// ---------------------------------------------------------------------------
// Robin Hood insertion
// ---------------------------------------------------------------------------

/// Allocate a hash table and insert all `entries` using Robin Hood hashing.
///
/// Starts at `ceil(n_keys / FILL_RATE)` buckets. On PSL overflow, retries
/// with a 1.5× larger table until insertion succeeds.
///
/// Returns `(table, retries)` where `retries` is the number of times the
/// table was grown due to PSL overflow (0 = no overflow occurred).
pub fn build(entries: &[Bucket]) -> Result<(Vec<Bucket>, usize)> {
    let mut n_buckets = bucket_count(entries.len());
    let mut retries = 0usize;
    loop {
        let mut table = vec![Bucket::default(); n_buckets];
        let mut psls = vec![0u8; n_buckets];
        let mut overflow = false;
        for &e in entries {
            if let Err(Error::PslOverflow { .. }) = insert(&mut table, &mut psls, e) {
                overflow = true;
                break;
            }
        }
        if !overflow {
            return Ok((table, retries));
        }
        let next = ((n_buckets as f64) * 1.5).ceil() as usize;
        log::debug!(
            "Robin Hood PSL overflow at {} buckets ({} keys, {:.0}% fill); \
             growing to {} buckets",
            n_buckets,
            entries.len(),
            entries.len() as f64 / n_buckets as f64 * 100.0,
            next,
        );
        n_buckets = next;
        retries += 1;
    }
}

/// Compute the number of buckets for `n_keys` at the target fill rate.
pub fn bucket_count(n_keys: usize) -> usize {
    ((n_keys as f64) / FILL_RATE).ceil() as usize
}

/// Insert one entry into `table` using Robin Hood displacement.
pub fn insert(table: &mut [Bucket], psls: &mut [u8], entry: Bucket) -> Result<()> {
    debug_assert_eq!(table.len(), psls.len(), "psls must be parallel to table");
    let n = table.len();
    let cfp = entry.fingerprint;
    let mut pos = cfp as usize % n;
    let mut psl = 0u8;
    let mut cur_fp = cfp;
    let mut cur_off = entry.offset;

    loop {
        let slot = table[pos];

        if slot.is_empty() {
            table[pos] = Bucket {
                fingerprint: cur_fp,
                offset: cur_off,
            };
            psls[pos] = psl;
            return Ok(());
        }

        if psls[pos] < psl {
            let evicted_psl = psls[pos];
            let evicted_fp = slot.fingerprint;
            let evicted_off = slot.offset;

            table[pos] = Bucket {
                fingerprint: cur_fp,
                offset: cur_off,
            };
            psls[pos] = psl;

            cur_fp = evicted_fp;
            cur_off = evicted_off;
            psl = evicted_psl;
        }

        pos = (pos + 1) % n;
        if psl == MAX_PSL {
            return Err(Error::PslOverflow {
                psl,
                n_keys: n_occupied(table),
                n_buckets: n,
            });
        }
        psl += 1;
    }
}

fn n_occupied(table: &[Bucket]) -> usize {
    table.iter().filter(|b| !b.is_empty()).count()
}

// ---------------------------------------------------------------------------
// Probe
// ---------------------------------------------------------------------------

/// Sentinel value in `ProbeResult::offsets` meaning "no match at this slot".
pub const NO_MATCH: u32 = u32::MAX;

/// Number of buckets examined per `probe_group` call (one cache line).
pub const GROUP_SIZE: usize = 8;

/// Result of probing one group of 8 buckets.
///
/// `offsets[i]` is the bucket's `offset` field if `fingerprint` matched,
/// or [`NO_MATCH`] otherwise. `done` is true if an empty bucket was found
/// in the group (the caller should stop probing after checking all matches
/// in this group).
pub struct ProbeResult {
    pub offsets: [u32; GROUP_SIZE],
    pub done: bool,
}

/// Compute the home position for a fingerprint in a table of `n` buckets.
#[inline]
pub fn home_position(fingerprint: u64, n: usize) -> usize {
    compact_fingerprint(fingerprint) as usize % n
}

/// Probe one group of 8 buckets starting at `pos`.
///
/// If `pos + 8 > table.len()`, falls back to scalar to handle wrap-around.
///
/// Dispatches to the best available SIMD implementation at runtime.
pub fn probe_group(table: &[Bucket], cfp: u32, pos: usize) -> ProbeResult {
    let n = table.len();

    #[cfg(target_arch = "x86_64")]
    if pos + GROUP_SIZE <= n && is_x86_feature_detected!("avx2") {
        return unsafe { probe_group_avx2(table, cfp, pos) };
    }
    #[cfg(target_arch = "aarch64")]
    if pos + GROUP_SIZE <= n {
        return unsafe { probe_group_neon(table, cfp, pos) };
    }

    probe_group_scalar(table, cfp, pos)
}

/// Scalar group probe — handles wrap-around and groups smaller than 8.
fn probe_group_scalar(table: &[Bucket], cfp: u32, start: usize) -> ProbeResult {
    let n = table.len();
    let mut result = ProbeResult {
        offsets: [NO_MATCH; GROUP_SIZE],
        done: false,
    };

    for i in 0..GROUP_SIZE {
        let pos = (start + i) % n;
        let bucket = table[pos];
        if bucket.is_empty() {
            result.done = true;
            break;
        }
        if bucket.fingerprint == cfp {
            result.offsets[i] = bucket.offset;
        }
    }

    result
}

/// AVX2 group probe: compare 8 fingerprints in one cache line.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn probe_group_avx2(table: &[Bucket], cfp: u32, pos: usize) -> ProbeResult {
    use std::arch::x86_64::*;

    let mut result = ProbeResult {
        offsets: [NO_MATCH; GROUP_SIZE],
        done: false,
    };

    let target = _mm256_set1_epi32(cfp as i32);
    let zero = _mm256_setzero_si256();
    let shuf = _mm256_setr_epi32(0, 2, 4, 6, 0, 2, 4, 6);

    let ptr = table.as_ptr().add(pos) as *const i32;
    let lo = _mm256_loadu_si256(ptr as *const __m256i);
    let hi = _mm256_loadu_si256(ptr.add(8) as *const __m256i);

    let fps_lo = _mm256_permutevar8x32_epi32(lo, shuf);
    let fps_hi = _mm256_permutevar8x32_epi32(hi, shuf);
    let fps = _mm256_permute2x128_si256(fps_lo, fps_hi, 0x20);

    let match_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi32(fps, target)) as u32;
    let empty_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi32(fps, zero)) as u32;

    // Each u32 lane → 4 bits in movemask. Check each lane.
    for lane in 0..8u32 {
        let lane_bits = 0xF << (lane * 4);
        if empty_mask & lane_bits != 0 {
            result.done = true;
            break;
        }
        if match_mask & lane_bits != 0 {
            result.offsets[lane as usize] = table[pos + lane as usize].offset;
        }
    }

    result
}

/// NEON group probe: compare 8 fingerprints in two loads of 4.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn probe_group_neon(table: &[Bucket], cfp: u32, pos: usize) -> ProbeResult {
    use std::arch::aarch64::*;

    let mut result = ProbeResult {
        offsets: [NO_MATCH; GROUP_SIZE],
        done: false,
    };

    let target = vdupq_n_u32(cfp);
    let zero = vdupq_n_u32(0);

    // First 4 buckets.
    let pair0 = vld2q_u32(table.as_ptr().add(pos) as *const u32);
    let match0 = vceqq_u32(pair0.0, target);
    let empty0 = vceqq_u32(pair0.0, zero);

    for i in 0..4u32 {
        if vgetq_lane_u32_dyn(empty0, i) != 0 {
            result.done = true;
            return result;
        }
        if vgetq_lane_u32_dyn(match0, i) != 0 {
            result.offsets[i as usize] = table[pos + i as usize].offset;
        }
    }

    // Second 4 buckets.
    let pair1 = vld2q_u32(table.as_ptr().add(pos + 4) as *const u32);
    let match1 = vceqq_u32(pair1.0, target);
    let empty1 = vceqq_u32(pair1.0, zero);

    for i in 0..4u32 {
        if vgetq_lane_u32_dyn(empty1, i) != 0 {
            result.done = true;
            return result;
        }
        if vgetq_lane_u32_dyn(match1, i) != 0 {
            result.offsets[4 + i as usize] = table[pos + 4 + i as usize].offset;
        }
    }

    result
}

/// Helper: extract lane `i` from a uint32x4_t at runtime.
#[cfg(target_arch = "aarch64")]
#[inline]
unsafe fn vgetq_lane_u32_dyn(v: std::arch::aarch64::uint32x4_t, i: u32) -> u32 {
    use std::arch::aarch64::*;
    match i {
        0 => vgetq_lane_u32::<0>(v),
        1 => vgetq_lane_u32::<1>(v),
        2 => vgetq_lane_u32::<2>(v),
        3 => vgetq_lane_u32::<3>(v),
        _ => unreachable!(),
    }
}

// ---------------------------------------------------------------------------
// Unified index file
// ---------------------------------------------------------------------------

/// Result of writing the unified `index.all` file.
pub struct UnifiedIndexInfo {
    /// Byte offset of each partition's bucket array within `index.all`.
    pub offsets: Vec<u64>,
    /// Number of logical buckets per partition.
    pub n_buckets: Vec<u64>,
}

/// Write all partition bucket tables into a single `index.all` file.
///
/// Each partition is padded to `INDEX_ALIGNMENT` (2 MiB) boundaries except
/// the last. Returns the offsets and bucket counts for `meta.json`.
pub fn write_unified_index(
    path: &std::path::Path,
    tables: &[Vec<Bucket>],
) -> Result<UnifiedIndexInfo> {
    use std::io::{Seek, SeekFrom, Write};

    let mut file = std::fs::File::create(path)?;
    let mut offsets = Vec::with_capacity(tables.len());
    let mut n_buckets = Vec::with_capacity(tables.len());
    let mut file_offset: u64 = 0;

    for (i, table) in tables.iter().enumerate() {
        offsets.push(file_offset);
        n_buckets.push(table.len() as u64);

        let bytes = bytemuck::cast_slice::<Bucket, u8>(table);
        file.write_all(bytes)?;
        file_offset += bytes.len() as u64;

        // Pad to next 2 MiB boundary (skip for last partition).
        if i + 1 < tables.len() && !file_offset.is_multiple_of(INDEX_ALIGNMENT) {
            let aligned = (file_offset + INDEX_ALIGNMENT - 1) & !(INDEX_ALIGNMENT - 1);
            file.set_len(aligned)?;
            file.seek(SeekFrom::Start(aligned))?;
            file_offset = aligned;
        }
    }
    file.flush()?;

    Ok(UnifiedIndexInfo { offsets, n_buckets })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert a value size in bytes to the number of bytes it occupies in
/// `data.bin` after 64-byte alignment padding.
#[inline]
pub fn aligned_size(size: u32) -> u64 {
    let a = VALUE_ALIGNMENT;
    (size as u64 + a - 1) & !(a - 1)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Test helper: probe the entire table for the first match.
    /// Takes a pre-compacted u32 fingerprint.
    fn probe(table: &[Bucket], cfp: u32) -> Option<u64> {
        let n = table.len();
        if n == 0 {
            return None;
        }
        let mut pos = cfp as usize % n;
        let mut steps = 0usize;

        while steps < n {
            let result = probe_group(table, cfp, pos);
            for i in 0..GROUP_SIZE {
                if result.offsets[i] != NO_MATCH {
                    return Some(result.offsets[i] as u64 * VALUE_ALIGNMENT);
                }
            }
            if result.done {
                return None;
            }
            pos = (pos + GROUP_SIZE) % n;
            steps += GROUP_SIZE;
        }
        None
    }

    // --- Bucket ---

    #[test]
    fn bucket_empty_sentinel() {
        let empty = Bucket::default();
        assert!(empty.is_empty());
        let occupied = Bucket {
            fingerprint: 1,
            offset: 0,
        };
        assert!(!occupied.is_empty());
    }

    #[test]
    fn bucket_byte_offset() {
        let b = Bucket {
            fingerprint: 1,
            offset: 42,
        };
        assert_eq!(b.byte_offset(), 42 * 64);
    }

    // --- Fingerprint ---

    #[test]
    fn fingerprint_never_zero() {
        for i in 0u64..1024 {
            assert_ne!(fingerprint(&i.to_le_bytes()), 0);
        }
    }

    #[test]
    fn compact_fingerprint_never_zero() {
        for i in 0u64..1024 {
            assert_ne!(compact_fingerprint(fingerprint(&i.to_le_bytes())), 0);
        }
    }

    // --- aligned_size ---

    #[test]
    fn aligned_size_multiples() {
        assert_eq!(aligned_size(0), 0);
        assert_eq!(aligned_size(1), 64);
        assert_eq!(aligned_size(64), 64);
        assert_eq!(aligned_size(65), 128);
        assert_eq!(aligned_size(128), 128);
        assert_eq!(aligned_size(100), 128);
    }

    #[test]
    fn bucket_rejects_max_offset() {
        let cfp = compact_fingerprint(fingerprint(b"key"));
        assert!(Bucket::new(cfp, u32::MAX as u64).is_err());
        // One below the sentinel is still valid.
        assert!(Bucket::new(cfp, (u32::MAX - 1) as u64).is_ok());
    }

    // --- build / probe ---

    fn make_entry(key: &[u8], byte_offset: u64) -> Bucket {
        let cfp = compact_fingerprint(fingerprint(key));
        let off = byte_offset / VALUE_ALIGNMENT;
        Bucket::new(cfp, off).unwrap()
    }

    #[test]
    fn build_and_probe_basic() {
        let entries = vec![
            make_entry(b"hello", 0),
            make_entry(b"world", 64),
            make_entry(b"foo", 128),
            make_entry(b"bar", 192),
        ];
        let (table, _) = build(&entries).unwrap();

        for e in &entries {
            let result = probe(&table, e.fingerprint);
            assert!(result.is_some(), "fingerprint {} not found", e.fingerprint);
            assert_eq!(result.unwrap(), e.offset as u64 * VALUE_ALIGNMENT);
        }
    }

    #[test]
    fn probe_miss() {
        let entries = vec![make_entry(b"hello", 0)];
        let (table, _) = build(&entries).unwrap();
        let absent = compact_fingerprint(fingerprint(b"absent"));
        assert_eq!(probe(&table, absent), None);
    }

    #[test]
    fn build_many() {
        let n = 10_000usize;
        let entries: Vec<Bucket> = (0..n)
            .map(|i| {
                let key = i.to_le_bytes();
                make_entry(&key, (i as u64) * 64)
            })
            .collect();

        let (table, _) = build(&entries).unwrap();

        for e in &entries {
            assert!(
                probe(&table, e.fingerprint).is_some(),
                "miss for fingerprint {}",
                e.fingerprint
            );
        }
    }

    // --- probe_group correctness ---

    fn simd_test_table() -> (Vec<Bucket>, Vec<Bucket>) {
        let n = 1_000usize;
        let entries: Vec<Bucket> = (0..n)
            .map(|i| make_entry(&(i as u64).to_le_bytes(), (i as u64) * 64))
            .collect();
        let (table, _) = build(&entries).unwrap();
        (table, entries)
    }

    /// Verify that probe_group (which dispatches to SIMD) agrees with
    /// probe_group_scalar for every entry in a 1000-key table.
    #[test]
    fn probe_group_matches_scalar() {
        let (table, entries) = simd_test_table();
        for e in &entries {
            let cfp = e.fingerprint;
            let pos = cfp as usize % table.len();

            let expected = probe_group_scalar(&table, cfp, pos);
            let got = probe_group(&table, cfp, pos);

            assert_eq!(
                got.offsets, expected.offsets,
                "offsets mismatch for fp={}",
                e.fingerprint
            );
            assert_eq!(
                got.done, expected.done,
                "done mismatch for fp={}",
                e.fingerprint
            );
        }
    }

    /// The convenience probe() function should find every inserted key.
    #[test]
    fn probe_finds_all_keys() {
        let (table, entries) = simd_test_table();
        for e in &entries {
            let result = probe(&table, e.fingerprint);
            assert!(result.is_some(), "miss for fingerprint {}", e.fingerprint);
            assert_eq!(result.unwrap(), e.offset as u64 * VALUE_ALIGNMENT);
        }
        assert_eq!(
            probe(&table, compact_fingerprint(fingerprint(b"absent"))),
            None
        );
    }

    // --- last-group boundary ---

    #[test]
    #[allow(clippy::needless_range_loop)]
    fn probe_last_group_boundary() {
        // Note: these hand-built tests pass small integers as fingerprints
        // directly. This works because compact_fingerprint(x) == x for
        // small non-zero x. The build/probe tests above exercise the real
        // fingerprint pipeline.
        //
        // Table: [∅ ∅ ∅ ∅ 4 5 6 7], home(4)=4. Group starting at 4
        // should find offset 4 at the first slot.
        let n = 8usize;
        let mut table = vec![Bucket::default(); n];
        for i in 4..8usize {
            table[i] = Bucket {
                fingerprint: i as u32,
                offset: i as u32,
            };
        }

        let result = probe_group(&table, 4, 4);
        assert_eq!(result.offsets[0], 4);

        // Convenience probe should also find it.
        assert_eq!(probe(&table, 4), Some(4u64 * 64));
    }

    // --- long PSL chain (multiple group iterations) ---

    #[test]
    #[allow(clippy::needless_range_loop)]
    fn probe_long_psl_chain() {
        // 32 buckets, fill positions 4..21 (18 consecutive occupied slots)
        // with distinct fingerprints, then place target cfp=99 at position 21.
        // home(99) = 99 % 32 = 3, but position 3 is empty so that won't work.
        // Instead: home = 4 (use cfp that homes to 4), fill 4..20 with
        // blockers, target at 20. That's PSL=16, spanning 3 group iterations.
        let n = 32usize;
        let mut table = vec![Bucket::default(); n];

        // Target: cfp = 4, so home = 4 % 32 = 4.
        let target_cfp = 4u32;
        // Fill positions 4..20 with blockers (different fingerprints).
        for i in 4..20usize {
            table[i] = Bucket {
                fingerprint: 100 + i as u32,
                offset: i as u32,
            };
        }
        // Place the actual target at position 20 (PSL = 16).
        table[20] = Bucket {
            fingerprint: target_cfp,
            offset: 42,
        };

        // probe must iterate: group at 4 (no match), group at 12 (no match),
        // group at 20 (match at slot 0).
        assert_eq!(probe(&table, target_cfp), Some(42u64 * 64));
    }

    #[test]
    #[allow(clippy::needless_range_loop)]
    fn probe_long_psl_chain_miss() {
        // Same layout as above but target cfp is absent.
        // home(4) = 4, positions 4..20 are occupied blockers, 21 is empty.
        // Probe must scan 3 groups before hitting the empty slot and returning None.
        let n = 32usize;
        let mut table = vec![Bucket::default(); n];
        for i in 4..21usize {
            table[i] = Bucket {
                fingerprint: 100 + i as u32,
                offset: i as u32,
            };
        }
        // cfp=4 homes to 4, scans groups at 4, 12, 20 — position 21 is empty → None.
        assert_eq!(probe(&table, 4), None);
    }

    // --- wrap-around ---

    #[test]
    fn probe_wrap_around() {
        let n = 9usize;
        let mut table = vec![Bucket::default(); n];
        table[6] = Bucket {
            fingerprint: 33,
            offset: 10,
        };
        table[7] = Bucket {
            fingerprint: 44,
            offset: 11,
        };
        table[8] = Bucket {
            fingerprint: 55,
            offset: 12,
        };
        table[0] = Bucket {
            fingerprint: 99,
            offset: 13,
        };
        table[1] = Bucket {
            fingerprint: 6,
            offset: 42,
        };

        // cfp=6, home=6. First group wraps — scalar fallback.
        // Should eventually find offset 42 at position 1.
        let expected = Some(42u64 * 64);
        assert_eq!(probe(&table, 6), expected);
    }
}
