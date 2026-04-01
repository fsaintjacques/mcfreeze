use bytemuck::{Pod, Zeroable};
use xxhash_rust::xxh64::xxh64;

use crate::{
    meta::{FILL_RATE, VALUE_ALIGNMENT},
    Error, Result,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum aligned offset that fits in a u32 (256 GB per partition at 64B alignment).
const MAX_OFFSET: u32 = u32::MAX;
const MAX_PSL:    u8  = u8::MAX;

pub const INDEX_MAGIC: [u8; 8] = *b"KVFXIDX\n";
pub const INDEX_HEADER_SIZE: usize = 64;

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
    if h == 0 { 1 } else { h }
}

/// Truncate a 64-bit fingerprint to the 32-bit bucket fingerprint.
/// Zero is biased to 1 (the full fingerprint is never zero, but truncation
/// could produce zero in theory — belt and suspenders).
#[inline]
pub fn compact_fingerprint(fp: u64) -> u32 {
    let t = fp as u32;
    if t == 0 { 1 } else { t }
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
    pub offset:      u32,
}

impl Bucket {
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
// Index header (64 bytes, one cache line)
// ---------------------------------------------------------------------------

/// Written at the start of every `index.idx` file.
pub struct IndexHeader {
    pub n_buckets: u64,
    pub n_keys:    u64,
}

impl IndexHeader {
    /// Serialize to the 64-byte on-disk representation.
    pub fn to_bytes(&self) -> [u8; INDEX_HEADER_SIZE] {
        let mut buf = [0u8; INDEX_HEADER_SIZE];
        buf[0..8].copy_from_slice(&INDEX_MAGIC);
        buf[8..10].copy_from_slice(&(crate::meta::FORMAT_VERSION as u16).to_le_bytes());
        // bytes [10..16]: pad (zero)
        buf[16..24].copy_from_slice(&self.n_buckets.to_le_bytes());
        buf[24..32].copy_from_slice(&self.n_keys.to_le_bytes());
        // bytes [32..64]: reserved (zero)
        buf
    }

    /// Deserialize from a 64-byte buffer.
    pub fn from_bytes(buf: &[u8; INDEX_HEADER_SIZE]) -> Result<Self> {
        if buf[0..8] != INDEX_MAGIC {
            return Err(Error::InvalidMagic);
        }
        let version = u16::from_le_bytes(buf[8..10].try_into().unwrap()) as u32;
        if version != crate::meta::FORMAT_VERSION {
            return Err(Error::VersionMismatch {
                expected: crate::meta::FORMAT_VERSION,
                got:      version,
            });
        }
        let n_buckets = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        let n_keys    = u64::from_le_bytes(buf[24..32].try_into().unwrap());
        Ok(Self { n_buckets, n_keys })
    }
}

// ---------------------------------------------------------------------------
// Robin Hood insertion
// ---------------------------------------------------------------------------

/// A raw entry accumulated during the data-write phase.
#[derive(Clone, Copy)]
pub struct RawEntry {
    /// Full 64-bit fingerprint (used for partitioning; truncated to 32 bits for the bucket).
    pub fingerprint:    u64,
    /// Aligned offset in VALUE_ALIGNMENT units.
    pub aligned_offset: u32,
}

impl RawEntry {
    /// Validate that offset fits in u32.
    pub fn new(fingerprint: u64, aligned_offset: u64) -> Result<Self> {
        if aligned_offset > MAX_OFFSET as u64 {
            return Err(Error::OffsetOverflow(aligned_offset));
        }
        Ok(Self { fingerprint, aligned_offset: aligned_offset as u32 })
    }
}

/// Allocate a hash table and insert all `entries` using Robin Hood hashing.
///
/// Starts at `ceil(n_keys / FILL_RATE)` buckets. On PSL overflow, retries
/// with a 1.5× larger table until insertion succeeds.
///
/// Returns `(table, retries)` where `retries` is the number of times the
/// table was grown due to PSL overflow (0 = no overflow occurred).
pub fn build(entries: &[RawEntry]) -> Result<(Vec<Bucket>, usize)> {
    let mut n_buckets = bucket_count(entries.len());
    let mut retries   = 0usize;
    loop {
        let mut table    = vec![Bucket::default(); n_buckets];
        let mut psls     = vec![0u8; n_buckets];
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
        retries  += 1;
    }
}

/// Compute the number of buckets for `n_keys` at the target fill rate.
pub fn bucket_count(n_keys: usize) -> usize {
    ((n_keys as f64) / FILL_RATE).ceil() as usize
}

/// Insert one entry into `table` using Robin Hood displacement.
pub fn insert(table: &mut [Bucket], psls: &mut [u8], entry: RawEntry) -> Result<()> {
    debug_assert_eq!(table.len(), psls.len(), "psls must be parallel to table");
    let n   = table.len();
    let cfp = compact_fingerprint(entry.fingerprint);
    let mut pos = cfp as usize % n;
    let mut psl = 0u8;
    let mut cur_fp  = cfp;
    let mut cur_off = entry.aligned_offset;

    loop {
        let slot = table[pos];

        if slot.is_empty() {
            table[pos] = Bucket { fingerprint: cur_fp, offset: cur_off };
            psls[pos]  = psl;
            return Ok(());
        }

        if psls[pos] < psl {
            let evicted_psl = psls[pos];
            let evicted_fp  = slot.fingerprint;
            let evicted_off = slot.offset;

            table[pos] = Bucket { fingerprint: cur_fp, offset: cur_off };
            psls[pos]  = psl;

            cur_fp  = evicted_fp;
            cur_off = evicted_off;
            psl     = evicted_psl;
        }

        pos = (pos + 1) % n;
        if psl == MAX_PSL {
            return Err(Error::PslOverflow {
                psl,
                n_keys:    n_occupied(table),
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

/// Look up `fingerprint` in an immutable bucket table.
///
/// Returns `byte_offset` on a hit, or `None` on a miss.
///
/// The fingerprint is truncated to 32 bits for the bucket comparison.
///
/// Dispatches to the best available SIMD implementation at runtime:
/// AVX-512F → AVX2 → NEON → scalar.
pub fn probe(table: &[Bucket], fingerprint: u64) -> Option<u64> {
    let n = table.len();
    if n == 0 {
        return None;
    }

    let cfp = compact_fingerprint(fingerprint);

    #[cfg(target_arch = "x86_64")]
    if is_x86_feature_detected!("avx2") {
        return unsafe { probe_avx2(table, cfp) };
    }
    #[cfg(target_arch = "aarch64")]
    return unsafe { probe_neon(table, cfp) };

    #[allow(unreachable_code)]
    probe_scalar(table, cfp)
}

/// Scalar probe starting from the home slot.
fn probe_scalar(table: &[Bucket], cfp: u32) -> Option<u64> {
    let n   = table.len();
    let pos = cfp as usize % n;
    probe_scalar_from(table, cfp, pos, 0, n)
}

/// Scalar probe resuming from an arbitrary position.
#[inline]
fn probe_scalar_from(
    table:   &[Bucket],
    cfp:     u32,
    mut pos: usize,
    mut steps: usize,
    n:       usize,
) -> Option<u64> {
    loop {
        debug_assert!(steps < n, "probe scanned all {n} buckets without finding an empty slot");
        let bucket = table[pos];
        if bucket.is_empty() {
            return None;
        }
        if bucket.fingerprint == cfp {
            return Some(bucket.byte_offset());
        }
        pos    = (pos + 1) % n;
        steps += 1;
    }
}

/// AVX2 probe: compare 8 fingerprints per iteration (one 64-byte cache line).
///
/// Each Bucket is 8 bytes (u32 fp + u32 offset). 8 buckets = 64 bytes = one cache line.
/// Load two 256-bit registers, shuffle to extract all 8 fingerprints into one
/// 256-bit register of u32 lanes, then vpcmpeqd.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn probe_avx2(table: &[Bucket], cfp: u32) -> Option<u64> {
    use std::arch::x86_64::*;

    const LANES: usize = 8;
    let n         = table.len();
    let mut pos   = cfp as usize % n;
    let mut steps = 0usize;

    let target = _mm256_set1_epi32(cfp as i32);
    let zero   = _mm256_setzero_si256();

    // Shuffle mask to extract fingerprints (even u32s) from interleaved [fp,off,fp,off,...].
    // For each 128-bit lane: bytes [0..3] → pos 0, bytes [8..11] → pos 1, etc.
    let shuf = _mm256_setr_epi32(0, 2, 4, 6, 0, 2, 4, 6);

    while steps + LANES <= n {
        if pos + LANES > n {
            break;
        }

        // Two 256-bit loads cover 64 bytes = 8 buckets.
        let ptr = table.as_ptr().add(pos) as *const i32;
        let lo  = _mm256_loadu_si256(ptr as *const __m256i);           // buckets 0-3
        let hi  = _mm256_loadu_si256(ptr.add(8) as *const __m256i);    // buckets 4-7

        // permutevar8x32 extracts even u32s (fingerprints) from each half.
        let fps_lo = _mm256_permutevar8x32_epi32(lo, shuf); // [fp0,fp1,fp2,fp3, fp0,fp1,fp2,fp3]
        let fps_hi = _mm256_permutevar8x32_epi32(hi, shuf); // [fp4,fp5,fp6,fp7, fp4,fp5,fp6,fp7]
        // Blend low 128 bits of fps_lo with high 128 bits of fps_hi.
        let fps = _mm256_permute2x128_si256(fps_lo, fps_hi, 0x20); // [fp0..fp3, fp4..fp7]

        let match_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi32(fps, target)) as u32;
        let empty_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi32(fps, zero))   as u32;

        if (match_mask | empty_mask) != 0 {
            // Each u32 lane produces 4 set bits in movemask; trailing_zeros / 4 → lane index.
            let match_lane = match_mask.trailing_zeros() / 4;
            let empty_lane = empty_mask.trailing_zeros() / 4;

            if match_mask != 0 && (empty_mask == 0 || match_lane < empty_lane) {
                let b = table[pos + match_lane as usize];
                return Some(b.byte_offset());
            }
            return None;
        }

        pos    += LANES;
        if pos >= n { pos -= n; }
        steps  += LANES;
    }

    probe_scalar_from(table, cfp, pos, steps, n)
}

/// NEON probe: compare 4 fingerprints per iteration (32 bytes = 4 buckets).
///
/// `vld2q_u32` deinterleaves on load: fingerprints land in `pair.0`,
/// offsets in `pair.1` — no shuffle needed.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn probe_neon(table: &[Bucket], cfp: u32) -> Option<u64> {
    use std::arch::aarch64::*;

    const LANES: usize = 4;
    let n         = table.len();
    let mut pos   = cfp as usize % n;
    let mut steps = 0usize;

    let target = vdupq_n_u32(cfp);
    let zero   = vdupq_n_u32(0);

    while steps + LANES <= n {
        if pos + LANES > n {
            break;
        }

        // vld2q_u32 loads 8 u32s and deinterleaves:
        //   pair.0 = [fp0, fp1, fp2, fp3]  pair.1 = [off0, off1, off2, off3]
        let pair = vld2q_u32(table.as_ptr().add(pos) as *const u32);
        let fps  = pair.0;

        let match_v = vceqq_u32(fps, target);
        let empty_v = vceqq_u32(fps, zero);

        // Process in probe order.
        if vgetq_lane_u32::<0>(empty_v) != 0 { return None; }
        if vgetq_lane_u32::<0>(match_v) != 0 { return Some(table[pos].byte_offset()); }
        if vgetq_lane_u32::<1>(empty_v) != 0 { return None; }
        if vgetq_lane_u32::<1>(match_v) != 0 { return Some(table[pos + 1].byte_offset()); }
        if vgetq_lane_u32::<2>(empty_v) != 0 { return None; }
        if vgetq_lane_u32::<2>(match_v) != 0 { return Some(table[pos + 2].byte_offset()); }
        if vgetq_lane_u32::<3>(empty_v) != 0 { return None; }
        if vgetq_lane_u32::<3>(match_v) != 0 { return Some(table[pos + 3].byte_offset()); }

        pos    += LANES;
        if pos >= n { pos -= n; }
        steps  += LANES;
    }

    probe_scalar_from(table, cfp, pos, steps, n)
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

    // --- Bucket ---

    #[test]
    fn bucket_empty_sentinel() {
        let empty = Bucket::default();
        assert!(empty.is_empty());
        let occupied = Bucket { fingerprint: 1, offset: 0 };
        assert!(!occupied.is_empty());
    }

    #[test]
    fn bucket_byte_offset() {
        let b = Bucket { fingerprint: 1, offset: 42 };
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
        assert_eq!(aligned_size(0),   0);
        assert_eq!(aligned_size(1),   64);
        assert_eq!(aligned_size(64),  64);
        assert_eq!(aligned_size(65),  128);
        assert_eq!(aligned_size(128), 128);
        assert_eq!(aligned_size(100), 128);
    }

    // --- build / probe ---

    fn make_entry(key: &[u8], byte_offset: u64) -> RawEntry {
        let fp  = fingerprint(key);
        let off = byte_offset / VALUE_ALIGNMENT;
        RawEntry::new(fp, off).unwrap()
    }

    #[test]
    fn build_and_probe_basic() {
        let entries = vec![
            make_entry(b"hello",   0),
            make_entry(b"world",   64),
            make_entry(b"foo",     128),
            make_entry(b"bar",     192),
        ];
        let (table, _) = build(&entries).unwrap();

        for e in &entries {
            let result = probe(&table, e.fingerprint);
            assert!(result.is_some(), "fingerprint {} not found", e.fingerprint);
            assert_eq!(result.unwrap(), e.aligned_offset as u64 * VALUE_ALIGNMENT);
        }
    }

    #[test]
    fn probe_miss() {
        let entries    = vec![make_entry(b"hello", 0)];
        let (table, _) = build(&entries).unwrap();
        let absent     = fingerprint(b"absent");
        assert_eq!(probe(&table, absent), None);
    }

    #[test]
    fn build_many() {
        let n = 10_000usize;
        let entries: Vec<RawEntry> = (0..n)
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

    #[test]
    fn header_roundtrip() {
        let hdr = IndexHeader { n_buckets: 12345, n_keys: 9999 };
        let bytes = hdr.to_bytes();
        let decoded = IndexHeader::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.n_buckets, 12345);
        assert_eq!(decoded.n_keys,    9999);
    }

    #[test]
    fn header_wrong_magic() {
        let mut bytes = IndexHeader { n_buckets: 1, n_keys: 1 }.to_bytes();
        bytes[0] = 0xFF;
        assert!(IndexHeader::from_bytes(&bytes).is_err());
    }

    // --- SIMD probe correctness ---

    fn simd_test_table() -> (Vec<Bucket>, Vec<RawEntry>) {
        let n = 1_000usize;
        let entries: Vec<RawEntry> = (0..n)
            .map(|i| make_entry(&(i as u64).to_le_bytes(), (i as u64) * 64))
            .collect();
        let (table, _) = build(&entries).unwrap();
        (table, entries)
    }

    #[test]
    #[cfg(target_arch = "x86_64")]
    fn probe_avx2_matches_scalar() {
        if !is_x86_feature_detected!("avx2") { return; }
        let (table, entries) = simd_test_table();
        for e in &entries {
            let cfp      = compact_fingerprint(e.fingerprint);
            let expected = probe_scalar(&table, cfp);
            let got      = unsafe { probe_avx2(&table, cfp) };
            assert_eq!(got, expected, "avx2 mismatch for fp={}", e.fingerprint);
        }
        assert_eq!(unsafe { probe_avx2(&table, compact_fingerprint(fingerprint(b"absent"))) }, None);
    }

    #[test]
    #[cfg(target_arch = "aarch64")]
    fn probe_neon_matches_scalar() {
        let (table, entries) = simd_test_table();
        for e in &entries {
            let cfp      = compact_fingerprint(e.fingerprint);
            let expected = probe_scalar(&table, cfp);
            let got      = unsafe { probe_neon(&table, cfp) };
            assert_eq!(got, expected, "neon mismatch for fp={}", e.fingerprint);
        }
        assert_eq!(unsafe { probe_neon(&table, compact_fingerprint(fingerprint(b"absent"))) }, None);
    }

    // --- last-group boundary ---

    #[test]
    fn probe_last_group_boundary() {
        let n         = 8usize;
        let target_fp = 4u32;

        let mut table = vec![Bucket::default(); n];
        for i in 4..8usize {
            table[i] = Bucket { fingerprint: i as u32, offset: i as u32 };
        }
        let expected = Some(4u64 * 64);

        assert_eq!(probe_scalar(&table, target_fp), expected, "scalar");

        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("avx2") {
            assert_eq!(unsafe { probe_avx2(&table, target_fp) }, expected, "avx2");
        }
        #[cfg(target_arch = "aarch64")]
        assert_eq!(unsafe { probe_neon(&table, target_fp) }, expected, "neon");
    }

    // --- wrap-around ---

    #[test]
    fn probe_wrap_around() {
        let n          = 9usize;
        let target_fp  = 6u32;
        let expected   = Some(42u64 * 64);

        let mut table = vec![Bucket::default(); n];
        table[6] = Bucket { fingerprint: 33, offset: 10 };
        table[7] = Bucket { fingerprint: 44, offset: 11 };
        table[8] = Bucket { fingerprint: 55, offset: 12 };
        table[0] = Bucket { fingerprint: 99, offset: 13 };
        table[1] = Bucket { fingerprint: target_fp, offset: 42 };

        assert_eq!(probe_scalar(&table, target_fp), expected, "scalar");

        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("avx2") {
            assert_eq!(unsafe { probe_avx2(&table, target_fp) }, expected, "avx2");
        }
        #[cfg(target_arch = "aarch64")]
        assert_eq!(unsafe { probe_neon(&table, target_fp) }, expected, "neon");
    }
}
