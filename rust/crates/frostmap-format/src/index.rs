use bytemuck::{Pod, Zeroable};
use xxhash_rust::xxh64::xxh64;

use crate::{
    meta::{FILL_RATE, OFFSET_BITS, SIZE_BITS, VALUE_ALIGNMENT},
    Error, Result,
};

// ---------------------------------------------------------------------------
// Constants derived from bit-field widths
// ---------------------------------------------------------------------------

const OFFSET_SHIFT: u8 = SIZE_BITS;                 // 27 — bits [63:27]
const SIZE_MASK:   u64 = (1u64 << SIZE_BITS) - 1;   // 0x7FF_FFFF
const MAX_OFFSET:  u64 = (1u64 << OFFSET_BITS) - 1;
const MAX_SIZE:    u32 = (1u32 << SIZE_BITS)   - 1;  // 128 MiB - 1
const MAX_PSL:      u8 = u8::MAX;

pub const INDEX_MAGIC: [u8; 8] = *b"KVFXIDX\n";
pub const INDEX_HEADER_SIZE: usize = 64;

// ---------------------------------------------------------------------------
// Fingerprinting
// ---------------------------------------------------------------------------

/// Compute the xxhash64 fingerprint of a key (seed 0).
///
/// A result of `0` is biased to `1` to preserve the zero-fingerprint
/// sentinel that marks empty buckets.
#[inline]
pub fn fingerprint(key: &[u8]) -> u64 {
    let h = xxh64(key, 0);
    if h == 0 { 1 } else { h }
}

/// Compute the verification fingerprint for the value header.
/// Uses a different seed than the index fingerprint so a collision
/// in one does not imply a collision in the other.
#[inline]
pub fn verify_fingerprint(key: &[u8], seed: u64) -> u64 {
    xxh64(key, seed)
}

// ---------------------------------------------------------------------------
// Bucket
// ---------------------------------------------------------------------------

/// A 16-byte index entry.
///
/// Two fields occupy one `u64` each, giving 4 buckets per 64-byte cache line.
///
/// `loc` packs two sub-fields (MSB → LSB):
///
/// ```text
/// bits [63:27]  aligned_offset  (OFFSET_BITS = 37)  in VALUE_ALIGNMENT units
/// bits [26: 0]  size            (SIZE_BITS   = 27)  in bytes
/// ```
///
/// A bucket with `fingerprint == 0` is empty.
#[repr(C)]
#[derive(Clone, Copy, Default, Pod, Zeroable)]
pub struct Bucket {
    pub fingerprint: u64,
    pub loc:         u64,
}

impl Bucket {
    #[inline]
    pub fn is_empty(self) -> bool {
        self.fingerprint == 0
    }

    /// Encode `(aligned_offset, size)` into the `loc` field.
    #[inline]
    pub fn encode_loc(aligned_offset: u64, size: u32) -> u64 {
        (aligned_offset << OFFSET_SHIFT) | (size as u64)
    }

    /// Byte offset in `data.bin` (`aligned_offset × VALUE_ALIGNMENT`).
    #[inline]
    pub fn byte_offset(self) -> u64 {
        (self.loc >> OFFSET_SHIFT) << 6 // × 64
    }

    /// Value size in bytes.
    #[inline]
    pub fn size(self) -> u32 {
        (self.loc & SIZE_MASK) as u32
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
    pub fingerprint:    u64,
    pub aligned_offset: u64,
    pub size:           u32,
}

impl RawEntry {
    /// Validate that offset and size fit within their bit fields.
    pub fn new(fingerprint: u64, aligned_offset: u64, size: u32) -> Result<Self> {
        if aligned_offset > MAX_OFFSET {
            return Err(Error::OffsetOverflow(aligned_offset));
        }
        if size > MAX_SIZE {
            return Err(Error::ValueTooLarge { size: size as usize });
        }
        Ok(Self { fingerprint, aligned_offset, size })
    }
}

/// Allocate a hash table and insert all `entries` using Robin Hood hashing.
///
/// Starts at `ceil(n_keys / FILL_RATE)` buckets. On PSL overflow, retries
/// with a 1.5× larger table until insertion succeeds.
///
/// PSL is tracked in a temporary columnar `Vec<u8>` during construction and
/// discarded once the table is built — it is not stored in the on-disk format.
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
///
/// `psls` is a parallel columnar array of probe sequence lengths, one `u8`
/// per bucket. It is allocated by `build()` and discarded after construction.
///
/// Returns `Err(Error::PslOverflow)` if the probe sequence exceeds `MAX_PSL`,
/// which indicates the table is full or the fill rate is too high.
pub fn insert(table: &mut [Bucket], psls: &mut [u8], mut entry: RawEntry) -> Result<()> {
    debug_assert_eq!(table.len(), psls.len(), "psls must be parallel to table");
    let n   = table.len();
    let mut pos = entry.fingerprint as usize % n;
    let mut psl = 0u8;

    loop {
        let slot = table[pos];

        if slot.is_empty() {
            table[pos].fingerprint = entry.fingerprint;
            table[pos].loc         = Bucket::encode_loc(entry.aligned_offset, entry.size);
            psls[pos]              = psl;
            return Ok(());
        }

        // Robin Hood: evict the "rich" occupant (lower PSL) in favour of
        // the "poor" incoming entry (higher PSL).
        if psls[pos] < psl {
            let evicted_psl        = psls[pos];
            table[pos].fingerprint = entry.fingerprint;
            table[pos].loc         = Bucket::encode_loc(entry.aligned_offset, entry.size);
            psls[pos]              = psl;

            // Continue reinserting the evicted entry.
            entry = RawEntry {
                fingerprint:    slot.fingerprint,
                aligned_offset: slot.loc >> OFFSET_SHIFT,
                size:           slot.size(),
            };
            psl = evicted_psl;
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
/// Returns `(byte_offset, size)` on a hit, or `None` on a miss.
///
/// Dispatches to the best available SIMD implementation at runtime:
/// AVX-512F → AVX2 → NEON → scalar.
///
/// `is_x86_feature_detected!` caches the result in a static atomic after the
/// first call; subsequent calls are a single atomic load.
pub fn probe(table: &[Bucket], fingerprint: u64) -> Option<(u64, u32)> {
    let n = table.len();
    if n == 0 {
        return None;
    }

    #[cfg(target_arch = "x86_64")]
    if is_x86_feature_detected!("avx512f") {
        return unsafe { probe_avx512(table, fingerprint) };
    }
    #[cfg(target_arch = "x86_64")]
    if is_x86_feature_detected!("avx2") {
        return unsafe { probe_avx2(table, fingerprint) };
    }
    // NEON is mandatory on AArch64 (ARMv8+); no runtime check required.
    #[cfg(target_arch = "aarch64")]
    return unsafe { probe_neon(table, fingerprint) };

    #[allow(unreachable_code)]
    probe_scalar(table, fingerprint)
}

/// Scalar probe starting from the home slot.
fn probe_scalar(table: &[Bucket], fingerprint: u64) -> Option<(u64, u32)> {
    let n   = table.len();
    let pos = fingerprint as usize % n;
    probe_scalar_from(table, fingerprint, pos, 0, n)
}

/// Scalar probe resuming from an arbitrary position, used as the tail of SIMD
/// paths when the remaining group would wrap around the table boundary.
#[inline]
fn probe_scalar_from(
    table:       &[Bucket],
    fingerprint: u64,
    mut pos:     usize,
    mut steps:   usize,
    n:           usize,
) -> Option<(u64, u32)> {
    loop {
        debug_assert!(steps < n, "probe scanned all {n} buckets without finding an empty slot");
        let bucket = table[pos];
        if bucket.is_empty() {
            return None;
        }
        if bucket.fingerprint == fingerprint {
            return Some((bucket.byte_offset(), bucket.size()));
        }
        pos    = (pos + 1) % n;
        steps += 1;
    }
}

/// AVX2 probe: compare 4 fingerprints per iteration (one 64-byte cache line).
///
/// Memory layout for 4 consecutive buckets:
///   [fp0, loc0, fp1, loc1, fp2, loc2, fp3, loc3]
///
/// Two 256-bit loads + two permutes deinterleave fingerprints into one register
/// for a single `vpcmpeqq` comparison.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn probe_avx2(table: &[Bucket], fingerprint: u64) -> Option<(u64, u32)> {
    use std::arch::x86_64::*;

    const LANES: usize = 4;
    let n         = table.len();
    let mut pos   = fingerprint as usize % n;
    let mut steps = 0usize;

    let target = _mm256_set1_epi64x(fingerprint as i64);
    let zero   = _mm256_setzero_si256();

    while steps + LANES <= n {
        if pos + LANES > n {
            // Group would wrap the table boundary: fall through to scalar.
            break;
        }

        // Two 256-bit loads cover 64 bytes = 4 buckets.
        let ptr = table.as_ptr().add(pos) as *const __m256i;
        let lo  = _mm256_loadu_si256(ptr);        // [fp0, loc0, fp1, loc1]
        let hi  = _mm256_loadu_si256(ptr.add(1)); // [fp2, loc2, fp3, loc3]

        // Deinterleave fingerprints into one register.
        // permute4x64 imm=0x88 (0b10001000): picks qwords [0,2,0,2].
        let plo = _mm256_permute4x64_epi64(lo, 0x88); // [fp0, fp1, fp0, fp1]
        let phi = _mm256_permute4x64_epi64(hi, 0x88); // [fp2, fp3, fp2, fp3]
        // permute2x128 imm=0x20: low-128 of plo | low-128 of phi.
        let fps = _mm256_permute2x128_si256(plo, phi, 0x20); // [fp0, fp1, fp2, fp3]

        // movemask_epi8 gives 8 bits per 64-bit lane (all-1 or all-0).
        // Lane k occupies bits [8k .. 8k+7]; trailing_zeros / 8 → lane index.
        let match_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi64(fps, target)) as u32;
        let empty_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi64(fps, zero))   as u32;

        if (match_mask | empty_mask) != 0 {
            let match_lane = match_mask.trailing_zeros() / 8; // 0–3, or 4 if absent
            let empty_lane = empty_mask.trailing_zeros() / 8; // 0–3, or 4 if absent

            if match_mask != 0 && (empty_mask == 0 || match_lane < empty_lane) {
                let b = table[pos + match_lane as usize];
                return Some((b.byte_offset(), b.size()));
            }
            return None;
        }

        pos    += LANES;
        if pos >= n { pos -= n; }
        steps  += LANES;
    }

    probe_scalar_from(table, fingerprint, pos, steps, n)
}

/// AVX-512F probe: compare 4 fingerprints per iteration using `vpcompressq`
/// and a k-register compare — one load, one compress, one compare.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn probe_avx512(table: &[Bucket], fingerprint: u64) -> Option<(u64, u32)> {
    use std::arch::x86_64::*;

    const LANES: usize = 4;
    let n         = table.len();
    let mut pos   = fingerprint as usize % n;
    let mut steps = 0usize;

    let target = _mm512_set1_epi64(fingerprint as i64);
    let zero   = _mm512_setzero_si512();

    while steps + LANES <= n {
        if pos + LANES > n {
            break;
        }

        // One 512-bit load covers 64 bytes = 4 buckets.
        // Layout: [fp0, loc0, fp1, loc1, fp2, loc2, fp3, loc3]
        // _mm512_loadu_si512 takes *const i32 on stable Rust (void* binding).
        let data = _mm512_loadu_si512(table.as_ptr().add(pos) as *const i32);

        // mask_compress with 0x55 (bits 0,2,4,6) gathers even qwords into lanes 0–3;
        // lanes 4–7 are filled with `zero` (the fill-source, not the empty sentinel).
        //   result: [fp0, fp1, fp2, fp3, 0, 0, 0, 0]
        let fps = _mm512_mask_compress_epi64(zero, 0x55, data);

        // k-mask compare: result is a 1-bit-per-lane u8; mask 0x0F limits to lanes 0–3.
        // `empty_sentinel` is semantically distinct from the compress fill-source above.
        let empty_sentinel = _mm512_setzero_si512();
        let match_mask: u8 = _mm512_mask_cmpeq_epi64_mask(0x0F, fps, target);
        let empty_mask: u8 = _mm512_mask_cmpeq_epi64_mask(0x0F, fps, empty_sentinel);

        if (match_mask | empty_mask) != 0 {
            let match_lane = match_mask.trailing_zeros(); // 0–3, or 8 if absent
            let empty_lane = empty_mask.trailing_zeros(); // 0–3, or 8 if absent

            if match_mask != 0 && (empty_mask == 0 || match_lane < empty_lane) {
                let b = table[pos + match_lane as usize];
                return Some((b.byte_offset(), b.size()));
            }
            return None;
        }

        pos    += LANES;
        if pos >= n { pos -= n; }
        steps  += LANES;
    }

    probe_scalar_from(table, fingerprint, pos, steps, n)
}

/// NEON probe: compare 2 fingerprints per iteration.
///
/// `vld2q_u64` deinterleaves on load: the two fingerprints land in `pair.0`
/// and the two `loc` values land in `pair.1` — no shuffle needed.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn probe_neon(table: &[Bucket], fingerprint: u64) -> Option<(u64, u32)> {
    use std::arch::aarch64::*;

    const LANES: usize = 2;
    let n         = table.len();
    let mut pos   = fingerprint as usize % n;
    let mut steps = 0usize;

    let target = vdupq_n_u64(fingerprint);
    let zero   = vdupq_n_u64(0);

    while steps + LANES <= n {
        if pos + LANES > n {
            break;
        }

        // vld2q_u64 loads 4 u64s and deinterleaves:
        //   pair.0 = [fp0,  fp1 ]   pair.1 = [loc0, loc1]
        let pair = vld2q_u64(table.as_ptr().add(pos) as *const u64);
        let fps  = pair.0;

        let match_v = vceqq_u64(fps, target); // u64::MAX per lane on match, 0 otherwise
        let empty_v = vceqq_u64(fps, zero);

        // Process in probe order (lane 0 has the shorter probe distance).
        if vgetq_lane_u64::<0>(empty_v) != 0 { return None; }
        if vgetq_lane_u64::<0>(match_v) != 0 { let b = table[pos];     return Some((b.byte_offset(), b.size())); }
        if vgetq_lane_u64::<1>(empty_v) != 0 { return None; }
        if vgetq_lane_u64::<1>(match_v) != 0 { let b = table[pos + 1]; return Some((b.byte_offset(), b.size())); }

        pos    += LANES;
        if pos >= n { pos -= n; }
        steps  += LANES;
    }

    probe_scalar_from(table, fingerprint, pos, steps, n)
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

    // --- Bucket encode / decode ---

    #[test]
    fn bucket_encode_decode_roundtrip() {
        let cases: &[(u64, u32)] = &[
            (0, 0),
            (1, 100),
            (MAX_OFFSET, MAX_SIZE),
            (1 << 17, 4096),
        ];
        for &(off, sz) in cases {
            let loc = Bucket::encode_loc(off, sz);
            let b = Bucket { fingerprint: 1, loc };
            assert_eq!(b.byte_offset(), off << 6, "byte_offset for off={off}");
            assert_eq!(b.size(),        sz,       "size for sz={sz}");
        }
    }

    #[test]
    fn bucket_empty_sentinel() {
        let empty = Bucket::default();
        assert!(empty.is_empty());
        let occupied = Bucket { fingerprint: 1, loc: 0 };
        assert!(!occupied.is_empty());
    }

    // --- Fingerprint ---

    #[test]
    fn fingerprint_never_zero() {
        // The zero-bias: if xxh64 produces 0, we return 1.
        // We can't force that collision deterministically, but we verify the
        // contract holds for a variety of inputs.
        for i in 0u64..1024 {
            assert_ne!(fingerprint(&i.to_le_bytes()), 0);
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

    fn make_entry(key: &[u8], byte_offset: u64, size: u32) -> RawEntry {
        let fp  = fingerprint(key);
        let off = byte_offset / VALUE_ALIGNMENT;
        RawEntry::new(fp, off, size).unwrap()
    }

    #[test]
    fn build_and_probe_basic() {
        let entries = vec![
            make_entry(b"hello",   0,   5),
            make_entry(b"world",   64,  5),
            make_entry(b"foo",     128, 3),
            make_entry(b"bar",     192, 3),
        ];
        let (table, _) = build(&entries).unwrap();

        for e in &entries {
            let result = probe(&table, e.fingerprint);
            assert!(result.is_some(), "fingerprint {} not found", e.fingerprint);
            let (off, sz) = result.unwrap();
            assert_eq!(off, e.aligned_offset * VALUE_ALIGNMENT);
            assert_eq!(sz,  e.size);
        }
    }

    #[test]
    fn probe_miss() {
        let entries    = vec![make_entry(b"hello", 0, 5)];
        let (table, _) = build(&entries).unwrap();
        let absent     = fingerprint(b"absent");
        assert_eq!(probe(&table, absent), None);
    }

    #[test]
    fn build_many() {
        // Verify correctness under load with 10 000 entries.
        let n = 10_000usize;
        let entries: Vec<RawEntry> = (0..n)
            .map(|i| {
                let key = i.to_le_bytes();
                make_entry(&key, (i as u64) * 64, 8)
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

    // --- SIMD probe correctness: each ISA path must agree with scalar ---

    fn simd_test_table() -> (Vec<Bucket>, Vec<RawEntry>) {
        let n = 1_000usize;
        let entries: Vec<RawEntry> = (0..n)
            .map(|i| make_entry(&(i as u64).to_le_bytes(), (i as u64) * 64, 8))
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
            let expected = probe_scalar(&table, e.fingerprint);
            let got      = unsafe { probe_avx2(&table, e.fingerprint) };
            assert_eq!(got, expected, "avx2 mismatch for fp={}", e.fingerprint);
        }
        assert_eq!(unsafe { probe_avx2(&table, fingerprint(b"absent")) }, None);
    }

    #[test]
    #[cfg(target_arch = "x86_64")]
    fn probe_avx512_matches_scalar() {
        if !is_x86_feature_detected!("avx512f") { return; }
        let (table, entries) = simd_test_table();
        for e in &entries {
            let expected = probe_scalar(&table, e.fingerprint);
            let got      = unsafe { probe_avx512(&table, e.fingerprint) };
            assert_eq!(got, expected, "avx512 mismatch for fp={}", e.fingerprint);
        }
        assert_eq!(unsafe { probe_avx512(&table, fingerprint(b"absent")) }, None);
    }

    #[test]
    #[cfg(target_arch = "aarch64")]
    fn probe_neon_matches_scalar() {
        let (table, entries) = simd_test_table();
        for e in &entries {
            let expected = probe_scalar(&table, e.fingerprint);
            let got      = unsafe { probe_neon(&table, e.fingerprint) };
            assert_eq!(got, expected, "neon mismatch for fp={}", e.fingerprint);
        }
        assert_eq!(unsafe { probe_neon(&table, fingerprint(b"absent")) }, None);
    }

    // --- last-group boundary (pos + LANES == n, no wrap) ---

    /// Verify that the SIMD loop correctly processes the last group when it ends
    /// exactly at the table boundary (`pos + LANES == n`), i.e. no break is taken
    /// and the scalar tail is never entered.
    ///
    /// Layout for LANES=4 (n=8, target at pos=4 = n-LANES):
    ///   pos: 0  1  2  3  4(hit)  5  6  7
    ///   fp:  ∅  ∅  ∅  ∅  4       5  6  7
    ///
    /// home(4) = 4 % 8 = 4. The SIMD group [4,5,6,7] is processed with 4+4==8
    /// (not > 8), so the loop does not break. Match found at lane 0 of that group.
    ///
    /// For NEON (LANES=2) the same table exercises the [4,5] and [6,7] groups,
    /// with the hit in the first lane of [4,5].
    #[test]
    fn probe_last_group_boundary() {
        let n         = 8usize;   // n % 4 == 0 and n % 2 == 0
        let target_fp = 4u64;     // 4 % 8 == 4  →  home at pos 4 = n - LANES(4)

        let mut table = vec![Bucket::default(); n];
        // Fill positions 4–7 with non-empty entries; 0–3 stay empty.
        for i in 4..8usize {
            table[i] = Bucket {
                fingerprint: i as u64,
                loc:         Bucket::encode_loc(i as u64, i as u32),
            };
        }
        let expected = Some(((4u64) << 6, 4u32));

        assert_eq!(probe_scalar(&table, target_fp), expected, "scalar");
        assert_eq!(probe(&table, target_fp),        expected, "dispatch");

        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("avx2") {
            assert_eq!(unsafe { probe_avx2(&table, target_fp) }, expected, "avx2");
        }
        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("avx512f") {
            assert_eq!(unsafe { probe_avx512(&table, target_fp) }, expected, "avx512");
        }
        #[cfg(target_arch = "aarch64")]
        assert_eq!(unsafe { probe_neon(&table, target_fp) }, expected, "neon");
    }

    // --- wrap-around ---

    /// Build a table where the probe for `target_fp` must wrap past the last
    /// bucket to find the key, exercising the scalar tail of every SIMD path.
    ///
    /// Layout (n=9):
    ///   pos: 0    1      2  3  4  5  6   7   8
    ///   fp:  99   6(hit) ∅  ∅  ∅  ∅  33  44  55
    ///
    /// home(6) = 6 % 9 = 6.
    ///   AVX2/AVX-512 (LANES=4): pos=6, 6+4=10 > 9 → break; scalar: 6→7→8→0→1 ✓
    ///   NEON (LANES=2):         pos=6, loads [6,7] (no hit); pos=8, 8+2=10>9 → break; scalar: 8→0→1 ✓
    #[test]
    fn probe_wrap_around() {
        let n          = 9usize;
        let target_fp  = 6u64; // 6 % 9 == 6
        let target_loc = Bucket::encode_loc(42, 7);
        let expected   = Some((42u64 << 6, 7u32)); // byte_offset = 42*64, size = 7

        let mut table = vec![Bucket::default(); n];
        table[6] = Bucket { fingerprint: 33, loc: Bucket::encode_loc(10, 1) };
        table[7] = Bucket { fingerprint: 44, loc: Bucket::encode_loc(11, 1) };
        table[8] = Bucket { fingerprint: 55, loc: Bucket::encode_loc(12, 1) };
        table[0] = Bucket { fingerprint: 99, loc: Bucket::encode_loc(13, 1) };
        table[1] = Bucket { fingerprint: target_fp, loc: target_loc };

        assert_eq!(probe_scalar(&table, target_fp), expected, "scalar");
        assert_eq!(probe(&table, target_fp),        expected, "dispatch");

        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("avx2") {
            assert_eq!(unsafe { probe_avx2(&table, target_fp) }, expected, "avx2");
        }
        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("avx512f") {
            assert_eq!(unsafe { probe_avx512(&table, target_fp) }, expected, "avx512");
        }
        #[cfg(target_arch = "aarch64")]
        assert_eq!(unsafe { probe_neon(&table, target_fp) }, expected, "neon");
    }
}
