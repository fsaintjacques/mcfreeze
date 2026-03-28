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

/// Compute the xxhash64 fingerprint of a key.
///
/// A result of `0` is biased to `1` to preserve the zero-fingerprint
/// sentinel that marks empty buckets.
#[inline]
pub fn fingerprint(key: &[u8]) -> u64 {
    let h = xxh64(key, 0);
    if h == 0 { 1 } else { h }
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
/// Terminates at the first empty bucket (fingerprint == 0).
pub fn probe(table: &[Bucket], fingerprint: u64) -> Option<(u64, u32)> {
    let n = table.len();
    if n == 0 {
        return None;
    }
    let mut pos   = fingerprint as usize % n;
    let mut steps = 0usize;

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
}
