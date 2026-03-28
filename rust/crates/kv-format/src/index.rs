use bytemuck::{Pod, Zeroable};
use xxhash_rust::xxh64::xxh64;

use crate::{
    meta::{FILL_RATE, OFFSET_BITS, PSL_BITS, SIZE_BITS, VALUE_ALIGNMENT},
    Error, Result,
};

// ---------------------------------------------------------------------------
// Constants derived from bit-field widths
// ---------------------------------------------------------------------------

const OFFSET_SHIFT: u8 = SIZE_BITS + PSL_BITS; // 30 — bits [63:30]
const SIZE_SHIFT:   u8 = PSL_BITS;             //  7 — bits [29:7]
const SIZE_MASK:   u64 = (1u64 << SIZE_BITS) - 1; // 0x7FFFFF
const PSL_MASK:    u64 = (1u64 << PSL_BITS)  - 1; // 0x7F
const MAX_OFFSET:  u64 = (1u64 << OFFSET_BITS) - 1;
const MAX_SIZE:    u32 = (1u32 << SIZE_BITS)   - 1; // 8 MiB - 1
const MAX_PSL:      u8 = (1u8  << PSL_BITS)   - 1; // 127

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
/// `loc` packs three sub-fields (MSB → LSB):
///
/// ```text
/// bits [63:30]  aligned_offset  (OFFSET_BITS = 34)  in VALUE_ALIGNMENT units
/// bits [29: 8]  size            (SIZE_BITS   = 22)  in bytes
/// bits [ 7: 0]  psl             (PSL_BITS    =  8)  probe sequence length
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

    /// Encode `(aligned_offset, size, psl)` into the `loc` field.
    #[inline]
    pub fn encode_loc(aligned_offset: u64, size: u32, psl: u8) -> u64 {
        (aligned_offset << OFFSET_SHIFT) | ((size as u64) << SIZE_SHIFT) | (psl as u64)
    }

    /// Byte offset in `data.bin` (`aligned_offset × VALUE_ALIGNMENT`).
    #[inline]
    pub fn byte_offset(self) -> u64 {
        (self.loc >> OFFSET_SHIFT) << 6 // × 64
    }

    /// Value size in bytes.
    #[inline]
    pub fn size(self) -> u32 {
        ((self.loc >> SIZE_SHIFT) & SIZE_MASK) as u32
    }

    /// Probe sequence length stored in this bucket.
    #[inline]
    pub fn psl(self) -> u8 {
        (self.loc & PSL_MASK) as u8
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
/// Returns the bucket slice sized to `ceil(n_keys / FILL_RATE)`.
pub fn build(entries: &[RawEntry]) -> Vec<Bucket> {
    let n_buckets = bucket_count(entries.len());
    let mut table = vec![Bucket::default(); n_buckets];
    for &e in entries {
        insert(&mut table, e);
    }
    table
}

/// Compute the number of buckets for `n_keys` at the target fill rate.
pub fn bucket_count(n_keys: usize) -> usize {
    ((n_keys as f64) / FILL_RATE).ceil() as usize
}

/// Insert one entry into `table` using Robin Hood displacement.
///
/// # Panics
/// Panics if the table is full (all buckets occupied). Callers must ensure the
/// table is sized with sufficient headroom via [`bucket_count`].
pub fn insert(table: &mut [Bucket], mut entry: RawEntry) {
    let n   = table.len();
    let mut pos = entry.fingerprint as usize % n;
    let mut psl = 0u8;

    loop {
        let slot = table[pos];

        if slot.is_empty() {
            table[pos].fingerprint = entry.fingerprint;
            table[pos].loc         = Bucket::encode_loc(entry.aligned_offset, entry.size, psl);
            return;
        }

        // Robin Hood: evict the "rich" occupant (lower PSL) in favour of
        // the "poor" incoming entry (higher PSL).
        if slot.psl() < psl {
            table[pos].fingerprint = entry.fingerprint;
            table[pos].loc         = Bucket::encode_loc(entry.aligned_offset, entry.size, psl);

            // Continue reinserting the evicted entry.
            entry = RawEntry {
                fingerprint:    slot.fingerprint,
                aligned_offset: slot.loc >> OFFSET_SHIFT,
                size:           slot.size(),
            };
            psl = slot.psl();
        }

        pos = (pos + 1) % n;
        assert!(psl < MAX_PSL, "PSL overflow ({psl}): table is full or fill rate too high");
        psl += 1;
    }
}

// ---------------------------------------------------------------------------
// Probe
// ---------------------------------------------------------------------------

/// Look up `fingerprint` in an immutable bucket table.
///
/// Returns `(byte_offset, size)` on a hit, or `None` on a miss.
///
/// The Robin Hood invariant allows early termination: if the stored PSL at the
/// current position is less than the expected PSL, the key cannot exist further
/// along the probe sequence.
pub fn probe(table: &[Bucket], fingerprint: u64) -> Option<(u64, u32)> {
    let n            = table.len();
    let mut pos      = fingerprint as usize % n;
    let mut expected = 0u8;

    loop {
        let bucket = table[pos];

        if bucket.is_empty() {
            return None;
        }

        if bucket.psl() < expected {
            return None; // Robin Hood invariant: key is absent.
        }

        if bucket.fingerprint == fingerprint {
            return Some((bucket.byte_offset(), bucket.size()));
        }

        pos      = (pos + 1) % n;
        expected = expected.saturating_add(1);
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
        let cases: &[(u64, u32, u8)] = &[
            (0, 0, 0),
            (1, 100, 2),
            (MAX_OFFSET, MAX_SIZE, MAX_PSL),
            ((1 << 17), 4096, 42),
        ];
        for &(off, sz, psl) in cases {
            let loc = Bucket::encode_loc(off, sz, psl);
            let b = Bucket { fingerprint: 1, loc };
            assert_eq!(b.byte_offset(), off << 6, "byte_offset for off={off}");
            assert_eq!(b.size(),        sz,       "size for sz={sz}");
            assert_eq!(b.psl(),         psl,      "psl for psl={psl}");
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
        let table = build(&entries);

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
        let entries = vec![make_entry(b"hello", 0, 5)];
        let table   = build(&entries);
        let absent  = fingerprint(b"absent");
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

        let table = build(&entries);

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
