use std::fs::File;
use std::path::Path;

use memmap2::MmapOptions;

use crate::{
    data::{pread, pread_up_to},
    index::{
        self, compact_fingerprint, fingerprint, home_position, probe_group, verify_fingerprint,
        Bucket, GROUP_SIZE, NO_MATCH,
    },
    meta::{index_path, partition_dir, Layout, Meta, VALUE_ALIGNMENT, VALUE_HEADER_SIZE},
    Result,
};

use crate::snapshot::GetOutcome;

// ---------------------------------------------------------------------------
// PartitionReader — index probe + data read for a single partition
// ---------------------------------------------------------------------------

/// Per-partition state: a borrowed bucket table and an open data file.
///
/// The bucket slice can come from any source (mmap, heap, local SSD cache).
pub struct PartitionReader<'a> {
    buckets: &'a [Bucket],
    data: &'a File,
    verify_seed: u64,
}

impl<'a> PartitionReader<'a> {
    pub fn new(buckets: &'a [Bucket], data: &'a File, verify_seed: u64) -> Self {
        Self {
            buckets,
            data,
            verify_seed,
        }
    }

    /// Look up `key`.  Returns `Miss { io: false }` when the index yields
    /// no candidate slots (no disk touched), `Miss { io: true }` when
    /// candidates existed — a compact-fingerprint collision paid one or
    /// more `pread`s — but none verified, or [`GetOutcome::Hit`] with the
    /// value.
    ///
    /// The caller must supply the full 64-bit `fingerprint` so that
    /// `compact_fingerprint` and `home_position` can be derived.
    pub fn get(&self, key: &[u8], fp: u64) -> Result<GetOutcome> {
        let n = self.buckets.len();
        if n == 0 {
            return Ok(GetOutcome::Miss { io: false });
        }

        let cfp = compact_fingerprint(fp);
        let expected_vfp = verify_fingerprint(key, self.verify_seed);
        let mut pos = home_position(fp, n);
        let mut steps = 0usize;
        let mut had_candidate = false;

        while steps < n {
            let result = probe_group(self.buckets, cfp, pos);

            for i in 0..GROUP_SIZE {
                if result.offsets[i] == NO_MATCH {
                    continue;
                }
                had_candidate = true;
                let byte_offset = result.offsets[i] as u64 * VALUE_ALIGNMENT;
                if let Some(value) = self.read_and_verify(byte_offset, expected_vfp)? {
                    return Ok(GetOutcome::Hit(value));
                }
            }

            if result.done {
                break;
            }

            pos = (pos + GROUP_SIZE) % n;
            steps += GROUP_SIZE;
        }

        Ok(GetOutcome::Miss { io: had_candidate })
    }

    /// Speculatively read to the next 4KB page boundary. This avoids a
    /// second pread for values that fit within the remainder of the page
    /// (the common case for values under ~4KB).
    fn read_and_verify(&self, byte_offset: u64, expected_vfp: u64) -> Result<Option<Vec<u8>>> {
        const PAGE_SIZE: u64 = 4096;
        let page_remaining = PAGE_SIZE - (byte_offset % PAGE_SIZE);
        // byte_offset is VALUE_ALIGNMENT-aligned and PAGE_SIZE % VALUE_ALIGNMENT == 0,
        // so page_remaining is always >= VALUE_ALIGNMENT.
        debug_assert!(page_remaining >= VALUE_ALIGNMENT);
        let speculative_size = page_remaining as u32;

        let first_read = pread_up_to(self.data, byte_offset, speculative_size)?;
        if first_read.len() < VALUE_HEADER_SIZE {
            return Err(crate::Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "short read: value header truncated",
            )));
        }

        let stored_vfp = u64::from_le_bytes(first_read[..8].try_into().unwrap());
        if stored_vfp != expected_vfp {
            return Ok(None);
        }

        let byte_len = u32::from_le_bytes(first_read[8..12].try_into().unwrap()) as usize;
        let on_disk_size = VALUE_HEADER_SIZE.saturating_add(byte_len);

        if on_disk_size <= first_read.len() {
            Ok(Some(
                first_read[VALUE_HEADER_SIZE..VALUE_HEADER_SIZE + byte_len].to_vec(),
            ))
        } else {
            let total = match u32::try_from(on_disk_size) {
                Ok(s) => index::aligned_size(s) as u32,
                Err(_) => return Ok(None),
            };
            let raw = pread(self.data, byte_offset, total)?;
            Ok(Some(
                raw[VALUE_HEADER_SIZE..VALUE_HEADER_SIZE + byte_len].to_vec(),
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// SnapshotReader
// ---------------------------------------------------------------------------

/// Read-only handle to a complete snapshot directory.
///
/// The unified `index.all` is memory-mapped once; each partition's bucket
/// table is a slice within that mapping. `data.bin` files are opened for
/// on-demand `pread`.
///
/// Constructed by `Snapshot::open` with metadata parsed by
/// `SnapshotDesc::load`.
pub struct SnapshotReader {
    layout: Layout,
    verify_seed: u64,
    index_mmap: memmap2::Mmap,
    data_files: Vec<File>,
    /// Per-partition bucket range within `index_mmap`: (byte_offset, n_buckets).
    ranges: Vec<(usize, usize)>,
}

impl SnapshotReader {
    /// Open a snapshot directory from already-parsed metadata (see
    /// `SnapshotDesc::load`). Mmaps `index.all`, opens every partition's
    /// `data.bin`, and synchronously populates the index into the page
    /// cache (`MADV_POPULATE_READ`, Linux 5.14+) so the first serving
    /// request does not page-fault against cold storage. Blocking and
    /// potentially heavy — call from a blocking context.
    pub fn open(root: impl AsRef<Path>, meta: &Meta) -> Result<Self> {
        let root = root.as_ref();
        let layout = meta.layout()?;

        let idx_file = File::open(index_path(root))?;
        let index_mmap = unsafe { MmapOptions::new().map(&idx_file)? };

        #[cfg(target_os = "linux")]
        {
            // Access-pattern hints: best-effort, performance-only.
            let _ = index_mmap.advise(memmap2::Advice::Random);
            let _ = index_mmap.advise(memmap2::Advice::HugePage);
            // Populate is load-bearing: open() promises a reader at
            // steady-state latency, so a populate that fails for a real
            // reason (ENOMEM, I/O error faulting from storage) must fail
            // the open. EINVAL/unsupported means the kernel (< 5.14)
            // does not know the hint — lazy faulting is the best
            // available there, not an error.
            if let Err(e) = index_mmap.advise(memmap2::Advice::PopulateRead) {
                match e.kind() {
                    std::io::ErrorKind::InvalidInput | std::io::ErrorKind::Unsupported => {}
                    _ => return Err(e.into()),
                }
            }
        }

        let mut data_files = Vec::with_capacity(layout.n_partitions as usize);
        let mut ranges = Vec::with_capacity(layout.n_partitions as usize);
        for (i, pm) in meta.partitions.iter().enumerate() {
            data_files.push(File::open(
                partition_dir(root, layout.n_partitions, i).join("data.bin"),
            )?);
            ranges.push((pm.index_offset as usize, pm.index_n_buckets as usize));
        }

        Ok(Self {
            layout,
            verify_seed: meta.verify_seed,
            index_mmap,
            data_files,
            ranges,
        })
    }

    /// Borrow the `PartitionReader` for the given partition index.
    pub fn partition(&self, idx: usize) -> PartitionReader<'_> {
        let (offset, n_buckets) = self.ranges[idx];
        let bucket_bytes =
            &self.index_mmap[offset..offset + n_buckets * std::mem::size_of::<Bucket>()];
        PartitionReader::new(
            bytemuck::cast_slice(bucket_bytes),
            &self.data_files[idx],
            self.verify_seed,
        )
    }

    /// Look up `key`.  See [`GetOutcome`] for the three non-error outcomes.
    pub fn get(&self, key: &[u8]) -> Result<GetOutcome> {
        let fp = fingerprint(key);
        self.partition(self.layout.partition_of(fp)).get(key, fp)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::SnapshotWriter;
    use tempfile::TempDir;

    fn build_snapshot(pairs: &[(&[u8], &[u8])], n_partitions: u32) -> TempDir {
        let dir = TempDir::new().unwrap();
        let mut w = SnapshotWriter::new(dir.path(), n_partitions).unwrap();
        for &(k, v) in pairs {
            w.write(k, v).unwrap();
        }
        w.finish().unwrap();
        dir
    }

    /// Parse meta.json and open the reader, as `Snapshot::open` would.
    fn open_snapshot(root: &Path) -> SnapshotReader {
        let json = std::fs::read_to_string(root.join("meta.json")).unwrap();
        let meta: Meta = serde_json::from_str(&json).unwrap();
        SnapshotReader::open(root, &meta).unwrap()
    }

    // --- basic roundtrip ---

    fn assert_hit(o: GetOutcome, expected: &[u8]) {
        match o {
            GetOutcome::Hit(v) => assert_eq!(v, expected),
            other => panic!("expected Hit({expected:?}), got {other:?}"),
        }
    }

    #[test]
    fn get_existing_keys() {
        let pairs: &[(&[u8], &[u8])] = &[
            (b"hello", b"world"),
            (b"foo", b"bar"),
            (b"alpha", b"beta gamma delta"),
        ];
        let dir = build_snapshot(pairs, 4);
        let r = open_snapshot(dir.path());

        for &(k, v) in pairs {
            assert_hit(r.get(k).unwrap(), v);
        }
    }

    #[test]
    fn get_missing_key_returns_miss() {
        let dir = build_snapshot(&[(b"present", b"yes")], 4);
        let r = open_snapshot(dir.path());
        assert_eq!(r.get(b"absent").unwrap(), GetOutcome::Miss { io: false });
    }

    // --- value sizes ---

    #[test]
    fn get_empty_value() {
        let dir = build_snapshot(&[(b"k", b"")], 1);
        let r = open_snapshot(dir.path());
        assert_hit(r.get(b"k").unwrap(), b"");
    }

    #[test]
    fn get_large_value() {
        let big = vec![0xABu8; 1024 * 1024]; // 1 MiB
        let dir = build_snapshot(&[(b"big", &big)], 1);
        let r = open_snapshot(dir.path());
        assert_hit(r.get(b"big").unwrap(), &big);
    }

    // --- many keys ---

    #[test]
    fn roundtrip_many_keys() {
        let n = 10_000usize;
        let vals: Vec<(Vec<u8>, Vec<u8>)> = (0..n)
            .map(|i| {
                (
                    format!("key-{i}").into_bytes(),
                    format!("value-{i}").into_bytes(),
                )
            })
            .collect();
        let pairs: Vec<(&[u8], &[u8])> = vals
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();

        let dir = build_snapshot(&pairs, 64);
        let r = open_snapshot(dir.path());

        for (k, v) in &vals {
            assert_hit(r.get(k).unwrap(), v);
        }
    }

    // --- n_partitions=1 ---

    #[test]
    fn single_partition_roundtrip() {
        let pairs: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
        let dir = build_snapshot(pairs, 1);
        let r = open_snapshot(dir.path());
        for &(k, v) in pairs {
            assert_hit(r.get(k).unwrap(), v);
        }
    }

    // --- speculative read ---

    #[test]
    fn speculative_read_covers_typical_value() {
        // 200-byte value + 12-byte header = 212 bytes, fits in one 4KB page read.
        let val = vec![0xCCu8; 200];
        let dir = build_snapshot(&[(b"k", &val)], 1);
        let r = open_snapshot(dir.path());
        assert_hit(r.get(b"k").unwrap(), &val);
    }

    #[test]
    fn speculative_read_falls_back_to_second_pread() {
        // Value larger than 4KB forces the second pread path.
        let val = vec![0xDDu8; 8192];
        let dir = build_snapshot(&[(b"big", &val)], 1);
        let r = open_snapshot(dir.path());
        assert_hit(r.get(b"big").unwrap(), &val);
    }

    #[test]
    fn speculative_read_near_eof() {
        // Single small value: data.bin is only one 64-byte block.
        // The speculative read requests up to 4KB but the file is smaller.
        let dir = build_snapshot(&[(b"tiny", b"x")], 1);
        let r = open_snapshot(dir.path());
        assert_hit(r.get(b"tiny").unwrap(), b"x");
    }

    #[test]
    fn truncated_data_file_returns_error() {
        let dir = build_snapshot(&[(b"k", b"v")], 1);
        let r = open_snapshot(dir.path());
        // Truncate data.bin to 0 — should error, not silently miss.
        std::fs::OpenOptions::new()
            .write(true)
            .open(dir.path().join("data/part-0/data.bin"))
            .unwrap()
            .set_len(0)
            .unwrap();
        assert!(r.get(b"k").is_err());
    }

    // --- 32-bit fingerprint collision ---

    /// Find two keys that produce the same compact_fingerprint and land in the
    /// same partition. Both must be retrievable (the reader continues probing
    /// on verify-fingerprint mismatch).
    #[test]
    fn compact_fingerprint_collision_both_retrievable() {
        use crate::index::{compact_fingerprint, fingerprint};
        use std::collections::HashMap;

        let mut seen: HashMap<u32, String> = HashMap::new();
        for i in 0u64..200_000 {
            let k = format!("collision-{i}");
            let fp = fingerprint(k.as_bytes());
            let cfp = compact_fingerprint(fp);
            if let Some(prev) = seen.get(&cfp) {
                let pairs: &[(&[u8], &[u8])] =
                    &[(prev.as_bytes(), b"value-a"), (k.as_bytes(), b"value-b")];
                let dir = build_snapshot(pairs, 1);
                let r = open_snapshot(dir.path());

                assert_hit(r.get(prev.as_bytes()).unwrap(), b"value-a");
                assert_hit(r.get(k.as_bytes()).unwrap(), b"value-b");
                return;
            }
            seen.insert(cfp, k);
        }
        panic!("failed to find a 32-bit fingerprint collision in 200K keys");
    }

    /// Writing key_a but querying key_b — where both share a compact-fingerprint —
    /// yields `Miss { io: true }`, not a free miss. The index returns a
    /// candidate slot, the pread succeeds, but the stored verify-fingerprint
    /// doesn't match and no further candidates exist.
    #[test]
    fn cfp_collision_yields_paid_io_miss() {
        use crate::index::{compact_fingerprint, fingerprint};
        use std::collections::HashMap;

        let mut seen: HashMap<u32, String> = HashMap::new();
        for i in 0u64..200_000 {
            let k = format!("collision-{i}");
            let fp = fingerprint(k.as_bytes());
            let cfp = compact_fingerprint(fp);
            if let Some(prev) = seen.get(&cfp) {
                let dir = build_snapshot(&[(prev.as_bytes(), b"stored")], 1);
                let r = open_snapshot(dir.path());
                assert_eq!(r.get(k.as_bytes()).unwrap(), GetOutcome::Miss { io: true });
                return;
            }
            seen.insert(cfp, k);
        }
        panic!("failed to find a 32-bit fingerprint collision in 200K keys");
    }
}
