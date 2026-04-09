use std::fs::{self, File};
use std::path::Path;

use memmap2::MmapOptions;

use crate::{
    data::pread,
    index::{
        self, compact_fingerprint, fingerprint, home_position, probe_group, verify_fingerprint,
        Bucket, GROUP_SIZE, NO_MATCH,
    },
    meta::{index_path, partition_dir, Layout, Meta, VALUE_ALIGNMENT, VALUE_HEADER_SIZE},
    Result,
};

// ---------------------------------------------------------------------------
// PartitionSlice — per-partition view into the unified index mmap
// ---------------------------------------------------------------------------

/// Per-partition metadata: byte range within the shared index mmap + data file.
struct PartitionSlice {
    /// Byte offset of this partition's bucket array within the index mmap.
    bucket_offset: usize,
    /// Number of logical buckets in this partition.
    n_buckets: usize,
    /// Open file descriptor for `pread` calls into `data.bin`.
    data: File,
}

// ---------------------------------------------------------------------------
// SnapshotReader
// ---------------------------------------------------------------------------

/// Read-only handle to a complete snapshot directory.
///
/// The unified `index.all` is memory-mapped once; each partition is a slice
/// within that mapping. `data.bin` files are opened for on-demand `pread`.
///
/// ```text
/// let r = SnapshotReader::open("/snapshots/v42")?;
/// if let Some(val) = r.get(b"my-key")? {
///     // use val
/// }
/// ```
pub struct SnapshotReader {
    layout: Layout,
    verify_seed: u64,
    /// Single mmap of `index.all` (all partitions, 2MB-aligned).
    index_mmap: memmap2::Mmap,
    partitions: Vec<PartitionSlice>,
}

impl SnapshotReader {
    /// Open a snapshot directory.  Reads `meta.json`, validates the format,
    /// mmaps `index.all`, then opens every partition's `data.bin`.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();

        let json = fs::read_to_string(root.join("meta.json"))?;
        let meta: Meta = serde_json::from_str(&json)?;
        let layout = meta.layout()?;

        // Single mmap of index.all.
        let idx_file = File::open(index_path(root))?;
        let index_mmap = unsafe { MmapOptions::new().map(&idx_file)? };

        // Advise huge pages on Linux for TLB efficiency (best-effort;
        // MADV_HUGEPAGE returns EINVAL on older kernels / some filesystems).
        #[cfg(target_os = "linux")]
        let _ = index_mmap.advise(memmap2::Advice::HugePage);

        let n = layout.n_partitions as usize;
        let mut partitions = Vec::with_capacity(n);
        for (i, pm) in meta.partitions.iter().enumerate() {
            let data = File::open(partition_dir(root, layout.n_partitions, i).join("data.bin"))?;
            partitions.push(PartitionSlice {
                bucket_offset: pm.index_offset as usize,
                n_buckets: pm.index_n_buckets as usize,
                data,
            });
        }

        Ok(Self {
            layout,
            verify_seed: meta.verify_seed,
            index_mmap,
            partitions,
        })
    }

    /// Look up `key` and return its value, or `None` if not present.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let fp = fingerprint(key);
        let idx = self.layout.partition_of(fp);
        let ps = &self.partitions[idx];

        let n = ps.n_buckets;
        if n == 0 {
            return Ok(None);
        }

        let bucket_bytes = &self.index_mmap
            [ps.bucket_offset..ps.bucket_offset + n * std::mem::size_of::<Bucket>()];
        let table: &[Bucket] = bytemuck::cast_slice(bucket_bytes);

        let cfp = compact_fingerprint(fp);
        let expected_vfp = verify_fingerprint(key, self.verify_seed);
        let mut pos = home_position(fp, n);
        let mut steps = 0usize;

        while steps < n {
            let result = probe_group(table, cfp, pos);

            for i in 0..GROUP_SIZE {
                if result.offsets[i] == NO_MATCH {
                    continue;
                }
                let byte_offset = result.offsets[i] as u64 * VALUE_ALIGNMENT;
                if let Some(value) = read_and_verify(&ps.data, byte_offset, expected_vfp)? {
                    return Ok(Some(value));
                }
            }

            if result.done {
                return Ok(None);
            }

            pos = (pos + GROUP_SIZE) % n;
            steps += GROUP_SIZE;
        }

        Ok(None)
    }
}

/// Read the value at `byte_offset`, check the verify fingerprint, and
/// return the value bytes (without the header) on match.
fn read_and_verify(data: &File, byte_offset: u64, expected_vfp: u64) -> Result<Option<Vec<u8>>> {
    let first_block = pread(data, byte_offset, VALUE_ALIGNMENT as u32)?;
    if first_block.len() < VALUE_HEADER_SIZE {
        return Ok(None);
    }

    let stored_vfp = u64::from_le_bytes(first_block[..8].try_into().unwrap());
    if stored_vfp != expected_vfp {
        return Ok(None);
    }

    let byte_len = u32::from_le_bytes(first_block[8..12].try_into().unwrap()) as usize;
    let on_disk_size = VALUE_HEADER_SIZE.saturating_add(byte_len);

    if on_disk_size <= VALUE_ALIGNMENT as usize {
        Ok(Some(
            first_block[VALUE_HEADER_SIZE..VALUE_HEADER_SIZE + byte_len].to_vec(),
        ))
    } else {
        let total = match u32::try_from(on_disk_size) {
            Ok(s) => index::aligned_size(s) as u32,
            Err(_) => return Ok(None),
        };
        let raw = pread(data, byte_offset, total)?;
        Ok(Some(
            raw[VALUE_HEADER_SIZE..VALUE_HEADER_SIZE + byte_len].to_vec(),
        ))
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

    // --- basic roundtrip ---

    #[test]
    fn get_existing_keys() {
        let pairs: &[(&[u8], &[u8])] = &[
            (b"hello", b"world"),
            (b"foo", b"bar"),
            (b"alpha", b"beta gamma delta"),
        ];
        let dir = build_snapshot(pairs, 4);
        let r = SnapshotReader::open(dir.path()).unwrap();

        for &(k, v) in pairs {
            let got = r.get(k).unwrap();
            assert_eq!(got.as_deref(), Some(v), "key={k:?}");
        }
    }

    #[test]
    fn get_missing_key_returns_none() {
        let dir = build_snapshot(&[(b"present", b"yes")], 4);
        let r = SnapshotReader::open(dir.path()).unwrap();
        assert_eq!(r.get(b"absent").unwrap(), None);
    }

    // --- value sizes ---

    #[test]
    fn get_empty_value() {
        let dir = build_snapshot(&[(b"k", b"")], 1);
        let r = SnapshotReader::open(dir.path()).unwrap();
        assert_eq!(r.get(b"k").unwrap().as_deref(), Some(b"".as_slice()));
    }

    #[test]
    fn get_large_value() {
        let big = vec![0xABu8; 1024 * 1024]; // 1 MiB
        let dir = build_snapshot(&[(b"big", &big)], 1);
        let r = SnapshotReader::open(dir.path()).unwrap();
        assert_eq!(r.get(b"big").unwrap(), Some(big));
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
        let r = SnapshotReader::open(dir.path()).unwrap();

        for (k, v) in &vals {
            let got = r.get(k).unwrap();
            assert_eq!(got.as_deref(), Some(v.as_slice()), "key={k:?}");
        }
    }

    // --- n_partitions=1 ---

    #[test]
    fn single_partition_roundtrip() {
        let pairs: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
        let dir = build_snapshot(pairs, 1);
        let r = SnapshotReader::open(dir.path()).unwrap();
        for &(k, v) in pairs {
            assert_eq!(r.get(k).unwrap().as_deref(), Some(v));
        }
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
                let r = SnapshotReader::open(dir.path()).unwrap();

                assert_eq!(
                    r.get(prev.as_bytes()).unwrap().as_deref(),
                    Some(b"value-a".as_slice()),
                    "key_a should be retrievable despite collision"
                );
                assert_eq!(
                    r.get(k.as_bytes()).unwrap().as_deref(),
                    Some(b"value-b".as_slice()),
                    "key_b should be retrievable despite collision"
                );
                return;
            }
            seen.insert(cfp, k);
        }
        panic!("failed to find a 32-bit fingerprint collision in 200K keys");
    }
}
