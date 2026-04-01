use std::fs::{self, File};
use std::path::Path;

use memmap2::MmapOptions;

use crate::{
    data::pread,
    index::{self, Bucket, IndexHeader, fingerprint, verify_fingerprint, INDEX_HEADER_SIZE},
    meta::{Layout, Meta, VALUE_HEADER_SIZE},
    Result,
};

// ---------------------------------------------------------------------------
// PartitionReader
// ---------------------------------------------------------------------------

/// Read-only view of one partition.
///
/// `index.idx` is memory-mapped (past the header) with `MADV_RANDOM`; the OS
/// page cache provides hot-key residency without explicit caching logic.
/// `data.bin` is opened for on-demand `pread` calls — never mmap'd.
struct PartitionReader {
    /// Read-only mmap of the bucket array (bytes after the 64-byte header).
    buckets:   memmap2::Mmap,
    /// Open file descriptor for `pread` calls into `data.bin`.
    data:      File,
}

impl PartitionReader {
    fn open(dir: &Path) -> Result<Self> {
        // --- index.idx ---
        let idx_file = File::open(dir.join("index.idx"))?;
        let idx_len  = idx_file.metadata()?.len() as usize;

        // Map the entire file; we'll slice off the header below.
        // SAFETY: the file is read-only and we hold it open for the mmap lifetime.
        let full_map = unsafe { MmapOptions::new().map(&idx_file)? };

        // Parse the header from the first 64 bytes.
        let hdr_bytes: &[u8; INDEX_HEADER_SIZE] = full_map[..INDEX_HEADER_SIZE]
            .try_into()
            .unwrap();
        let header = IndexHeader::from_bytes(hdr_bytes)?;

        // Map only the bucket region (skip the header).
        let buckets = unsafe {
            MmapOptions::new()
                .offset(INDEX_HEADER_SIZE as u64)
                .len(idx_len - INDEX_HEADER_SIZE)
                .map(&idx_file)?
        };

        // Advise random access — readahead would waste I/O and pollute the cache.
        #[cfg(unix)]
        buckets.advise(memmap2::Advice::Random)?;

        // --- data.bin ---
        let data = File::open(dir.join("data.bin"))?;

        let _ = header; // n_buckets is derived from the mmap length at probe time
        Ok(Self { buckets, data })
    }

    /// Look up `fp` in the bucket array and read the value from `data.bin`.
    /// Checks the 12-byte value header (verify fingerprint + byte length)
    /// and strips it before returning. A verification mismatch is treated
    /// as a miss (index fingerprint collision).
    fn get(&self, key: &[u8], fp: u64, verify_seed: u64) -> Result<Option<Vec<u8>>> {
        let table: &[Bucket] = bytemuck::cast_slice(&self.buckets);
        match index::probe(table, fp) {
            None              => Ok(None),
            Some((off, size)) => {
                let raw = pread(&self.data, off, size)?;
                if raw.len() < VALUE_HEADER_SIZE {
                    return Ok(None); // corrupt entry
                }
                let stored_vfp = u64::from_le_bytes(raw[..8].try_into().unwrap());
                let expected_vfp = verify_fingerprint(key, verify_seed);
                if stored_vfp != expected_vfp {
                    return Ok(None); // fingerprint collision — treat as miss
                }
                let byte_len = u32::from_le_bytes(raw[8..12].try_into().unwrap()) as usize;
                let end = VALUE_HEADER_SIZE + byte_len;
                if end > raw.len() {
                    return Ok(None); // corrupt entry
                }
                Ok(Some(raw[VALUE_HEADER_SIZE..end].to_vec()))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SnapshotReader
// ---------------------------------------------------------------------------

/// Read-only handle to a complete snapshot directory.
///
/// ```text
/// let r = SnapshotReader::open("/snapshots/v42")?;
/// if let Some(val) = r.get(b"my-key")? {
///     // use val
/// }
/// ```
pub struct SnapshotReader {
    layout:      Layout,
    verify_seed: u64,
    partitions:  Vec<PartitionReader>,
}

impl SnapshotReader {
    /// Open a snapshot directory.  Reads `meta.json`, validates the format,
    /// then opens every partition.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();

        let json   = fs::read_to_string(root.join("meta.json"))?;
        let meta: Meta = serde_json::from_str(&json)?;
        let layout = meta.layout()?;

        let partitions = (0..layout.n_partitions as usize)
            .map(|i| {
                let dir = crate::meta::partition_dir(root, layout.n_partitions, i);
                PartitionReader::open(&dir)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { layout, verify_seed: meta.verify_seed, partitions })
    }

    /// Look up `key` and return its value, or `None` if not present.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let fp  = fingerprint(key);
        let idx = self.layout.partition_of(fp);
        self.partitions[idx].get(key, fp, self.verify_seed)
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
        w.finish(dir.path()).unwrap();
        dir
    }

    // --- basic roundtrip ---

    #[test]
    fn get_existing_keys() {
        let pairs: &[(&[u8], &[u8])] = &[
            (b"hello", b"world"),
            (b"foo",   b"bar"),
            (b"alpha", b"beta gamma delta"),
        ];
        let dir = build_snapshot(pairs, 4);
        let r   = SnapshotReader::open(dir.path()).unwrap();

        for &(k, v) in pairs {
            let got = r.get(k).unwrap();
            assert_eq!(got.as_deref(), Some(v), "key={k:?}");
        }
    }

    #[test]
    fn get_missing_key_returns_none() {
        let dir = build_snapshot(&[(b"present", b"yes")], 4);
        let r   = SnapshotReader::open(dir.path()).unwrap();
        assert_eq!(r.get(b"absent").unwrap(), None);
    }

    // --- value sizes ---

    #[test]
    fn get_empty_value() {
        let dir = build_snapshot(&[(b"k", b"")], 1);
        let r   = SnapshotReader::open(dir.path()).unwrap();
        assert_eq!(r.get(b"k").unwrap().as_deref(), Some(b"".as_slice()));
    }

    #[test]
    fn get_large_value() {
        let big = vec![0xABu8; 1024 * 1024]; // 1 MiB
        let dir = build_snapshot(&[(b"big", &big)], 1);
        let r   = SnapshotReader::open(dir.path()).unwrap();
        assert_eq!(r.get(b"big").unwrap(), Some(big));
    }

    // --- many keys ---

    #[test]
    fn roundtrip_many_keys() {
        let n    = 10_000usize;
        let vals: Vec<(Vec<u8>, Vec<u8>)> = (0..n)
            .map(|i| (format!("key-{i}").into_bytes(), format!("value-{i}").into_bytes()))
            .collect();
        let pairs: Vec<(&[u8], &[u8])> = vals.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();

        let dir = build_snapshot(&pairs, 64);
        let r   = SnapshotReader::open(dir.path()).unwrap();

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
        let r   = SnapshotReader::open(dir.path()).unwrap();
        for &(k, v) in pairs {
            assert_eq!(r.get(k).unwrap().as_deref(), Some(v));
        }
    }
}
