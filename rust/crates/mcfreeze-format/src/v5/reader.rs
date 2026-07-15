// SPDX-License-Identifier: Apache-2.0

//! V5 read path: owned fence arrays + block `pread` + scan.
//!
//! Fences are small enough to be **owned, not borrowed**: `open` copies
//! each partition's `fences.bin` into an anonymous allocation
//! (`MADV_HUGEPAGE`-advised on Linux) and closes the fd immediately.
//! Residency is by construction — the copy faults every page in — so
//! there is no populate step and nothing for the kernel to evict from
//! under the reader; only `blocks.bin`/`heap.bin` fds stay open.

use std::fs::{self, File};
use std::path::Path;

use crate::{
    data::pread,
    index::fingerprint,
    meta::{partition_dir, Layout},
    snapshot::GetOutcome,
    v5::{
        block::{self, checksum32, Record},
        fence,
        meta::Meta,
        verify_fingerprint,
    },
    Error, Result,
};

// ---------------------------------------------------------------------------
// FenceArray — owned, resident fence storage
// ---------------------------------------------------------------------------

/// One partition's fence array in owned anonymous memory.
///
/// `None` for an empty partition (zero blocks): `map_anon(0)` is not a
/// thing, and an empty slice needs no backing.
struct FenceArray {
    map: Option<memmap2::MmapMut>,
}

impl FenceArray {
    /// Read `path` into a fresh anonymous mapping and close the file.
    /// `expected_blocks` comes from `meta.json`; a size mismatch is
    /// corruption (truncated or foreign `fences.bin`), caught at open.
    fn load(path: &Path, expected_blocks: u64, partition: usize) -> Result<Self> {
        let len = fs::metadata(path)?.len();
        if len != expected_blocks * 4 {
            return Err(Error::SnapshotFileSize {
                partition,
                file: "fences.bin",
                got: len,
                expected: expected_blocks * 4,
            });
        }
        if len == 0 {
            return Ok(Self { map: None });
        }

        let mut map = memmap2::MmapMut::map_anon(len as usize)?;
        #[cfg(target_os = "linux")]
        {
            // Best-effort THP hint; the array works (slower TLB) without.
            let _ = map.advise(memmap2::Advice::HugePage);
        }
        use std::io::Read;
        File::open(path)?.read_exact(&mut map)?;
        Ok(Self { map: Some(map) })
    }

    fn as_slice(&self) -> &[u32] {
        match &self.map {
            // Anonymous mappings are page-aligned, comfortably u32-aligned.
            Some(m) => bytemuck::cast_slice(m),
            None => &[],
        }
    }
}

// ---------------------------------------------------------------------------
// SnapshotReader
// ---------------------------------------------------------------------------

struct Partition {
    fences: FenceArray,
    blocks: File,
    heap: File,
}

pub(crate) struct SnapshotReader {
    partitions: Vec<Partition>,
    layout: Layout,
    verify_seed: u64,
    block_size: usize,
}

impl SnapshotReader {
    /// Open a V5 snapshot from already-parsed metadata (see
    /// `SnapshotDesc::load`): validate and load every partition's fence
    /// array into owned memory, open `blocks.bin`/`heap.bin` fds.
    /// Blocking (reads every fence byte) — call from a blocking context.
    pub fn open(root: impl AsRef<Path>, meta: &Meta) -> Result<Self> {
        let root = root.as_ref();
        let layout = meta.layout()?;
        let block_size = meta.block_size as u64;

        let mut partitions = Vec::with_capacity(meta.partitions.len());
        for (i, pm) in meta.partitions.iter().enumerate() {
            let dir = partition_dir(root, layout.n_partitions, i);
            let fences = FenceArray::load(&dir.join("fences.bin"), pm.n_blocks, i)?;

            let blocks = File::open(dir.join("blocks.bin"))?;
            let blocks_len = blocks.metadata()?.len();
            if blocks_len != pm.n_blocks * block_size {
                return Err(Error::SnapshotFileSize {
                    partition: i,
                    file: "blocks.bin",
                    got: blocks_len,
                    expected: pm.n_blocks * block_size,
                });
            }

            partitions.push(Partition {
                fences,
                blocks,
                heap: File::open(dir.join("heap.bin"))?,
            });
        }

        Ok(Self {
            partitions,
            layout,
            verify_seed: meta.verify_seed,
            block_size: meta.block_size as usize,
        })
    }

    /// Look up `key`. `Miss { io: false }` only when the fence search
    /// yields no candidate block (empty partition, or `high32(fp)` below
    /// the first fence); every scanned-but-unmatched block is paid I/O.
    pub fn get(&self, key: &[u8]) -> Result<GetOutcome> {
        let fp = fingerprint(key);
        let vfp = verify_fingerprint(key, self.verify_seed);
        let part = &self.partitions[self.layout.partition_of(fp)];
        let fences = part.fences.as_slice();

        let mut io = false;
        for b in fence::candidate_blocks(fences, fence::fence_of(fp)) {
            let buf = pread(
                &part.blocks,
                b as u64 * self.block_size as u64,
                self.block_size as u32,
            )?;
            io = true;
            match block::find(&buf, vfp)? {
                Some(Record::Inline { value, .. }) => {
                    return Ok(GetOutcome::Hit(value.to_vec()));
                }
                Some(Record::Stub { stub, .. }) => {
                    let value = pread(&part.heap, stub.heap_offset, stub.value_len)?;
                    // Heap bytes are outside any block checksum; the
                    // stub carries their own.
                    if checksum32(&value) != stub.value_checksum {
                        return Err(Error::ValueChecksumMismatch);
                    }
                    return Ok(GetOutcome::Hit(value));
                }
                None => {}
            }
        }
        Ok(GetOutcome::Miss { io })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::os::unix::fs::FileExt;

    use crate::builder::{builder_for, BuilderConfig, V5Options};
    use crate::meta::{partition_dir, Layout, Stats, DEFAULT_VERIFY_SEED};
    use crate::{Error, FormatId, GetOutcome, Snapshot};
    use tempfile::TempDir;

    /// Build a complete V5 snapshot through the production write path.
    fn build(pairs: &[(&[u8], &[u8])], n: u32) -> TempDir {
        let dir = TempDir::new().unwrap();
        let builder = builder_for(
            FormatId::V5,
            BuilderConfig {
                root: dir.path().to_path_buf(),
                n_partitions: n,
                verify_seed: DEFAULT_VERIFY_SEED,
                data_buf_bytes: 64 * 1024,
                spill_buf_bytes: 4096,
                v5: V5Options {
                    block_size: Some(4096),
                    ..Default::default()
                },
            },
        )
        .unwrap();
        let layout = Layout::new(n).unwrap();
        let mut apps: Vec<_> = (0..n as usize)
            .map(|p| builder.appender(p).unwrap())
            .collect();
        for &(k, v) in pairs {
            let fp = crate::index::fingerprint(k);
            apps[layout.partition_of(fp)].append(k, fp, v).unwrap();
        }
        for a in apps {
            a.finish().unwrap();
        }
        builder.plan().unwrap();
        let done = builder.build(2, None).unwrap();
        builder
            .finalize(
                Stats {
                    n_keys: done.n_keys,
                    created_at: "2026-01-01T00:00:00Z".into(),
                    scatter: None,
                    index: None,
                },
                None,
            )
            .unwrap();
        dir
    }

    #[test]
    fn empty_snapshot_misses_for_free() {
        let dir = build(&[], 4);
        let snap = Snapshot::open_path(dir.path()).unwrap();
        assert_eq!(snap.desc().format(), FormatId::V5);
        assert_eq!(
            snap.get(b"anything").unwrap(),
            GetOutcome::Miss { io: false }
        );
    }

    #[test]
    fn corrupt_heap_value_is_error_not_miss() {
        // A 3000-byte value stubs out at block_size 4096 / threshold
        // 2048. Flipping one heap byte leaves every block checksum
        // valid — only the stub's value checksum can catch it.
        let big = vec![0xABu8; 3000];
        let dir = build(&[(b"stubbed", &big)], 1);
        let snap = Snapshot::open_path(dir.path()).unwrap();
        assert_eq!(snap.get(b"stubbed").unwrap(), GetOutcome::Hit(big.clone()));

        let heap_path = partition_dir(dir.path(), 1, 0).join("heap.bin");
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(heap_path)
            .unwrap();
        f.write_all_at(&[!0xABu8], 100).unwrap();

        assert!(matches!(
            snap.get(b"stubbed"),
            Err(Error::ValueChecksumMismatch)
        ));
    }

    #[test]
    fn truncated_fences_fail_at_open() {
        let dir = build(&[(b"k", b"v")], 1);
        let fences = partition_dir(dir.path(), 1, 0).join("fences.bin");
        let len = std::fs::metadata(&fences).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&fences)
            .unwrap()
            .set_len(len - 4)
            .unwrap();
        assert!(matches!(
            Snapshot::open_path(dir.path()),
            Err(Error::SnapshotFileSize {
                file: "fences.bin",
                ..
            })
        ));
    }

    #[test]
    fn truncated_blocks_fail_at_open() {
        let dir = build(&[(b"k", b"v")], 1);
        let blocks = partition_dir(dir.path(), 1, 0).join("blocks.bin");
        let len = std::fs::metadata(&blocks).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&blocks)
            .unwrap()
            .set_len(len - 1)
            .unwrap();
        assert!(matches!(
            Snapshot::open_path(dir.path()),
            Err(Error::SnapshotFileSize {
                file: "blocks.bin",
                ..
            })
        ));
    }

    #[test]
    fn expected_miss_io_rate_is_one_without_sketch() {
        let dir = build(&[(b"k", b"v")], 1);
        let snap = Snapshot::open_path(dir.path()).unwrap();
        assert_eq!(snap.expected_miss_io_rate(), 1.0);
    }
}
