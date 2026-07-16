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
        compress, fence,
        meta::Meta,
        sketch::{Sketch, FALSE_POSITIVE_RATE},
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
    /// `expected_blocks` comes from `meta.json` (already bounded by
    /// `Meta::layout`, so the `× 4` cannot overflow); a size mismatch
    /// is corruption (truncated or foreign `fences.bin`), caught at
    /// open. One open, sized from the handle: the stat and the read
    /// see the same file.
    fn load(path: &Path, expected_blocks: u64, partition: usize) -> Result<Self> {
        let mut file = File::open(path)?;
        let len = file.metadata()?.len();
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
        file.read_exact(&mut map)?;
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
    /// Present iff `meta.sketch` is set and the partition is non-empty.
    sketch: Option<Sketch>,
}

pub(crate) struct SnapshotReader {
    partitions: Vec<Partition>,
    layout: Layout,
    verify_seed: u64,
    block_size: usize,
    has_sketch: bool,
    /// Present iff `meta.compression` declares a codec. Holds the
    /// verified dictionary and the pooled decompression contexts.
    codec: Option<compress::Decompressor>,
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

        // Codec first: dict.bin is read into owned memory and verified
        // against meta.compression.dict_checksum. Value checksums cover
        // stored bytes, so a corrupt dictionary decompresses cleanly
        // into wrong values — the one failure mode they cannot catch
        // (exactly like the sketch's false negatives) — and fails the
        // open instead. `layout()` already validated codec coherence.
        let codec = match &meta.compression {
            Some(c) => {
                // Keyed on the semantic flag; `layout()` guarantees the
                // checksum accompanies it (and never appears without it).
                let dict = if c.dict {
                    let expected = c.dict_checksum.ok_or(Error::InvalidCompressionMeta(
                        "dict: true requires dict_checksum",
                    ))?;
                    let bytes = fs::read(root.join("dict.bin")).map_err(Error::DictUnreadable)?;
                    compress::verify_dict(&bytes, expected)?;
                    Some(bytes)
                } else {
                    None
                };
                Some(compress::Decompressor::new(dict)?)
            }
            None => None,
        };

        let mut partitions = Vec::with_capacity(meta.partitions.len());
        for (i, pm) in meta.partitions.iter().enumerate() {
            let dir = partition_dir(root, layout.n_partitions, i);
            let fences = FenceArray::load(&dir.join("fences.bin"), pm.n_blocks, i)?;

            let blocks = File::open(dir.join("blocks.bin"))?;
            let blocks_len = blocks.metadata()?.len();
            // No overflow: Meta::layout bounds n_blocks × block_size.
            if blocks_len != pm.n_blocks * block_size {
                return Err(Error::SnapshotFileSize {
                    partition: i,
                    file: "blocks.bin",
                    got: blocks_len,
                    expected: pm.n_blocks * block_size,
                });
            }

            // Sketch corruption is a false-negative machine (a present
            // key reported absent), so a sketch that exists but fails
            // verification fails the open — never a degraded filter.
            let sketch = if meta.sketch.is_some() && pm.n_blocks > 0 {
                let bytes =
                    fs::read(dir.join("sketch.bin")).map_err(|source| Error::SnapshotFileRead {
                        partition: i,
                        file: "sketch.bin",
                        source,
                    })?;
                Some(Sketch::parse(bytes)?)
            } else {
                None
            };

            partitions.push(Partition {
                fences,
                blocks,
                heap: File::open(dir.join("heap.bin"))?,
                sketch,
            });
        }

        Ok(Self {
            partitions,
            layout,
            verify_seed: meta.verify_seed,
            block_size: meta.block_size as usize,
            has_sketch: meta.sketch.is_some(),
            codec,
        })
    }

    /// Expected fraction of misses that pay I/O: the sketch's
    /// false-positive rate when enabled, ≈1 otherwise.
    pub fn expected_miss_io_rate(&self) -> f64 {
        if self.has_sketch {
            FALSE_POSITIVE_RATE
        } else {
            1.0
        }
    }

    /// Look up `key`. `Miss { io: false }` only when the fence search
    /// yields no candidate block (empty partition, or `high32(fp)` below
    /// the first fence); every scanned-but-unmatched block is paid I/O.
    pub fn get(&self, key: &[u8]) -> Result<GetOutcome> {
        let fp = fingerprint(key);
        let vfp = verify_fingerprint(key, self.verify_seed);
        let part = &self.partitions[self.layout.partition_of(fp)];

        // Sketch rejection: absence concluded in RAM, zero preads.
        if let Some(sketch) = &part.sketch {
            if !sketch.contains(fp) {
                return Ok(GetOutcome::Miss { io: false });
            }
        }
        let fences = part.fences.as_slice();

        let mut io = false;
        for b in fence::candidate_blocks(fences, fence::fence_of(fp)) {
            let buf = pread(
                &part.blocks,
                b as u64 * self.block_size as u64,
                self.block_size as u32,
            )?;
            io = true;
            // Decompression happens only after a vfp match, through the
            // codec loaded at open; a bit-30 record in a codec-less
            // snapshot is corruption. Both are `decode`'s contract.
            match block::find(&buf, vfp)? {
                Some(Record::Inline {
                    value, compressed, ..
                }) => {
                    let value = compress::decode(value, compressed, self.codec.as_ref())?;
                    return Ok(GetOutcome::Hit(value.into_owned()));
                }
                Some(Record::Stub {
                    stub, compressed, ..
                }) => {
                    let value = pread(&part.heap, stub.heap_offset, stub.stored_len)?;
                    // Checksums cover stored bytes, before any
                    // decompression: heap bytes are outside any block
                    // checksum, so the stub carries their own.
                    if checksum32(&value) != stub.value_checksum {
                        return Err(Error::ValueChecksumMismatch);
                    }
                    let value = compress::decode(&value, compressed, self.codec.as_ref())?;
                    return Ok(GetOutcome::Hit(value.into_owned()));
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
        build_opts(pairs, n, false)
    }

    fn build_opts(pairs: &[(&[u8], &[u8])], n: u32, sketch: bool) -> TempDir {
        build_v5(
            pairs,
            n,
            V5Options {
                block_size: Some(4096),
                sketch,
                ..Default::default()
            },
        )
    }

    fn build_v5(pairs: &[(&[u8], &[u8])], n: u32, v5: V5Options) -> TempDir {
        let dir = TempDir::new().unwrap();
        let builder = builder_for(
            FormatId::V5,
            BuilderConfig {
                root: dir.path().to_path_buf(),
                n_partitions: n,
                verify_seed: DEFAULT_VERIFY_SEED,
                data_buf_bytes: 64 * 1024,
                spill_buf_bytes: 4096,
                v5,
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

    // -----------------------------------------------------------------
    // Compression read path (doc/plan/V5_COMPRESSION.md stage 4)
    // -----------------------------------------------------------------

    fn compressible_pairs(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..n)
            .map(|i| {
                (
                    format!("key-{i}").into_bytes(),
                    format!("user-{i}|city-{}|{}", i % 50, "payload ".repeat(12)).into_bytes(),
                )
            })
            .collect()
    }

    fn build_dict_snapshot(n_keys: usize) -> (TempDir, Vec<(Vec<u8>, Vec<u8>)>) {
        let vals = compressible_pairs(n_keys);
        let pairs: Vec<(&[u8], &[u8])> = vals.iter().map(|(k, v)| (&k[..], &v[..])).collect();
        let dir = build_v5(
            &pairs,
            2,
            V5Options {
                block_size: Some(4096),
                compression: crate::v5::compress::Mode::ZstdDict,
                ..Default::default()
            },
        );
        (dir, vals)
    }

    #[test]
    fn compressed_snapshot_roundtrips_through_facade() {
        let (dir, vals) = build_dict_snapshot(2000);

        // meta.json declares the codec and anchors the dictionary.
        let meta: crate::v5::meta::Meta =
            serde_json::from_str(&std::fs::read_to_string(dir.path().join("meta.json")).unwrap())
                .unwrap();
        let cm = meta.compression.expect("compression section");
        assert_eq!(cm.codec, "zstd");
        assert!(cm.dict, "2000 samples must train, not fall back");
        let dict = std::fs::read(dir.path().join("dict.bin")).unwrap();
        assert_eq!(cm.dict_checksum, Some(crate::v5::block::checksum32(&dict)));

        let snap = Snapshot::open_path(dir.path()).unwrap();
        for (k, v) in &vals {
            assert_eq!(snap.get(k).unwrap(), GetOutcome::Hit(v.clone()), "{k:?}");
        }
        assert!(matches!(
            snap.get(b"definitely-absent").unwrap(),
            GetOutcome::Miss { .. }
        ));
    }

    #[test]
    fn compressed_stub_roundtrips_through_facade() {
        // A value whose *stored* frame still exceeds the inline
        // threshold: raw 6000 B of doubled noise compresses ~2× to
        // ~3000 B > 2048, exercising pread → stored-byte checksum →
        // decompress on the heap path.
        let mut half = vec![0u8; 3000];
        let mut h = 7u64;
        for b in half.iter_mut() {
            h = h.wrapping_mul(6364136223846793005).wrapping_add(1);
            *b = (h >> 33) as u8;
        }
        let big: Vec<u8> = [half.clone(), half].concat();
        let dir = build_v5(
            &[(b"stubbed", &big)],
            1,
            V5Options {
                block_size: Some(4096),
                compression: crate::v5::compress::Mode::Zstd,
                ..Default::default()
            },
        );

        let done: crate::v5::builder::IndexDone =
            serde_json::from_str(&std::fs::read_to_string(dir.path().join("index.done")).unwrap())
                .unwrap();
        assert_eq!(done.n_compressed, 1);
        assert_eq!(done.n_stubs, 1, "stored size must still stub out");

        let snap = Snapshot::open_path(dir.path()).unwrap();
        assert_eq!(snap.get(b"stubbed").unwrap(), GetOutcome::Hit(big));
    }

    #[test]
    fn corrupt_dict_fails_open() {
        let (dir, _) = build_dict_snapshot(2000);
        let path = dir.path().join("dict.bin");
        let mut dict = std::fs::read(&path).unwrap();
        let mid = dict.len() / 2;
        dict[mid] ^= 0xFF;
        std::fs::write(&path, &dict).unwrap();
        assert!(matches!(
            Snapshot::open_path(dir.path()),
            Err(Error::DictChecksumMismatch { .. })
        ));
    }

    #[test]
    fn truncated_dict_fails_open() {
        let (dir, _) = build_dict_snapshot(2000);
        let path = dir.path().join("dict.bin");
        let len = std::fs::metadata(&path).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(len / 2)
            .unwrap();
        assert!(matches!(
            Snapshot::open_path(dir.path()),
            Err(Error::DictChecksumMismatch { .. })
        ));
    }

    #[test]
    fn missing_dict_fails_open() {
        let (dir, _) = build_dict_snapshot(2000);
        std::fs::remove_file(dir.path().join("dict.bin")).unwrap();
        assert!(matches!(
            Snapshot::open_path(dir.path()),
            Err(Error::DictUnreadable(_))
        ));
    }

    #[test]
    fn unknown_codec_fails_at_load() {
        let (dir, _) = build_dict_snapshot(100);
        let meta_path = dir.path().join("meta.json");
        let json = std::fs::read_to_string(&meta_path)
            .unwrap()
            .replace(r#""codec": "zstd""#, r#""codec": "lz5""#);
        assert!(json.contains("lz5"), "meta.json shape changed");
        std::fs::write(&meta_path, json).unwrap();
        assert!(matches!(
            Snapshot::open_path(dir.path()),
            Err(Error::UnsupportedCodec(c)) if c == "lz5"
        ));
    }

    #[test]
    fn bit30_without_meta_compression_is_corruption() {
        // A stray or future-foreign compressed record in a snapshot
        // whose meta declares no codec must fail loudly, never decode
        // as the stored bytes. Set bit 30 on the one record's
        // length|flags field and re-seal the block so only this rule —
        // not the block checksum — can reject it.
        let dir = build(&[(b"k", b"v")], 1);
        let path = partition_dir(dir.path(), 1, 0).join("blocks.bin");
        let mut block = std::fs::read(&path).unwrap();
        block[11] |= 0x40; // bit 30 of the u32 at offset 8, LE
        let body = block.len() - 4;
        let cksum = crate::v5::block::checksum32(&block[..body]);
        block[body..].copy_from_slice(&cksum.to_le_bytes());
        std::fs::write(&path, &block).unwrap();

        let snap = Snapshot::open_path(dir.path()).unwrap();
        assert!(matches!(
            snap.get(b"k"),
            Err(Error::CompressedValueWithoutCodec)
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
    fn paid_miss_reports_io_true() {
        // Guards the exported expected_miss_io_rate contract: sketchless
        // V5 misses that reach a block must say so. With 100 present
        // keys, only a fingerprint below the partition's first fence
        // misses for free (~1% of random keys), so an absent key paying
        // I/O is found within a handful of tries.
        let vals: Vec<(Vec<u8>, Vec<u8>)> = (0..100)
            .map(|i| {
                (
                    format!("key-{i}").into_bytes(),
                    format!("val-{i}").into_bytes(),
                )
            })
            .collect();
        let pairs: Vec<(&[u8], &[u8])> = vals.iter().map(|(k, v)| (&k[..], &v[..])).collect();
        let dir = build(&pairs, 1);
        let snap = Snapshot::open_path(dir.path()).unwrap();

        let paid = (0..1000).any(
            |i| match snap.get(format!("absent-{i}").as_bytes()).unwrap() {
                GetOutcome::Miss { io } => io,
                GetOutcome::Hit(_) => panic!("absent key hit"),
            },
        );
        assert!(paid, "no absent key produced a paid miss in 1000 tries");
    }

    #[test]
    fn expected_miss_io_rate_is_one_without_sketch() {
        let dir = build(&[(b"k", b"v")], 1);
        let snap = Snapshot::open_path(dir.path()).unwrap();
        assert_eq!(snap.expected_miss_io_rate(), 1.0);
    }

    #[test]
    fn sketch_restores_free_misses_without_false_negatives() {
        let vals: Vec<(Vec<u8>, Vec<u8>)> = (0..2000)
            .map(|i| {
                (
                    format!("key-{i}").into_bytes(),
                    format!("val-{i}").into_bytes(),
                )
            })
            .collect();
        let pairs: Vec<(&[u8], &[u8])> = vals.iter().map(|(k, v)| (&k[..], &v[..])).collect();
        let dir = build_opts(&pairs, 2, true);
        let snap = Snapshot::open_path(dir.path()).unwrap();
        assert_eq!(
            snap.expected_miss_io_rate(),
            crate::v5::sketch::FALSE_POSITIVE_RATE
        );

        // No false negatives: every present key still hits.
        for (k, v) in &vals {
            match snap.get(k).unwrap() {
                GetOutcome::Hit(got) => assert_eq!(&got, v),
                other => panic!("expected hit for {k:?}, got {other:?}"),
            }
        }

        // Misses are overwhelmingly free again: over 1000 absent keys,
        // expect ~4 sketch false positives (ε ≈ 0.39%).
        let paid = (0..1000)
            .filter(
                |i| match snap.get(format!("absent-{i}").as_bytes()).unwrap() {
                    GetOutcome::Miss { io } => io,
                    GetOutcome::Hit(_) => panic!("absent key hit"),
                },
            )
            .count();
        assert!(
            paid < 30,
            "sketch not rejecting misses: {paid}/1000 paid I/O"
        );
    }

    #[test]
    fn sketch_written_and_recorded_in_meta() {
        let dir = build_opts(&[(b"k", b"v")], 1, true);
        assert!(partition_dir(dir.path(), 1, 0).join("sketch.bin").exists());
        let meta: crate::v5::meta::Meta =
            serde_json::from_str(&std::fs::read_to_string(dir.path().join("meta.json")).unwrap())
                .unwrap();
        assert_eq!(meta.sketch.unwrap().kind, "binary_fuse8");
    }

    #[test]
    fn missing_sketch_fails_open_with_partition_context() {
        let dir = build_opts(&[(b"k", b"v")], 1, true);
        std::fs::remove_file(partition_dir(dir.path(), 1, 0).join("sketch.bin")).unwrap();
        assert!(matches!(
            Snapshot::open_path(dir.path()),
            Err(Error::SnapshotFileRead {
                partition: 0,
                file: "sketch.bin",
                ..
            })
        ));
    }

    #[test]
    fn corrupt_sketch_fails_open() {
        let dir = build_opts(&[(b"k", b"v")], 1, true);
        let path = partition_dir(dir.path(), 1, 0).join("sketch.bin");
        let mut bytes = std::fs::read(&path).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        std::fs::write(&path, bytes).unwrap();
        assert!(matches!(
            Snapshot::open_path(dir.path()),
            Err(Error::CorruptSketch(_))
        ));
    }

    #[test]
    fn sketch_with_empty_partition_opens_and_misses_free() {
        // 2 partitions, 1 key: one partition is empty and has no
        // sketch.bin; open must not demand one.
        let dir = build_opts(&[(b"only", b"v")], 2, true);
        let snap = Snapshot::open_path(dir.path()).unwrap();
        match snap.get(b"only").unwrap() {
            GetOutcome::Hit(v) => assert_eq!(v, b"v"),
            other => panic!("expected hit, got {other:?}"),
        }
    }
}
