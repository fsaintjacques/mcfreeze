use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use bytemuck::cast_slice;
use log::info;
use rayon::prelude::*;

use kv_format::{
    index::{IndexHeader, RawEntry},
    meta::Layout,
};

use crate::{
    error::LoaderError,
    spill::SpillReader,
};

// ---------------------------------------------------------------------------
// IndexBuildPhase
// ---------------------------------------------------------------------------

pub struct IndexBuildPhase {
    root:        std::path::PathBuf,
    layout:      Layout,
    parallelism: usize,
}

impl IndexBuildPhase {
    pub fn new(root: &Path, layout: Layout, parallelism: usize) -> Self {
        Self {
            root:        root.to_path_buf(),
            layout,
            parallelism: parallelism.max(1),
        }
    }

    /// Build all partition indexes in parallel. Returns total key count.
    pub fn run(self) -> Result<u64, LoaderError> {
        let n     = self.layout.n_partitions as usize;
        let width = format!("{}", self.layout.n_partitions - 1).len();
        let root  = &self.root;

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.parallelism)
            .build()?;

        let results: Vec<Result<u64, LoaderError>> = pool.install(|| {
            (0..n)
                .into_par_iter()
                .map(|i| {
                    let dir = root.join(format!("part-{:0>width$}", i, width = width));
                    build_partition(&dir)
                })
                .collect()
        });

        results.into_iter().try_fold(0u64, |acc, r| Ok(acc + r?))
    }
}

// ---------------------------------------------------------------------------
// Per-partition index build
// ---------------------------------------------------------------------------

/// Load `spill.bin`, build Robin Hood table, write `index.idx`.
///
/// Idempotent: if `index.idx` already exists (and `spill.bin` is absent) the
/// partition was successfully indexed in a previous run — skip it and return
/// the key count from the header.
///
/// To avoid leaving a partial index visible on failure, the file is written to
/// `index.idx.tmp` and renamed to `index.idx` only after a successful flush.
/// `spill.bin` is removed only after the rename succeeds.
fn build_partition(dir: &Path) -> Result<u64, LoaderError> {
    let idx_path   = dir.join("index.idx");
    let spill_path = dir.join("spill.bin");

    // --- Skip if already indexed ---
    if idx_path.exists() && !spill_path.exists() {
        use std::io::Read;
        let mut f   = File::open(&idx_path)?;
        let mut buf = [0u8; kv_format::index::INDEX_HEADER_SIZE];
        f.read_exact(&mut buf)?;
        let hdr = IndexHeader::from_bytes(&buf)?;
        return Ok(hdr.n_keys);
    }

    let spill = SpillReader::open(&spill_path)?;
    let n     = spill.count() as usize;

    let entries: Vec<RawEntry> = spill.records()
        .map(|r| -> Result<RawEntry, LoaderError> {
            let r = r?;
            Ok(RawEntry::new(r.fingerprint, r.aligned_offset, r.size)?)
        })
        .collect::<Result<_, _>>()?;

    let (table, retries) = kv_format::index::build(&entries)?;
    let n_buckets = table.len();

    if retries > 0 {
        info!(
            "{}: PSL overflow — rebuilt index {} time(s), final table {} buckets ({} keys)",
            dir.display(), retries, n_buckets, n,
        );
    }

    // Write to a tmp file, then atomically rename.
    let tmp_path = dir.join("index.idx.tmp");
    {
        let mut f = BufWriter::new(File::create(&tmp_path)?);
        let header = IndexHeader { n_buckets: n_buckets as u64, n_keys: n as u64 };
        f.write_all(&header.to_bytes())?;
        f.write_all(cast_slice::<kv_format::index::Bucket, u8>(&table))?;
        f.flush()?;
    }
    std::fs::rename(&tmp_path, &idx_path)?;

    // Remove spill only after the index is durably in place.
    std::fs::remove_file(&spill_path)?;

    Ok(n as u64)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scatter::ScatterPhase;
    use crate::source::VecBatch;
    use kv_format::index::IndexHeader;
    use tempfile::TempDir;

    fn layout(n: u32) -> Layout { Layout::new(n).unwrap() }

    fn part_dir(root: &std::path::Path, n: u32, i: usize) -> std::path::PathBuf {
        let width = format!("{}", n - 1).len();
        root.join(format!("part-{:0>width$}", i, width = width))
    }

    async fn scatter_and_build(pairs: &[(&[u8], &[u8])], n: u32) -> TempDir {
        let dir   = TempDir::new().unwrap();
        let batch = VecBatch(pairs.iter().map(|&(k, v)| (k.to_vec(), v.to_vec())).collect());
        let phase = ScatterPhase::new(dir.path(), layout(n), 4, 1024 * 1024, 4096).unwrap();
        let mut fanout = phase.fanout();
        fanout.scatter_batch(&batch).await.unwrap();
        phase.finish(vec![fanout], std::time::Instant::now()).await.unwrap();
        IndexBuildPhase::new(dir.path(), layout(n), 2).run().unwrap();
        dir
    }

    #[tokio::test]
    async fn build_produces_index_files() {
        let dir = scatter_and_build(&[(b"k", b"v")], 4).await;
        for i in 0..4 {
            let idx = part_dir(dir.path(), 4, i).join("index.idx");
            assert!(idx.exists(), "{idx:?}");
        }
    }

    #[tokio::test]
    async fn spill_files_removed_after_build() {
        let dir = scatter_and_build(&[(b"k", b"v")], 4).await;
        for i in 0..4 {
            let spill = part_dir(dir.path(), 4, i).join("spill.bin");
            assert!(!spill.exists(), "spill.bin should be removed: {spill:?}");
        }
    }

    #[tokio::test]
    async fn index_key_count() {
        let pairs: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
        let dir = scatter_and_build(pairs, 1).await;

        let hdr_bytes = {
            use std::io::Read;
            let mut f   = File::open(part_dir(dir.path(), 1, 0).join("index.idx")).unwrap();
            let mut buf = [0u8; 64];
            f.read_exact(&mut buf).unwrap();
            buf
        };
        let hdr = IndexHeader::from_bytes(&hdr_bytes).unwrap();
        assert_eq!(hdr.n_keys, 3);
    }

    #[tokio::test]
    async fn empty_partition_builds_ok() {
        let dir = scatter_and_build(&[], 4).await;
        for i in 0..4 {
            let idx = part_dir(dir.path(), 4, i).join("index.idx");
            assert!(idx.exists());
        }
    }

    #[tokio::test]
    async fn index_build_is_idempotent() {
        let pairs: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"b", b"2")];
        let dir = scatter_and_build(pairs, 1).await;

        let idx      = part_dir(dir.path(), 1, 0).join("index.idx");
        let tmp      = part_dir(dir.path(), 1, 0).join("index.idx.tmp");
        let snapshot = std::fs::read(&idx).unwrap();

        // Second run: spill.bin is gone, index.idx exists → skip.
        let n_keys = IndexBuildPhase::new(dir.path(), layout(1), 1).run().unwrap();
        assert_eq!(n_keys, 2, "key count must be preserved on skip");
        assert_eq!(std::fs::read(&idx).unwrap(), snapshot, "index.idx must be unchanged");
        assert!(!tmp.exists(), "skip path must not leave a tmp file");
    }

    #[tokio::test]
    async fn tmp_file_removed_on_success() {
        let dir = scatter_and_build(&[(b"k", b"v")], 1).await;
        let tmp = part_dir(dir.path(), 1, 0).join("index.idx.tmp");
        assert!(!tmp.exists(), "index.idx.tmp should be gone after successful build");
    }
}
