use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use bytemuck::cast_slice;
use rayon::prelude::*;

use kv_format::{
    index::{Bucket, IndexHeader, RawEntry, bucket_count, insert},
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
/// Streams spill records directly into the bucket table — no intermediate Vec.
fn build_partition(dir: &Path) -> Result<u64, LoaderError> {
    let spill = SpillReader::open(&dir.join("spill.bin"))?;
    let n     = spill.count() as usize;

    let n_buckets = bucket_count(n);
    let mut table = vec![Bucket::default(); n_buckets];

    for record in spill.records() {
        let r     = record?;
        let entry = RawEntry::new(r.fingerprint, r.aligned_offset, r.size)?;
        insert(&mut table, entry);
    }

    let header = IndexHeader { n_buckets: n_buckets as u64, n_keys: n as u64 };
    let mut f  = BufWriter::new(File::create(dir.join("index.idx"))?);
    f.write_all(&header.to_bytes())?;
    f.write_all(cast_slice::<Bucket, u8>(&table))?;
    f.flush()?;

    // Clean up spill file after successful index build.
    std::fs::remove_file(dir.join("spill.bin"))?;

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
        phase.finish(vec![fanout]).await.unwrap();
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
}
