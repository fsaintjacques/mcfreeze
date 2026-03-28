use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use kv_format::{
    data::AlignedWriter,
    index::fingerprint,
    meta::Layout,
};

use crate::{
    error::LoaderError,
    source::KvBatch,
    spill::{SpillRecord, SpillWriter},
};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A sub-batch routed to one partition: `(fingerprint, owned_value)`.
type PartitionBatch = Vec<(u64, Vec<u8>)>;

pub struct ScatterStats {
    pub n_keys:     u64,
    pub data_bytes: u64,
}

// ---------------------------------------------------------------------------
// Fanout
// ---------------------------------------------------------------------------

/// Lightweight fan-out handle: hashes keys, buckets values by partition, and
/// sends sub-batches to the per-partition writer channels.
///
/// `Fanout` is intentionally cheap to create — it clones the channel senders
/// (which are `Arc`-backed) and allocates one empty `Vec` per partition for
/// the reusable sub-batch buffer.  Multiple `Fanout` instances can run
/// concurrently on different async tasks with zero contention; each has its
/// own sub-batch buffer and the underlying `mpsc::Sender` allows multiple
/// producers.
pub struct Fanout {
    layout:      Layout,
    senders:     Vec<mpsc::Sender<PartitionBatch>>,
    /// Reusable per-partition accumulation buffer.  `mem::take` moves the
    /// populated Vec into the channel and leaves an empty one in its place,
    /// so the grown capacity is reused on the next batch.
    sub_batches: Vec<PartitionBatch>,
    pub n_keys:      u64,
    pub data_bytes:  u64,
}

impl Fanout {
    /// Bucket one source batch by partition and send each non-empty sub-batch.
    ///
    /// Blocks (async-awaits) only if the target partition's channel is full,
    /// providing natural backpressure from slow writers to the source.
    pub async fn scatter_batch(&mut self, batch: &impl KvBatch) -> Result<(), LoaderError> {
        for (key, value) in batch.iter() {
            let fp  = fingerprint(key);
            let idx = self.layout.partition_of(fp);
            self.sub_batches[idx].push((fp, value.to_vec()));
            self.n_keys    += 1;
            self.data_bytes += value.len() as u64;
        }

        for (idx, slot) in self.sub_batches.iter_mut().enumerate() {
            if slot.is_empty() {
                continue;
            }
            let sub_batch = std::mem::take(slot);
            self.senders[idx].send(sub_batch).await.map_err(|_| {
                LoaderError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "partition writer task exited unexpectedly",
                ))
            })?;
        }

        Ok(())
    }

    /// Current `(n_keys, data_bytes)` counters for progress reporting.
    pub fn counters(&self) -> (u64, u64) {
        (self.n_keys, self.data_bytes)
    }
}

// ---------------------------------------------------------------------------
// ScatterPhase
// ---------------------------------------------------------------------------

/// Owns the per-partition `spawn_blocking` writer tasks and the channel
/// senders used to reach them.
///
/// Call [`fanout`] to obtain a [`Fanout`] for each concurrent producer.
/// When all producers are done, call [`finish`] with the completed fanouts
/// to aggregate stats, drain the channels, and join the writers.
pub struct ScatterPhase {
    layout:  Layout,
    senders: Vec<mpsc::Sender<PartitionBatch>>,
    handles: Vec<JoinHandle<Result<(), LoaderError>>>,
}

impl ScatterPhase {
    pub fn new(
        root:             &Path,
        layout:           Layout,
        channel_capacity: usize,
        data_buf_bytes:   usize,
        spill_buf_bytes:  usize,
    ) -> Result<Self, LoaderError> {
        let n     = layout.n_partitions as usize;
        let width = format!("{}", layout.n_partitions - 1).len();

        let mut senders  = Vec::with_capacity(n);
        let mut handles  = Vec::with_capacity(n);

        for i in 0..n {
            let dir = root.join(format!("part-{:0>width$}", i, width = width));
            std::fs::create_dir_all(&dir)?;

            let (tx, rx) = mpsc::channel::<PartitionBatch>(channel_capacity.max(1));
            let handle   = tokio::task::spawn_blocking(move || {
                partition_writer(dir, rx, data_buf_bytes, spill_buf_bytes)
            });

            senders.push(tx);
            handles.push(handle);
        }

        Ok(Self { layout, senders, handles })
    }

    /// Create a [`Fanout`] for one concurrent producer.
    ///
    /// Cloning `mpsc::Sender` is cheap (arc bump).  Each fanout gets its own
    /// sub-batch buffer, so multiple fanouts never contend with each other.
    pub fn fanout(&self) -> Fanout {
        let n = self.layout.n_partitions as usize;
        Fanout {
            layout:      self.layout,
            senders:     self.senders.clone(),
            sub_batches: (0..n).map(|_| Vec::new()).collect(),
            n_keys:      0,
            data_bytes:  0,
        }
    }

    /// Aggregate stats from completed fanouts, close all channels, and join
    /// the writer tasks.
    ///
    /// `fanouts` must include every fanout created by this phase; dropping
    /// them here (together with the phase's own sender copies) closes the
    /// channels so writers can flush and exit.
    pub async fn finish(self, fanouts: Vec<Fanout>) -> Result<ScatterStats, LoaderError> {
        let n_keys     = fanouts.iter().map(|f| f.n_keys).sum();
        let data_bytes = fanouts.iter().map(|f| f.data_bytes).sum();

        // Close all sender ends so each writer's blocking_recv() returns None.
        drop(fanouts);
        drop(self.senders);

        for handle in self.handles {
            handle.await??;
        }

        Ok(ScatterStats { n_keys, data_bytes })
    }
}

// ---------------------------------------------------------------------------
// Partition writer (runs in spawn_blocking)
// ---------------------------------------------------------------------------

fn partition_writer(
    dir:             PathBuf,
    mut rx:          mpsc::Receiver<PartitionBatch>,
    data_buf_bytes:  usize,
    spill_buf_bytes: usize,
) -> Result<(), LoaderError> {
    let mut data  = AlignedWriter::new(BufWriter::with_capacity(
        data_buf_bytes,
        File::create(dir.join("data.bin"))?,
    ));
    let mut spill = SpillWriter::create(&dir.join("spill.bin"), spill_buf_bytes)?;

    while let Some(batch) = rx.blocking_recv() {
        for (fp, value) in batch {
            let aligned_offset = data.write_value(&value)?;
            spill.push(SpillRecord {
                fingerprint:    fp,
                aligned_offset,
                size:           value.len() as u32,
                _pad:           0,
            })?;
        }
    }

    data.finish()?;
    spill.finish()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::VecBatch;
    use crate::spill::SpillReader;
    use kv_format::{data::pread, meta::VALUE_ALIGNMENT};
    use tempfile::TempDir;

    fn layout(n: u32) -> Layout { Layout::new(n).unwrap() }

    async fn scatter(pairs: &[(&[u8], &[u8])], n: u32) -> (TempDir, ScatterStats) {
        let dir   = TempDir::new().unwrap();
        let phase = ScatterPhase::new(dir.path(), layout(n), 4, 1024 * 1024, 4096).unwrap();
        let batch = VecBatch(pairs.iter().map(|&(k, v)| (k.to_vec(), v.to_vec())).collect());
        let mut fanout = phase.fanout();
        fanout.scatter_batch(&batch).await.unwrap();
        let stats = phase.finish(vec![fanout]).await.unwrap();
        (dir, stats)
    }

    #[tokio::test]
    async fn counts_sum_to_total() {
        let pairs: &[(&[u8], &[u8])] = &[
            (b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4"),
        ];
        let (_, stats) = scatter(pairs, 4).await;
        assert_eq!(stats.n_keys, pairs.len() as u64);
    }

    #[tokio::test]
    async fn spill_entries_match_values() {
        let pairs: &[(&[u8], &[u8])] = &[(b"hello", b"world"), (b"foo", b"bar")];
        let (dir, _) = scatter(pairs, 1).await;

        let spill_path = dir.path().join("part-0").join("spill.bin");
        let reader     = SpillReader::open(&spill_path).unwrap();
        assert_eq!(reader.count(), 2);

        let data_file = File::open(dir.path().join("part-0").join("data.bin")).unwrap();
        for rec in reader.records() {
            let rec = rec.unwrap();
            let val = pread(&data_file, rec.aligned_offset * VALUE_ALIGNMENT, rec.size).unwrap();
            assert!(val == b"world" || val == b"bar", "unexpected value: {val:?}");
        }
    }

    #[tokio::test]
    async fn data_bin_alignment() {
        let (dir, _) = scatter(&[(b"k", b"v")], 1).await;
        let meta = std::fs::metadata(dir.path().join("part-0").join("data.bin")).unwrap();
        assert_eq!(meta.len() % VALUE_ALIGNMENT, 0);
        assert_eq!(meta.len(), 64);
    }

    #[tokio::test]
    async fn scatter_stats() {
        let pairs: &[(&[u8], &[u8])] = &[(b"k1", b"hello"), (b"k2", b"world")];
        let (_, stats) = scatter(pairs, 1).await;
        assert_eq!(stats.n_keys,     2);
        assert_eq!(stats.data_bytes, 10);
    }

    #[tokio::test]
    async fn empty_scatter() {
        let (_, stats) = scatter(&[], 4).await;
        assert_eq!(stats.n_keys, 0);
    }

    #[tokio::test]
    async fn multiple_batches_accumulate() {
        let dir   = TempDir::new().unwrap();
        let phase = ScatterPhase::new(dir.path(), layout(1), 4, 1024 * 1024, 4096).unwrap();
        let mut fanout = phase.fanout();

        for i in 0u64..10 {
            let key = i.to_le_bytes().to_vec();
            let val = i.to_le_bytes().to_vec();
            fanout.scatter_batch(&VecBatch(vec![(key, val)])).await.unwrap();
        }

        let stats = phase.finish(vec![fanout]).await.unwrap();
        assert_eq!(stats.n_keys, 10);

        let spill = SpillReader::open(&dir.path().join("part-0").join("spill.bin")).unwrap();
        assert_eq!(spill.count(), 10);
    }

    #[tokio::test]
    async fn parallel_fanouts_no_data_loss() {
        // Two concurrent fanouts writing to the same partition writers.
        let dir    = TempDir::new().unwrap();
        let phase  = ScatterPhase::new(dir.path(), layout(1), 8, 1024 * 1024, 4096).unwrap();
        let mut f0 = phase.fanout();
        let mut f1 = phase.fanout();

        let batch0 = VecBatch((0u64..500)
            .map(|i| (i.to_le_bytes().to_vec(), b"v0".to_vec())).collect());
        let batch1 = VecBatch((500u64..1000)
            .map(|i| (i.to_le_bytes().to_vec(), b"v1".to_vec())).collect());

        // Drive both fanouts concurrently.
        tokio::try_join!(
            f0.scatter_batch(&batch0),
            f1.scatter_batch(&batch1),
        ).unwrap();

        let stats = phase.finish(vec![f0, f1]).await.unwrap();
        assert_eq!(stats.n_keys, 1000);

        let spill = SpillReader::open(&dir.path().join("part-0").join("spill.bin")).unwrap();
        assert_eq!(spill.count(), 1000);
    }

    #[tokio::test]
    async fn backpressure_does_not_deadlock() {
        let dir   = TempDir::new().unwrap();
        let phase = ScatterPhase::new(dir.path(), layout(1), 1, 1024 * 1024, 4096).unwrap();
        let mut fanout = phase.fanout();
        let batch = VecBatch((0u64..1000)
            .map(|i| (i.to_le_bytes().to_vec(), i.to_le_bytes().to_vec()))
            .collect());
        fanout.scatter_batch(&batch).await.unwrap();
        let stats = phase.finish(vec![fanout]).await.unwrap();
        assert_eq!(stats.n_keys, 1000);
    }
}
