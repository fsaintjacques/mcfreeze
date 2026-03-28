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
// ScatterPhase
// ---------------------------------------------------------------------------

/// Async fan-out scatter with one `spawn_blocking` writer per partition.
///
/// `scatter_batch` buckets a source batch by partition and sends each
/// non-empty sub-batch to the corresponding writer via a bounded channel.
/// Backpressure propagates naturally: if a writer falls behind, the channel
/// fill causes `scatter_batch` to await, slowing the source.
///
/// Each writer runs in `spawn_blocking`, calling `blocking_recv()` in a
/// tight loop and writing to `BufWriter<File>` — pure sync I/O off the
/// async executor.
pub struct ScatterPhase {
    layout:      Layout,
    senders:     Vec<mpsc::Sender<PartitionBatch>>,
    handles:     Vec<JoinHandle<Result<(), LoaderError>>>,
    /// Reusable fanout buffers — one per partition, cleared between batches
    /// via `mem::take` so the (grown) Vec is reused as the new slot after
    /// the previous contents are sent to the channel.
    sub_batches: Vec<PartitionBatch>,
    n_keys:      u64,
    data_bytes:  u64,
}

impl ScatterPhase {
    pub fn new(
        root:            &Path,
        layout:          Layout,
        channel_capacity: usize,
        data_buf_bytes:  usize,
        spill_buf_bytes: usize,
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

        Ok(Self {
            layout,
            senders,
            handles,
            sub_batches: (0..n).map(|_| Vec::new()).collect(),
            n_keys:      0,
            data_bytes:  0,
        })
    }

    /// Fan out one source batch to per-partition sub-batches and send them.
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
            // mem::take moves the populated Vec into the channel and puts a
            // fresh empty Vec back in the slot — no extra allocation per cycle
            // once the Vec has grown to its steady-state capacity.
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

    /// Current `(n_keys, data_bytes)` counters (for progress reporting).
    pub fn counters(&self) -> (u64, u64) {
        (self.n_keys, self.data_bytes)
    }

    /// Drop senders (signals EOF to writers), then join all writer tasks.
    pub async fn finish(self) -> Result<ScatterStats, LoaderError> {
        let stats = ScatterStats { n_keys: self.n_keys, data_bytes: self.data_bytes };

        // Dropping senders closes the channels, causing each writer's
        // `blocking_recv()` loop to return `None` and flush+exit cleanly.
        drop(self.senders);
        drop(self.sub_batches);

        for handle in self.handles {
            handle.await??;
        }

        Ok(stats)
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
        let dir    = TempDir::new().unwrap();
        let mut phase = ScatterPhase::new(dir.path(), layout(n), 4, 1024 * 1024, 4096).unwrap();
        let batch  = VecBatch(
            pairs.iter().map(|&(k, v)| (k.to_vec(), v.to_vec())).collect()
        );
        phase.scatter_batch(&batch).await.unwrap();
        let stats = phase.finish().await.unwrap();
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
        let mut phase = ScatterPhase::new(dir.path(), layout(1), 4, 1024 * 1024, 4096).unwrap();

        for i in 0u64..10 {
            let key = i.to_le_bytes().to_vec();
            let val = i.to_le_bytes().to_vec();
            let batch = VecBatch(vec![(key, val)]);
            phase.scatter_batch(&batch).await.unwrap();
        }

        let stats = phase.finish().await.unwrap();
        assert_eq!(stats.n_keys, 10);

        let spill = SpillReader::open(&dir.path().join("part-0").join("spill.bin")).unwrap();
        assert_eq!(spill.count(), 10);
    }

    #[tokio::test]
    async fn backpressure_does_not_deadlock() {
        // channel_capacity=1 forces the sender to wait after each sub-batch.
        let dir   = TempDir::new().unwrap();
        let mut phase = ScatterPhase::new(dir.path(), layout(1), 1, 1024 * 1024, 4096).unwrap();

        let batch = VecBatch((0u64..1000)
            .map(|i| (i.to_le_bytes().to_vec(), i.to_le_bytes().to_vec()))
            .collect());
        phase.scatter_batch(&batch).await.unwrap();
        let stats = phase.finish().await.unwrap();
        assert_eq!(stats.n_keys, 1000);
    }
}
