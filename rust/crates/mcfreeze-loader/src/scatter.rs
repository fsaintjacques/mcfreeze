// SPDX-License-Identifier: Apache-2.0

use std::path::Path;
use std::time::Instant;

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use mcfreeze_format::{
    builder::{MarkerMode, PartitionAppender},
    index::fingerprint,
    meta::Layout,
};

use crate::{error::LoaderError, source::KvBatch};

/// Publish a loader sentinel (`scatter.done`) atomically. Presence is a
/// completion signal, so the file must never exist truncated: POSIX
/// gets write + fsync + rename; object-store FUSE mounts get plain
/// create-write-close (atomic there; rename is not).
pub(crate) fn write_sentinel(
    path: &std::path::Path,
    json: &str,
    mode: MarkerMode,
) -> std::io::Result<()> {
    use std::io::Write;
    let publish = |target: &std::path::Path| -> std::io::Result<()> {
        let mut f = std::fs::File::create(target)?;
        f.write_all(json.as_bytes())?;
        f.sync_all()
    };
    match mode {
        MarkerMode::Rename => {
            let mut tmp = path.as_os_str().to_owned();
            tmp.push(".tmp");
            let tmp = std::path::PathBuf::from(tmp);
            publish(&tmp)?;
            std::fs::rename(&tmp, path)
        }
        MarkerMode::DirectWrite => publish(path),
    }
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A sub-batch routed to one partition: `(owned_key, fingerprint, owned_value)`.
type PartitionBatch = Vec<(Vec<u8>, u64, Vec<u8>)>;

pub struct ScatterStats {
    pub n_keys: u64,
    pub data_bytes: u64,
}

// ---------------------------------------------------------------------------
// scatter.done JSON
// ---------------------------------------------------------------------------

/// Upper bounds (inclusive, bytes) for Prometheus-style histogram buckets.
/// Powers of two from 64 B (VALUE_ALIGNMENT) up to 8 MiB (MAX_SIZE).
pub const SIZE_BUCKETS: &[u64] = &[
    64, 128, 256, 512, 1_024, 2_048, 4_096, 8_192, 16_384, 32_768, 65_536, 131_072, 262_144,
    524_288, 1_048_576, 2_097_152, 4_194_304, 8_388_607,
];

/// One bucket in a Prometheus-style cumulative histogram.
#[derive(Debug, Serialize, Deserialize)]
pub struct SizeBucket {
    /// Upper bound in bytes (`le` in Prometheus notation).
    pub le: u64,
    /// Cumulative count of values with size ≤ `le`.
    pub count: u64,
}

/// Value-size distribution stored in `scatter.done`.
///
/// Mirrors the Prometheus histogram exposition format:
/// cumulative bucket counts, plus summary fields (`count`, `sum`, `min`, `max`).
/// `sample_keys` may be less than `count` on the fallback path when some
/// partitions were already indexed and their spill files are gone.
#[derive(Debug, Serialize, Deserialize)]
pub struct ValueSizeHistogram {
    pub count: u64,
    pub sum: u64,
    pub min: u64,
    pub max: u64,
    pub mean: f64,
    pub sample_keys: u64,
    pub buckets: Vec<SizeBucket>,
}

impl ValueSizeHistogram {
    pub fn from_histogram(hist: &Histogram<u64>, sum: u64, sample_keys: u64) -> Self {
        let mut cumulative = 0u64;
        let mut recorded = hist.iter_recorded().peekable();
        let mut buckets = Vec::with_capacity(SIZE_BUCKETS.len());

        for &le in SIZE_BUCKETS {
            while recorded.peek().is_some_and(|v| v.value_iterated_to() <= le) {
                cumulative += recorded.next().unwrap().count_at_value();
            }
            buckets.push(SizeBucket {
                le,
                count: cumulative,
            });
        }

        ValueSizeHistogram {
            count: hist.len(),
            sum,
            min: if hist.is_empty() { 0 } else { hist.min() },
            max: if hist.is_empty() { 0 } else { hist.max() },
            mean: hist.mean(),
            sample_keys,
            buckets,
        }
    }
}

/// Per-partition stats stored in `scatter.done`.
#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionDone {
    pub n_keys: u64,
    /// `None` when size data was unavailable (already-indexed partition on the fallback path).
    pub value_sizes: Option<ValueSizeHistogram>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ScatterDone {
    /// Format that produced the transient scatter state. Defaults to V4
    /// when absent (sentinels written before this field existed).
    #[serde(default)]
    pub format: mcfreeze_format::FormatId,
    pub n_keys: u64,
    pub n_partitions: u32,
    pub data_bytes: u64,
    /// Wall-clock seconds for the scatter phase. `None` on the fallback path.
    pub wall_secs: Option<f64>,
    /// Unpadded payload bytes per second. `None` on the fallback path.
    pub bytes_per_sec: Option<u64>,
    /// Aggregate value-size histogram across all partitions.
    pub value_sizes: Option<ValueSizeHistogram>,
    /// Per-partition stats, indexed by partition number.
    pub partitions: Vec<PartitionDone>,
}

/// Create a fresh value-size histogram covering 1 B … 8 MiB with 3 sig-figs.
pub fn new_size_histogram() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(1, 8_388_607, 3)
        .expect("histogram bounds are valid constants")
}

// Internal: what each partition_writer task returns.
struct PartitionStats {
    n_keys: u64,
    sum_bytes: u64,
    histogram: Histogram<u64>,
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
    layout: Layout,
    senders: Vec<mpsc::Sender<PartitionBatch>>,
    /// Reusable per-partition accumulation buffer.  `mem::take` moves the
    /// populated Vec into the channel and leaves an empty one in its place,
    /// so the grown capacity is reused on the next batch.
    sub_batches: Vec<PartitionBatch>,
    pub n_keys: u64,
    pub data_bytes: u64,
    /// Operational value-size cap ([`LoaderConfig::max_value_bytes`]),
    /// enforced here — the earliest point every record passes through,
    /// before any routing or disk write.
    max_value_bytes: usize,
}

impl Fanout {
    /// Bucket one source batch by partition and send each non-empty sub-batch.
    ///
    /// Blocks (async-awaits) only if the target partition's channel is full,
    /// providing natural backpressure from slow writers to the source.
    pub async fn scatter_batch(&mut self, batch: &impl KvBatch) -> Result<(), LoaderError> {
        // for_each_kv's callback cannot return an error; capture the
        // first violation and fail the batch after the walk.
        let mut oversized: Option<LoaderError> = None;
        batch.for_each_kv(|key, value| {
            if oversized.is_some() {
                return;
            }
            if value.len() > self.max_value_bytes {
                oversized = Some(LoaderError::ValueTooLarge {
                    key: key_preview(key),
                    len: value.len(),
                    max: self.max_value_bytes,
                });
                return;
            }
            let fp = fingerprint(key);
            let idx = self.layout.partition_of(fp);
            self.sub_batches[idx].push((key.to_vec(), fp, value.to_vec()));
            self.n_keys += 1;
            self.data_bytes += value.len() as u64;
        });
        if let Some(e) = oversized {
            return Err(e);
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

/// Lossy UTF-8 preview of a key for error messages, truncated so a
/// pathological key cannot flood the log line that reports it.
fn key_preview(key: &[u8]) -> String {
    const MAX_CHARS: usize = 64;
    let s = String::from_utf8_lossy(key);
    if s.chars().count() <= MAX_CHARS {
        return s.into_owned();
    }
    let truncated: String = s.chars().take(MAX_CHARS).collect();
    format!("{truncated}…")
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
    root: std::path::PathBuf,
    layout: Layout,
    format: mcfreeze_format::FormatId,
    senders: Vec<mpsc::Sender<PartitionBatch>>,
    handles: Vec<JoinHandle<Result<PartitionStats, LoaderError>>>,
    max_value_bytes: usize,
    marker_mode: MarkerMode,
}

impl ScatterPhase {
    pub fn new(
        root: &Path,
        layout: Layout,
        format: mcfreeze_format::FormatId,
        appenders: Vec<Box<dyn PartitionAppender>>,
        channel_capacity: usize,
        max_value_bytes: usize,
        marker_mode: MarkerMode,
    ) -> Result<Self, LoaderError> {
        let n = layout.n_partitions as usize;
        assert_eq!(appenders.len(), n);

        let mut senders = Vec::with_capacity(n);
        let mut handles = Vec::with_capacity(n);

        for appender in appenders {
            let (tx, rx) = mpsc::channel::<PartitionBatch>(channel_capacity.max(1));
            let handle = tokio::task::spawn_blocking(move || partition_writer(appender, rx));

            senders.push(tx);
            handles.push(handle);
        }

        Ok(Self {
            root: root.to_path_buf(),
            layout,
            format,
            senders,
            handles,
            max_value_bytes,
            marker_mode,
        })
    }

    /// Create a [`Fanout`] for one concurrent producer.
    ///
    /// Cloning `mpsc::Sender` is cheap (arc bump).  Each fanout gets its own
    /// sub-batch buffer, so multiple fanouts never contend with each other.
    pub fn fanout(&self) -> Fanout {
        let n = self.layout.n_partitions as usize;
        Fanout {
            layout: self.layout,
            senders: self.senders.clone(),
            sub_batches: (0..n).map(|_| Vec::new()).collect(),
            n_keys: 0,
            data_bytes: 0,
            max_value_bytes: self.max_value_bytes,
        }
    }

    /// Aggregate stats from completed fanouts, close all channels, and join
    /// the writer tasks.
    ///
    /// `fanouts` must include every fanout created by this phase; dropping
    /// them here (together with the phase's own sender copies) closes the
    /// channels so writers can flush and exit.
    ///
    /// On success writes `<root>/scatter.done` (JSON) containing per-partition
    /// key counts and value-size quantiles, so a subsequent run can skip the
    /// scatter phase entirely.
    pub async fn finish(
        self,
        fanouts: Vec<Fanout>,
        start: Instant,
    ) -> Result<ScatterStats, LoaderError> {
        let data_bytes = fanouts.iter().map(|f| f.data_bytes).sum::<u64>();

        // Close all sender ends so each writer's blocking_recv() returns None.
        drop(fanouts);
        drop(self.senders);

        let mut all_partition_stats: Vec<PartitionStats> = Vec::with_capacity(self.handles.len());
        for handle in self.handles {
            all_partition_stats.push(handle.await??);
        }

        // Measure after all writers have flushed — this is the true wall time.
        let wall_secs = start.elapsed().as_secs_f64();
        let bytes_per_sec = (data_bytes as f64 / wall_secs.max(f64::MIN_POSITIVE)) as u64;

        let mut merged = new_size_histogram();
        let mut total_sum = 0u64;
        let mut partitions: Vec<PartitionDone> = Vec::with_capacity(all_partition_stats.len());

        for ps in all_partition_stats {
            merged
                .add(&ps.histogram)
                .expect("all partition histograms share the same bounds");
            total_sum += ps.sum_bytes;
            let part_sizes =
                ValueSizeHistogram::from_histogram(&ps.histogram, ps.sum_bytes, ps.n_keys);
            partitions.push(PartitionDone {
                n_keys: ps.n_keys,
                value_sizes: Some(part_sizes),
            });
        }

        let n_keys = partitions.iter().map(|p| p.n_keys).sum();
        let value_sizes = Some(ValueSizeHistogram::from_histogram(
            &merged, total_sum, n_keys,
        ));

        let done = ScatterDone {
            format: self.format,
            n_keys,
            n_partitions: self.layout.n_partitions,
            data_bytes,
            wall_secs: Some(wall_secs),
            bytes_per_sec: Some(bytes_per_sec),
            value_sizes,
            partitions,
        };
        let json = serde_json::to_string_pretty(&done)
            .map_err(|e| LoaderError::Io(std::io::Error::other(e)))?;
        write_sentinel(&self.root.join("scatter.done"), &json, self.marker_mode)?;

        Ok(ScatterStats { n_keys, data_bytes })
    }
}

// ---------------------------------------------------------------------------
// Partition writer (runs in spawn_blocking)
// ---------------------------------------------------------------------------

fn partition_writer(
    mut appender: Box<dyn PartitionAppender>,
    mut rx: mpsc::Receiver<PartitionBatch>,
) -> Result<PartitionStats, LoaderError> {
    let mut histogram = new_size_histogram();
    let mut sum_bytes = 0u64;
    let mut n_keys = 0u64;

    while let Some(batch) = rx.blocking_recv() {
        for (key, fp, value) in batch {
            let size = value.len() as u64;
            // record(0) is invalid; empty values map to 1 for histogram purposes.
            histogram.record(size.max(1)).unwrap_or_default();
            sum_bytes += size;
            appender.append(&key, fp, &value)?;
            n_keys += 1;
        }
    }

    appender.finish()?;
    Ok(PartitionStats {
        n_keys,
        sum_bytes,
        histogram,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::VecBatch;
    use mcfreeze_format::meta::DEFAULT_VERIFY_SEED;
    use mcfreeze_format::spill::SpillReader;
    use mcfreeze_format::{
        data::pread,
        meta::{VALUE_ALIGNMENT, VALUE_HEADER_SIZE},
    };
    use std::fs::File;
    use tempfile::TempDir;

    fn layout(n: u32) -> Layout {
        Layout::new(n).unwrap()
    }

    fn make_appenders(root: &std::path::Path, n: u32) -> Vec<Box<dyn PartitionAppender>> {
        let builder = mcfreeze_format::builder_for(
            mcfreeze_format::FormatId::V4,
            mcfreeze_format::BuilderConfig {
                root: root.to_path_buf(),
                n_partitions: n,
                verify_seed: DEFAULT_VERIFY_SEED,
                data_buf_bytes: 1024 * 1024,
                spill_buf_bytes: 4096,
                v5: Default::default(),
            },
        )
        .unwrap();
        (0..n as usize)
            .map(|p| builder.appender(p).unwrap())
            .collect()
    }

    async fn scatter(pairs: &[(&[u8], &[u8])], n: u32) -> (TempDir, ScatterStats) {
        let dir = TempDir::new().unwrap();
        let writers = make_appenders(dir.path(), n);
        let phase = ScatterPhase::new(
            dir.path(),
            layout(n),
            mcfreeze_format::FormatId::V4,
            writers,
            4,
            usize::MAX,
            MarkerMode::Rename,
        )
        .unwrap();
        let batch = VecBatch(
            pairs
                .iter()
                .map(|&(k, v)| (k.to_vec(), v.to_vec()))
                .collect(),
        );
        let mut fanout = phase.fanout();
        fanout.scatter_batch(&batch).await.unwrap();
        let stats = phase
            .finish(vec![fanout], std::time::Instant::now())
            .await
            .unwrap();
        (dir, stats)
    }

    #[tokio::test]
    async fn counts_sum_to_total() {
        let pairs: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")];
        let (_, stats) = scatter(pairs, 4).await;
        assert_eq!(stats.n_keys, pairs.len() as u64);
    }

    #[tokio::test]
    async fn oversized_value_is_rejected_with_key_context() {
        let dir = TempDir::new().unwrap();
        let phase = ScatterPhase::new(
            dir.path(),
            layout(1),
            mcfreeze_format::FormatId::V4,
            make_appenders(dir.path(), 1),
            4,
            8, // max_value_bytes
            MarkerMode::Rename,
        )
        .unwrap();
        let mut fanout = phase.fanout();
        let batch = VecBatch(vec![
            (b"fine".to_vec(), b"small".to_vec()),
            (b"the-culprit".to_vec(), vec![0u8; 9]),
        ]);
        match fanout.scatter_batch(&batch).await {
            Err(LoaderError::ValueTooLarge { key, len, max }) => {
                assert_eq!(key, "the-culprit");
                assert_eq!(len, 9);
                assert_eq!(max, 8);
            }
            other => panic!("expected ValueTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn key_preview_truncates_and_lossy_decodes() {
        assert_eq!(key_preview(b"short"), "short");
        let long = "k".repeat(200);
        let preview = key_preview(long.as_bytes());
        assert_eq!(preview.chars().count(), 65); // 64 + ellipsis
        assert!(key_preview(&[0xFF, 0xFE, b'x']).contains('x')); // lossy, no panic
    }

    #[tokio::test]
    async fn spill_entries_match_values() {
        let pairs: &[(&[u8], &[u8])] = &[(b"hello", b"world"), (b"foo", b"bar")];
        let (dir, _) = scatter(pairs, 1).await;

        let spill_path = mcfreeze_format::meta::partition_dir(dir.path(), 1, 0).join("spill.bin");
        let reader = SpillReader::open(&spill_path).unwrap();
        assert_eq!(reader.count(), 2);

        let data_file =
            File::open(mcfreeze_format::meta::partition_dir(dir.path(), 1, 0).join("data.bin"))
                .unwrap();
        for rec in reader.records() {
            let rec = rec.unwrap();
            // Read one aligned block to get the value header.
            let raw = pread(
                &data_file,
                rec.offset as u64 * VALUE_ALIGNMENT,
                VALUE_ALIGNMENT as u32,
            )
            .unwrap();
            // Parse the 12-byte header: 8B verify_fp + 4B byte_length.
            let byte_len = u32::from_le_bytes(raw[8..12].try_into().unwrap()) as usize;
            let val = &raw[VALUE_HEADER_SIZE..VALUE_HEADER_SIZE + byte_len];
            assert!(
                val == b"world" || val == b"bar",
                "unexpected value: {val:?}"
            );
        }
    }

    #[tokio::test]
    async fn data_bin_alignment() {
        let (dir, _) = scatter(&[(b"k", b"v")], 1).await;
        let meta = std::fs::metadata(
            mcfreeze_format::meta::partition_dir(dir.path(), 1, 0).join("data.bin"),
        )
        .unwrap();
        assert_eq!(meta.len() % VALUE_ALIGNMENT, 0);
        // 12-byte header + 1-byte value = 13 → padded to 64.
        assert_eq!(meta.len(), 64);
    }

    #[tokio::test]
    async fn scatter_stats() {
        let pairs: &[(&[u8], &[u8])] = &[(b"k1", b"hello"), (b"k2", b"world")];
        let (_, stats) = scatter(pairs, 1).await;
        assert_eq!(stats.n_keys, 2);
        assert_eq!(stats.data_bytes, 10);
    }

    #[tokio::test]
    async fn empty_scatter() {
        let (_, stats) = scatter(&[], 4).await;
        assert_eq!(stats.n_keys, 0);
    }

    #[tokio::test]
    async fn multiple_batches_accumulate() {
        let dir = TempDir::new().unwrap();
        let phase = ScatterPhase::new(
            dir.path(),
            layout(1),
            mcfreeze_format::FormatId::V4,
            make_appenders(dir.path(), 1),
            4,
            usize::MAX,
            MarkerMode::Rename,
        )
        .unwrap();
        let mut fanout = phase.fanout();

        for i in 0u64..10 {
            let key = i.to_le_bytes().to_vec();
            let val = i.to_le_bytes().to_vec();
            fanout
                .scatter_batch(&VecBatch(vec![(key, val)]))
                .await
                .unwrap();
        }

        let stats = phase
            .finish(vec![fanout], std::time::Instant::now())
            .await
            .unwrap();
        assert_eq!(stats.n_keys, 10);

        let spill = SpillReader::open(
            &mcfreeze_format::meta::partition_dir(dir.path(), 1, 0).join("spill.bin"),
        )
        .unwrap();
        assert_eq!(spill.count(), 10);
    }

    #[tokio::test]
    async fn parallel_fanouts_no_data_loss() {
        // Two concurrent fanouts writing to the same partition writers.
        let dir = TempDir::new().unwrap();
        let phase = ScatterPhase::new(
            dir.path(),
            layout(1),
            mcfreeze_format::FormatId::V4,
            make_appenders(dir.path(), 1),
            8,
            usize::MAX,
            MarkerMode::Rename,
        )
        .unwrap();
        let mut f0 = phase.fanout();
        let mut f1 = phase.fanout();

        let batch0 = VecBatch(
            (0u64..500)
                .map(|i| (i.to_le_bytes().to_vec(), b"v0".to_vec()))
                .collect(),
        );
        let batch1 = VecBatch(
            (500u64..1000)
                .map(|i| (i.to_le_bytes().to_vec(), b"v1".to_vec()))
                .collect(),
        );

        // Drive both fanouts concurrently.
        tokio::try_join!(f0.scatter_batch(&batch0), f1.scatter_batch(&batch1),).unwrap();

        let stats = phase
            .finish(vec![f0, f1], std::time::Instant::now())
            .await
            .unwrap();
        assert_eq!(stats.n_keys, 1000);

        let spill = SpillReader::open(
            &mcfreeze_format::meta::partition_dir(dir.path(), 1, 0).join("spill.bin"),
        )
        .unwrap();
        assert_eq!(spill.count(), 1000);
    }

    #[tokio::test]
    async fn backpressure_does_not_deadlock() {
        let dir = TempDir::new().unwrap();
        let phase = ScatterPhase::new(
            dir.path(),
            layout(1),
            mcfreeze_format::FormatId::V4,
            make_appenders(dir.path(), 1),
            1,
            usize::MAX,
            MarkerMode::Rename,
        )
        .unwrap();
        let mut fanout = phase.fanout();
        let batch = VecBatch(
            (0u64..1000)
                .map(|i| (i.to_le_bytes().to_vec(), i.to_le_bytes().to_vec()))
                .collect(),
        );
        fanout.scatter_batch(&batch).await.unwrap();
        let stats = phase
            .finish(vec![fanout], std::time::Instant::now())
            .await
            .unwrap();
        assert_eq!(stats.n_keys, 1000);
    }
}
