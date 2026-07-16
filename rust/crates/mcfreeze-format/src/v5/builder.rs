// SPDX-License-Identifier: Apache-2.0

//! V5 [`FormatBuilder`]: fingerprint-sorted block construction behind
//! the format seam (`doc/plan/FORMAT_V5_SPARSE_INDEX.md`).
//!
//! Scatter appends `(fp, vfp, value)` frames to a transient per-partition
//! `arrival.bin` in arrival order, tracking a log2 record-size histogram.
//! `plan()` derives `block_size` once, globally, from the merged
//! histograms (or the `--block-size` override) and persists the decision
//! to `v5.plan` so a resumed build cannot re-derive a different value
//! after some partitions' transient state is gone. `build()` runs the
//! per-partition radix sort + block emit; the recovery unit is the
//! partition (`build.done` marker written *before* transient cleanup).

use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    builder::{
        BuildDone, BuilderConfig, FormatBuilder, MarkerMode, PartitionAppender, ScatterProbe,
    },
    desc::FormatId,
    meta::{partition_dir, Layout, Stats, HASH_ALGORITHM},
    v5::{
        block::{checksum32, encoded_len, BlockAssembler, Stub, MAX_VALUE_LEN},
        meta::{self, validate_block_size, PartitionMeta, SketchMeta},
        verify_fingerprint,
    },
    Error, Result,
};

// ---------------------------------------------------------------------------
// File names
// ---------------------------------------------------------------------------

const ARRIVAL_BIN: &str = "arrival.bin";
const ARRIVAL_STATS: &str = "arrival.stats";
const BUILD_DONE: &str = "build.done";
const BLOCKS_BIN: &str = "blocks.bin";
const HEAP_BIN: &str = "heap.bin";
const FENCES_BIN: &str = "fences.bin";
const SKETCH_BIN: &str = "sketch.bin";
const BUCKET_PREFIX: &str = "bucket-";
/// Root-level phase sentinel (same role as V4's `index.done`).
const INDEX_DONE: &str = "index.done";
/// Root-level persisted `plan()` decision; deleted with `index.done`.
const PLAN_FILE: &str = "v5.plan";

const DEFAULT_BUCKET_BYTES: u64 = 128 * 1024 * 1024;
/// Arrival frame header: 8B fp + 8B vfp + 4B value length.
const FRAME_HEADER: usize = 20;
/// Log2 histogram buckets; index = bit width of the encoded record size.
const HIST_BUCKETS: usize = 33;

// ---------------------------------------------------------------------------
// Transient JSON schemas
// ---------------------------------------------------------------------------

/// Per-partition scatter summary (`arrival.stats`), written by the
/// appender's `finish`. Its presence is the "scatter completed" marker
/// for the partition; `arrival.bin` without it is a crashed scatter.
#[derive(Debug, Serialize, Deserialize)]
struct ArrivalStats {
    n_keys: u64,
    data_bytes: u64,
    hist: Vec<u64>,
}

/// The persisted `plan()` decision (`v5.plan`). Everything here is
/// immutable for the snapshot once the first partition builds: a
/// resumed process's own configuration must not be able to produce a
/// mixed partition set (e.g. some partitions built with a sketch and
/// some without — an unopenable snapshot).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct Plan {
    block_size: u32,
    inline_threshold: u32,
    sketch: bool,
}

/// Per-partition build result (`build.done`). Written before the
/// partition's transient state is deleted; its presence makes the
/// partition build skippable on resume.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionBuildDone {
    pub n_keys: u64,
    /// Scatter-time frame bytes (`arrival.bin` size), preserved so
    /// `scatter_probe` reports the same value before and after build.
    pub data_bytes: u64,
    pub n_blocks: u64,
    pub n_stubs: u64,
    pub blocks_bytes: u64,
    pub heap_bytes: u64,
    #[serde(default)]
    pub sketch_bytes: u64,
}

/// Phase sentinel (`index.done`; stable schema, embedded into
/// `meta.json` stats by the loader).
#[derive(Debug, Serialize, Deserialize)]
pub struct IndexDone {
    pub n_keys: u64,
    pub n_blocks: u64,
    pub n_stubs: u64,
    pub block_size: u32,
    pub inline_threshold: u32,
    pub blocks_bytes: u64,
    pub heap_bytes: u64,
    pub fences_bytes: u64,
    /// Whether per-partition sketches were built — the source of truth
    /// for `meta.sketch` (a resumed finalize must reflect what was
    /// built, not what the resuming process was configured with).
    #[serde(default)]
    pub sketch: bool,
    #[serde(default)]
    pub sketch_bytes: u64,
    pub wall_secs: Option<f64>,
    pub partitions: Vec<PartitionBuildDone>,
}

// ---------------------------------------------------------------------------
// V5Appender
// ---------------------------------------------------------------------------

struct V5Appender {
    dir: PathBuf,
    out: BufWriter<File>,
    verify_seed: u64,
    marker_mode: MarkerMode,
    n_keys: u64,
    data_bytes: u64,
    hist: Vec<u64>,
}

impl PartitionAppender for V5Appender {
    fn append(&mut self, key: &[u8], fp: u64, value: &[u8]) -> Result<()> {
        // Enforce the format's 31-bit value limit at scatter time: an
        // oversized value must fail here, not corrupt the u32 frame
        // length (≥ 4 GiB wraps) or surface hours later when build()
        // feeds the assembler.
        if value.len() as u64 > MAX_VALUE_LEN as u64 {
            return Err(Error::ValueTooLarge {
                len: value.len() as u64,
                max: MAX_VALUE_LEN,
            });
        }
        let vfp = verify_fingerprint(key, self.verify_seed);
        self.out.write_all(&fp.to_le_bytes())?;
        self.out.write_all(&vfp.to_le_bytes())?;
        self.out.write_all(&(value.len() as u32).to_le_bytes())?;
        self.out.write_all(value)?;

        let rec = encoded_len(value.len()) as u64;
        self.hist[(u64::BITS - rec.leading_zeros()) as usize] += 1;
        self.n_keys += 1;
        self.data_bytes += (FRAME_HEADER + value.len()) as u64;
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<()> {
        let this = *self;
        // arrival.bin must be durable and closed before arrival.stats
        // asserts scatter completed: a crash between the two must read
        // as "not scattered", never as a marker over a truncated log —
        // parse_frames stops cleanly at a frame boundary, so a lost
        // tail would otherwise become a silently short partition.
        sync_and_close(this.out)?;
        let stats = ArrivalStats {
            n_keys: this.n_keys,
            data_bytes: this.data_bytes,
            hist: this.hist,
        };
        write_marker(
            &this.dir.join(ARRIVAL_STATS),
            &serde_json::to_string(&stats)?,
            this.marker_mode,
        )
    }
}

// ---------------------------------------------------------------------------
// V5Builder
// ---------------------------------------------------------------------------

pub struct V5Builder {
    root: PathBuf,
    layout: Layout,
    verify_seed: u64,
    data_buf_bytes: usize,
    block_size_override: Option<u32>,
    bucket_bytes: u64,
    sketch: bool,
    marker_mode: MarkerMode,
    /// Loaded/derived by `plan()`; consumed by `build`.
    plan: Mutex<Option<Plan>>,
    /// Set by `build` (live and sentinel-skip paths); consumed by
    /// `finalize` for `meta.json`.
    done: Mutex<Option<IndexDone>>,
}

impl V5Builder {
    pub fn new(config: BuilderConfig) -> Result<Self> {
        if let Some(bs) = config.v5.block_size {
            validate_block_size(bs)?;
        }
        Ok(Self {
            layout: Layout::new(config.n_partitions)?,
            verify_seed: config.verify_seed,
            data_buf_bytes: config.data_buf_bytes,
            block_size_override: config.v5.block_size,
            bucket_bytes: config.v5.bucket_bytes.unwrap_or(DEFAULT_BUCKET_BYTES),
            sketch: config.v5.sketch,
            marker_mode: config.v5.marker_mode,
            root: config.root,
            plan: Mutex::new(None),
            done: Mutex::new(None),
        })
    }

    fn partition_dir(&self, p: usize) -> PathBuf {
        partition_dir(&self.root, self.layout.n_partitions, p)
    }

    /// Load or derive the plan, exactly once per snapshot: an existing
    /// `v5.plan` always wins (the decision is immutable once the first
    /// block is cut), otherwise auto-tune from the merged scatter
    /// histograms and persist before returning.
    fn ensure_plan(&self) -> Result<Plan> {
        let mut guard = self.plan.lock().expect("plan poisoned");
        if let Some(plan) = *guard {
            return Ok(plan);
        }

        let path = self.root.join(PLAN_FILE);
        let plan = if path.exists() {
            serde_json::from_str(&fs::read_to_string(&path)?)?
        } else {
            let mut hist = vec![0u64; HIST_BUCKETS];
            for p in 0..self.layout.n_partitions as usize {
                let stats_path = self.partition_dir(p).join(ARRIVAL_STATS);
                let stats: ArrivalStats = serde_json::from_str(&fs::read_to_string(stats_path)?)?;
                for (h, s) in hist.iter_mut().zip(stats.hist) {
                    *h += s;
                }
            }
            let block_size = match self.block_size_override {
                Some(bs) => bs,
                None => auto_tune_block_size(&hist),
            };
            let plan = Plan {
                block_size,
                inline_threshold: block_size / 2,
                sketch: self.sketch,
            };
            write_marker(&path, &serde_json::to_string(&plan)?, self.marker_mode)?;
            plan
        };
        *guard = Some(plan);
        Ok(plan)
    }

    /// Best-effort sweep of transient files that survive a crash after
    /// `index.done` was written.
    fn sweep_transients(&self) {
        let _ = fs::remove_file(self.root.join(PLAN_FILE));
        for p in 0..self.layout.n_partitions as usize {
            sweep_partition_transients(&self.partition_dir(p));
        }
    }
}

fn sweep_partition_transients(dir: &Path) {
    let _ = fs::remove_file(dir.join(ARRIVAL_BIN));
    let _ = fs::remove_file(dir.join(ARRIVAL_STATS));
    remove_bucket_files(dir);
}

/// Removes radix bucket files and orphaned `.tmp` marker files (a crash
/// between `write_marker`'s create and rename leaves one behind).
fn remove_bucket_files(dir: &Path) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(BUCKET_PREFIX) || name.ends_with(".tmp") {
            let _ = fs::remove_file(entry.path());
        }
    }
}

/// Atomically publish a marker file. Marker *presence* is a completion
/// signal (`arrival.stats`, `v5.plan`, `build.done`, `index.done`,
/// `meta.json`), so a marker must never exist truncated — a plain
/// interrupted write leaves a file that asserts completion while its
/// JSON no longer parses, wedging every resume path until it is
/// manually deleted. The atomicity mechanism is backend-dependent
/// ([`MarkerMode`]): POSIX gets write + fsync + rename; object-store
/// FUSE mounts get create + write + close, which is already atomic
/// there (and where rename is unsupported or non-atomic).
fn write_marker(path: &Path, json: &str, mode: MarkerMode) -> Result<()> {
    match mode {
        MarkerMode::Rename => {
            let mut tmp = path.as_os_str().to_owned();
            tmp.push(".tmp");
            let tmp = PathBuf::from(tmp);
            {
                let mut f = File::create(&tmp)?;
                f.write_all(json.as_bytes())?;
                f.sync_all()?;
            }
            fs::rename(&tmp, path)?;
        }
        MarkerMode::DirectWrite => {
            let mut f = File::create(path)?;
            f.write_all(json.as_bytes())?;
            f.sync_all()?;
        }
    }
    Ok(())
}

/// `clamp(next_pow2(2 × median_record_size), 4 KiB, 64 KiB)`, with the
/// median read from the log2 histogram (bucket `b` covers sizes in
/// `[2^(b-1), 2^b)`; the power-of-two decision needs no finer grain).
fn auto_tune_block_size(hist: &[u64]) -> u32 {
    let total: u64 = hist.iter().sum();
    if total == 0 {
        return meta::MIN_BLOCK_SIZE;
    }
    let mut cumulative = 0u64;
    let median_bucket = hist
        .iter()
        .position(|&c| {
            cumulative += c;
            cumulative >= total.div_ceil(2)
        })
        .expect("non-empty histogram");
    // Sizes in bucket b are < 2^b; twice that is < 2^(b+1).
    let doubled_log2 = median_bucket as u32 + 1;
    let block = 1u64 << doubled_log2.min(31);
    (block as u32).clamp(meta::MIN_BLOCK_SIZE, meta::MAX_AUTO_BLOCK_SIZE)
}

// ---------------------------------------------------------------------------
// FormatBuilder impl
// ---------------------------------------------------------------------------

impl FormatBuilder for V5Builder {
    fn format(&self) -> FormatId {
        FormatId::V5
    }

    fn appender(&self, p: usize) -> Result<Box<dyn PartitionAppender>> {
        let dir = self.partition_dir(p);
        fs::create_dir_all(&dir)?;
        // Fresh scatter for this partition: a leftover arrival.stats from
        // a previous run must not survive next to the new arrival.bin.
        let _ = fs::remove_file(dir.join(ARRIVAL_STATS));
        let file = File::create(dir.join(ARRIVAL_BIN))?;
        Ok(Box::new(V5Appender {
            out: BufWriter::with_capacity(self.data_buf_bytes, file),
            dir,
            verify_seed: self.verify_seed,
            marker_mode: self.marker_mode,
            n_keys: 0,
            data_bytes: 0,
            hist: vec![0; HIST_BUCKETS],
        }))
    }

    fn scatter_probe(&self, p: usize) -> Result<Option<ScatterProbe>> {
        let dir = self.partition_dir(p);
        let stats_path = dir.join(ARRIVAL_STATS);
        if stats_path.exists() {
            let stats: ArrivalStats = serde_json::from_str(&fs::read_to_string(stats_path)?)?;
            return Ok(Some(ScatterProbe {
                n_keys: stats.n_keys,
                data_bytes: stats.data_bytes,
            }));
        }
        let done_path = dir.join(BUILD_DONE);
        if done_path.exists() {
            // Already built: scatter necessarily completed.
            let done: PartitionBuildDone = serde_json::from_str(&fs::read_to_string(done_path)?)?;
            return Ok(Some(ScatterProbe {
                n_keys: done.n_keys,
                // The scatter-time byte count, persisted through the
                // marker so the probe answer is stable across a resume.
                data_bytes: done.data_bytes,
            }));
        }
        // arrival.bin without arrival.stats is a crashed scatter: report
        // "not scattered" so the loader redoes it (appender truncates).
        Ok(None)
    }

    fn plan(&self) -> Result<()> {
        if self.root.join(INDEX_DONE).exists() {
            return Ok(()); // build will take the sentinel-skip path
        }
        self.ensure_plan().map(|_| ())
    }

    fn build(
        &self,
        parallelism: usize,
        progress: Option<Arc<dyn Fn(u64, u64) + Send + Sync>>,
    ) -> Result<BuildDone> {
        let sentinel = self.root.join(INDEX_DONE);
        if sentinel.exists() {
            let done: IndexDone = serde_json::from_str(&fs::read_to_string(&sentinel)?)?;
            tracing::info!(n_keys = done.n_keys, "v5 index already complete, skipping");
            // A crash between writing index.done and the transient sweep
            // leaves files behind; every later run takes this path.
            self.sweep_transients();
            let n_keys = done.n_keys;
            *self.done.lock().expect("done poisoned") = Some(done);
            return Ok(BuildDone { n_keys });
        }

        let plan = self.ensure_plan()?;
        let start = Instant::now();
        let cb = &progress;

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(parallelism.max(1))
            .build()
            .map_err(|e| Error::Io(std::io::Error::other(e)))?;

        let results: Vec<Result<PartitionBuildDone>> = pool.install(|| {
            (0..self.layout.n_partitions as usize)
                .into_par_iter()
                .map(|p| {
                    let done = build_partition(
                        &self.partition_dir(p),
                        plan,
                        self.bucket_bytes,
                        self.data_buf_bytes,
                        self.marker_mode,
                    )?;
                    if let Some(ref f) = cb {
                        f(1, 0);
                    }
                    Ok(done)
                })
                .collect()
        });
        let partitions = results.into_iter().collect::<Result<Vec<_>>>()?;

        let done = IndexDone {
            n_keys: partitions.iter().map(|p| p.n_keys).sum(),
            n_blocks: partitions.iter().map(|p| p.n_blocks).sum(),
            n_stubs: partitions.iter().map(|p| p.n_stubs).sum(),
            block_size: plan.block_size,
            inline_threshold: plan.inline_threshold,
            blocks_bytes: partitions.iter().map(|p| p.blocks_bytes).sum(),
            heap_bytes: partitions.iter().map(|p| p.heap_bytes).sum(),
            fences_bytes: partitions.iter().map(|p| p.n_blocks * 4).sum(),
            sketch: plan.sketch,
            sketch_bytes: partitions.iter().map(|p| p.sketch_bytes).sum(),
            wall_secs: Some(start.elapsed().as_secs_f64()),
            partitions,
        };
        write_marker(
            &sentinel,
            &serde_json::to_string_pretty(&done)?,
            self.marker_mode,
        )?;
        // The plan is never needed once the phase sentinel exists.
        let _ = fs::remove_file(self.root.join(PLAN_FILE));

        let n_keys = done.n_keys;
        *self.done.lock().expect("done poisoned") = Some(done);
        Ok(BuildDone { n_keys })
    }

    fn finalize(&self, stats: Stats, encoding: Option<serde_json::Value>) -> Result<()> {
        let guard = self.done.lock().expect("done poisoned");
        let done = guard.as_ref().ok_or(Error::FinalizeBeforeBuild)?;
        let meta = meta::Meta {
            format_version: meta::FORMAT_VERSION,
            hash_algorithm: HASH_ALGORITHM.to_string(),
            verify_seed: self.verify_seed,
            block_size: done.block_size,
            inline_threshold: done.inline_threshold,
            sketch: done.sketch.then(|| SketchMeta {
                kind: crate::v5::sketch::KIND.to_string(),
            }),
            partitions: done
                .partitions
                .iter()
                .map(|p| PartitionMeta {
                    n_blocks: p.n_blocks,
                })
                .collect(),
            stats: Some(stats),
            encoding,
        };
        // meta.json is the snapshot completeness sentinel — atomic like
        // every other marker.
        write_marker(
            &self.root.join("meta.json"),
            &serde_json::to_string_pretty(&meta)?,
            self.marker_mode,
        )
    }
}

// ---------------------------------------------------------------------------
// Per-partition build: radix pass + in-RAM sorts + block emit
// ---------------------------------------------------------------------------

fn build_partition(
    dir: &Path,
    plan: Plan,
    bucket_bytes: u64,
    buf_bytes: usize,
    marker_mode: MarkerMode,
) -> Result<PartitionBuildDone> {
    let done_path = dir.join(BUILD_DONE);
    if done_path.exists() {
        let done = serde_json::from_str(&fs::read_to_string(&done_path)?)?;
        // Crash between the marker write and transient cleanup: finish
        // the cleanup now (idempotent).
        sweep_partition_transients(dir);
        return Ok(done);
    }

    // Rebuild from scratch: sweep partial outputs from a crashed build.
    for f in [BLOCKS_BIN, HEAP_BIN, FENCES_BIN, SKETCH_BIN] {
        let _ = fs::remove_file(dir.join(f));
    }
    remove_bucket_files(dir);

    let arrival = dir.join(ARRIVAL_BIN);
    let arrival_bytes = fs::metadata(&arrival)?.len();

    // Radix pass: split arrival.bin into S sorted-concatenable bucket
    // files by the top log2(S) bits of fp. S == 1 sorts arrival.bin
    // directly — no copy.
    let n_buckets = bucket_count(arrival_bytes, bucket_bytes);
    if (n_buckets as u64) * bucket_bytes < arrival_bytes {
        tracing::warn!(
            arrival_bytes,
            n_buckets,
            actual_bucket_bytes = arrival_bytes / n_buckets as u64,
            target_bucket_bytes = bucket_bytes,
            "radix bucket count capped; sort RAM per bucket exceeds target"
        );
    }
    let buckets: Vec<PathBuf> = if n_buckets == 1 {
        vec![arrival.clone()]
    } else {
        scatter_to_buckets(dir, &arrival, n_buckets, buf_bytes)?
    };

    let blocks_file = File::create(dir.join(BLOCKS_BIN))?;
    let mut blocks_out = BufWriter::with_capacity(buf_bytes, blocks_file);
    let mut heap_out = BufWriter::with_capacity(buf_bytes, File::create(dir.join(HEAP_BIN))?);

    let mut n_keys = 0u64;
    let mut n_stubs = 0u64;
    let mut heap_offset = 0u64;
    // Sketch input: every fp of the partition, accumulated across the
    // bucket passes (binary fuse needs the full set). Arrives sorted,
    // so adjacent dedup below satisfies the filter's uniqueness
    // requirement. Transient build RAM when enabled: 8 B/key for this
    // vector plus ~10–15 B/key of xorf construction working memory —
    // per building partition, independent of `bucket_bytes`.
    let sketch = plan.sketch;
    let mut sketch_fps: Vec<u64> = Vec::new();
    let mut asm = BlockAssembler::new(plan.block_size as usize, |b: &[u8]| {
        blocks_out.write_all(b).map_err(Error::from)
    });

    for bucket in &buckets {
        let buf = fs::read(bucket)?;
        let mut frames = parse_frames(&buf)?;
        frames.sort_unstable_by_key(|f| f.fp);
        for f in frames {
            if sketch {
                sketch_fps.push(f.fp);
            }
            let value = &buf[f.start..f.start + f.len];
            if value.len() as u32 <= plan.inline_threshold {
                asm.push_inline(f.fp, f.vfp, value)?;
            } else {
                heap_out.write_all(value)?;
                asm.push_stub(
                    f.fp,
                    f.vfp,
                    Stub {
                        heap_offset,
                        value_len: value.len() as u32,
                        value_checksum: checksum32(value),
                    },
                )?;
                heap_offset += value.len() as u64;
                n_stubs += 1;
            }
            n_keys += 1;
        }
        if *bucket != arrival {
            let _ = fs::remove_file(bucket);
        }
    }

    let fences = asm.finish()?;
    // Durability ordering: every byte build.done attests must be synced
    // and closed BEFORE the marker publishes. A marker outliving a
    // truncated data file is exactly the torn state markers exist to
    // prevent — and on object-store FUSE mounts, close is what uploads
    // the object at all.
    sync_and_close(blocks_out)?;
    sync_and_close(heap_out)?;

    let mut fence_bytes = Vec::with_capacity(fences.len() * 4);
    for f in &fences {
        fence_bytes.extend_from_slice(&f.to_le_bytes());
    }
    write_data_file(&dir.join(FENCES_BIN), &fence_bytes)?;

    // Empty partitions get no sketch: empty fences already resolve
    // every lookup to a zero-I/O miss, and the reader skips loading.
    let mut sketch_bytes = 0u64;
    if sketch && !sketch_fps.is_empty() {
        sketch_fps.dedup();
        let bytes = crate::v5::sketch::build(&sketch_fps)?;
        sketch_bytes = bytes.len() as u64;
        write_data_file(&dir.join(SKETCH_BIN), &bytes)?;
    }

    let done = PartitionBuildDone {
        n_keys,
        data_bytes: arrival_bytes,
        n_blocks: fences.len() as u64,
        n_stubs,
        blocks_bytes: fences.len() as u64 * plan.block_size as u64,
        heap_bytes: heap_offset,
        sketch_bytes,
    };
    // Marker before cleanup: a crash in between leaves the partition
    // skippable, with only idempotent deletion left to redo.
    write_marker(&done_path, &serde_json::to_string(&done)?, marker_mode)?;
    sweep_partition_transients(dir);
    Ok(done)
}

/// Flush, fsync, and close a buffered data stream. Must run before the
/// marker that attests the file is published.
fn sync_and_close(out: BufWriter<File>) -> Result<()> {
    let file = out.into_inner().map_err(|e| Error::Io(e.into_error()))?;
    file.sync_all()?;
    Ok(())
}

/// Write a whole data file and fsync it before returning, so a marker
/// published afterwards never attests bytes the kernel still holds.
fn write_data_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = File::create(path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(())
}

/// Radix bucket count for a partition: enough buckets to hit the target
/// bucket size, capped to bound simultaneously open bucket files
/// (`n_buckets` fds per building partition, times the build
/// parallelism). At the cap, buckets grow past the target — sort RAM
/// per bucket rises, the build still completes (warned at the call
/// site).
const MAX_RADIX_BUCKETS: u64 = 64;

fn bucket_count(arrival_bytes: u64, bucket_bytes: u64) -> usize {
    arrival_bytes
        .div_ceil(bucket_bytes.max(1))
        .checked_next_power_of_two()
        .unwrap_or(u64::MAX)
        .clamp(1, MAX_RADIX_BUCKETS) as usize
}

/// One streaming pass over `arrival.bin`, appending each frame to the
/// bucket file owning its fp's top bits. Bucket order is fp order, so
/// concatenating sorted buckets is the sorted partition.
fn scatter_to_buckets(
    dir: &Path,
    arrival: &Path,
    n_buckets: usize,
    buf_bytes: usize,
) -> Result<Vec<PathBuf>> {
    let shift = 64 - n_buckets.trailing_zeros();
    let paths: Vec<PathBuf> = (0..n_buckets)
        .map(|b| dir.join(format!("{BUCKET_PREFIX}{b:04}.bin")))
        .collect();
    let mut outs: Vec<BufWriter<File>> = paths
        .iter()
        .map(|p| {
            let cap = (buf_bytes / n_buckets).max(4096);
            Ok(BufWriter::with_capacity(cap, File::create(p)?))
        })
        .collect::<Result<_>>()?;

    let mut input = BufReader::with_capacity(buf_bytes, File::open(arrival)?);
    let mut header = [0u8; FRAME_HEADER];
    let mut value = Vec::new();
    loop {
        if !read_exact_or_eof(&mut input, &mut header)? {
            break;
        }
        let fp = u64::from_le_bytes(header[0..8].try_into().unwrap());
        let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
        value.resize(len, 0);
        input.read_exact(&mut value)?;

        let out = &mut outs[(fp >> shift) as usize];
        out.write_all(&header)?;
        out.write_all(&value)?;
    }
    for mut out in outs {
        out.flush()?;
    }
    Ok(paths)
}

/// Read exactly `buf.len()` bytes, or return `false` on clean EOF at a
/// frame boundary. EOF mid-frame is an error (truncated arrival.bin).
fn read_exact_or_eof(r: &mut impl Read, buf: &mut [u8]) -> Result<bool> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = r.read(&mut buf[filled..])?;
        if n == 0 {
            if filled == 0 {
                return Ok(false);
            }
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated arrival frame",
            )));
        }
        filled += n;
    }
    Ok(true)
}

struct FrameRef {
    fp: u64,
    vfp: u64,
    start: usize,
    len: usize,
}

/// Parse a bucket buffer into frame references for the in-RAM sort.
fn parse_frames(buf: &[u8]) -> Result<Vec<FrameRef>> {
    let mut frames = Vec::new();
    let mut pos = 0;
    while pos < buf.len() {
        if pos + FRAME_HEADER > buf.len() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated arrival frame header",
            )));
        }
        let fp = u64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap());
        let vfp = u64::from_le_bytes(buf[pos + 8..pos + 16].try_into().unwrap());
        let len = u32::from_le_bytes(buf[pos + 16..pos + 20].try_into().unwrap()) as usize;
        let start = pos + FRAME_HEADER;
        if start + len > buf.len() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated arrival frame value",
            )));
        }
        frames.push(FrameRef {
            fp,
            vfp,
            start,
            len,
        });
        pos = start + len;
    }
    Ok(frames)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::V5Options;
    use crate::index::fingerprint;
    use crate::meta::DEFAULT_VERIFY_SEED;
    use crate::v5::{block, fence};
    use tempfile::TempDir;

    fn make_builder(root: &Path, n: u32, v5: V5Options) -> V5Builder {
        V5Builder::new(BuilderConfig {
            root: root.to_path_buf(),
            n_partitions: n,
            verify_seed: DEFAULT_VERIFY_SEED,
            data_buf_bytes: 64 * 1024,
            spill_buf_bytes: 4096,
            v5,
        })
        .unwrap()
    }

    fn scatter(builder: &V5Builder, pairs: &[(Vec<u8>, Vec<u8>)], n: u32) {
        let layout = Layout::new(n).unwrap();
        let mut apps: Vec<_> = (0..n as usize)
            .map(|p| builder.appender(p).unwrap())
            .collect();
        for (k, v) in pairs {
            let fp = fingerprint(k);
            apps[layout.partition_of(fp)].append(k, fp, v).unwrap();
        }
        for a in apps {
            a.finish().unwrap();
        }
    }

    fn build_snapshot(pairs: &[(Vec<u8>, Vec<u8>)], n: u32, v5: V5Options) -> (TempDir, V5Builder) {
        let dir = TempDir::new().unwrap();
        let builder = make_builder(dir.path(), n, v5);
        scatter(&builder, pairs, n);
        builder.plan().unwrap();
        builder.build(2, None).unwrap();
        (dir, builder)
    }

    fn pairs(n: usize, value_len: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..n)
            .map(|i| {
                (
                    format!("key-{i}").into_bytes(),
                    format!("value-{i}-{}", "x".repeat(value_len)).into_bytes(),
                )
            })
            .collect()
    }

    /// Reader-less lookup against the built files, using the stage-1
    /// primitives: fences route, blocks scan, stubs follow into heap.
    fn lookup(dir: &Path, n: u32, block_size: usize, key: &[u8]) -> Option<Vec<u8>> {
        let layout = Layout::new(n).unwrap();
        let fp = fingerprint(key);
        let vfp = verify_fingerprint(key, DEFAULT_VERIFY_SEED);
        let pdir = partition_dir(dir, n, layout.partition_of(fp));

        let fence_bytes = fs::read(pdir.join(FENCES_BIN)).unwrap();
        let fences: Vec<u32> = fence_bytes
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
            .collect();
        let blocks = fs::read(pdir.join(BLOCKS_BIN)).unwrap();
        assert_eq!(blocks.len(), fences.len() * block_size);

        for b in fence::candidate_blocks(&fences, fence::fence_of(fp)) {
            let blk = &blocks[b * block_size..(b + 1) * block_size];
            match block::find(blk, vfp).unwrap() {
                Some(block::Record::Inline { value, .. }) => return Some(value.to_vec()),
                Some(block::Record::Stub { stub, .. }) => {
                    let heap = fs::read(pdir.join(HEAP_BIN)).unwrap();
                    let start = stub.heap_offset as usize;
                    let value = heap[start..start + stub.value_len as usize].to_vec();
                    assert_eq!(block::checksum32(&value), stub.value_checksum);
                    return Some(value);
                }
                None => {}
            }
        }
        None
    }

    fn assert_all_present(dir: &Path, n: u32, block_size: usize, pairs: &[(Vec<u8>, Vec<u8>)]) {
        for (k, v) in pairs {
            assert_eq!(
                lookup(dir, n, block_size, k).as_deref(),
                Some(&v[..]),
                "missing or wrong value for {:?}",
                String::from_utf8_lossy(k)
            );
        }
    }

    fn opts(block_size: u32) -> V5Options {
        V5Options {
            block_size: Some(block_size),
            ..Default::default()
        }
    }

    #[test]
    fn roundtrip_inline_multi_block_multi_partition() {
        let data = pairs(2000, 100); // ~36 blocks across 4 partitions
        let (dir, builder) = build_snapshot(&data, 4, opts(4096));
        assert_all_present(dir.path(), 4, 4096, &data);
        assert!(lookup(dir.path(), 4, 4096, b"absent-key").is_none());

        let guard = builder.done.lock().unwrap();
        let done = guard.as_ref().unwrap();
        assert_eq!(done.n_keys, 2000);
        assert_eq!(done.n_stubs, 0);
        assert!(done.n_blocks > 4, "expected multiple blocks per partition");
    }

    #[test]
    fn values_above_threshold_go_to_heap() {
        // 4096 blocks -> threshold 2048; 3000-byte values are stubbed,
        // 100-byte ones stay inline.
        let data: Vec<(Vec<u8>, Vec<u8>)> = (0..100)
            .map(|i| {
                let len = if i < 50 { 3000 } else { 100 };
                (format!("k{i}").into_bytes(), vec![b'x'; len])
            })
            .collect();
        let (dir, builder) = build_snapshot(&data, 2, opts(4096));
        assert_all_present(dir.path(), 2, 4096, &data);

        let guard = builder.done.lock().unwrap();
        let done = guard.as_ref().unwrap();
        assert_eq!(done.n_stubs, 50);
        assert!(done.heap_bytes >= 50 * 3000);
    }

    #[test]
    fn radix_bucketing_preserves_order() {
        // Tiny bucket target forces many buckets; the assembler's
        // fp-sorted debug assertion validates cross-bucket order.
        let data = pairs(1000, 100);
        let v5 = V5Options {
            block_size: Some(4096),
            bucket_bytes: Some(4096),
            ..Default::default()
        };
        let (dir, _) = build_snapshot(&data, 1, v5);
        assert_all_present(dir.path(), 1, 4096, &data);
        // Bucket files are transient and must be gone.
        let pdir = partition_dir(dir.path(), 1, 0);
        for entry in fs::read_dir(&pdir).unwrap().flatten() {
            assert!(
                !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(BUCKET_PREFIX),
                "leftover bucket file: {:?}",
                entry.path()
            );
        }
    }

    #[test]
    fn empty_partitions_build_ok() {
        let (dir, builder) = build_snapshot(&[], 4, opts(4096));
        for p in 0..4 {
            let pdir = partition_dir(dir.path(), 4, p);
            assert_eq!(fs::metadata(pdir.join(BLOCKS_BIN)).unwrap().len(), 0);
            assert_eq!(fs::metadata(pdir.join(FENCES_BIN)).unwrap().len(), 0);
        }
        let guard = builder.done.lock().unwrap();
        assert_eq!(guard.as_ref().unwrap().n_keys, 0);
    }

    #[test]
    fn transients_removed_after_build() {
        let (dir, _) = build_snapshot(&pairs(100, 100), 2, opts(4096));
        for p in 0..2 {
            let pdir = partition_dir(dir.path(), 2, p);
            assert!(!pdir.join(ARRIVAL_BIN).exists());
            assert!(!pdir.join(ARRIVAL_STATS).exists());
            assert!(pdir.join(BUILD_DONE).exists());
        }
        assert!(!dir.path().join(PLAN_FILE).exists());
        assert!(dir.path().join(INDEX_DONE).exists());
    }

    #[test]
    fn scatter_probe_branches() {
        let dir = TempDir::new().unwrap();
        let builder = make_builder(dir.path(), 1, opts(4096));

        // Fresh: nothing scattered.
        assert!(builder.scatter_probe(0).unwrap().is_none());

        // Crashed scatter: arrival.bin without arrival.stats.
        let mut app = builder.appender(0).unwrap();
        let fp = fingerprint(b"k");
        app.append(b"k", fp, b"v").unwrap();
        drop(app); // no finish()
        assert!(builder.scatter_probe(0).unwrap().is_none());

        // Completed scatter: stats present, keys countable.
        let mut app = builder.appender(0).unwrap();
        app.append(b"k", fp, b"v").unwrap();
        app.finish().unwrap();
        let probe = builder.scatter_probe(0).unwrap().unwrap();
        assert_eq!(probe.n_keys, 1);
        assert!(probe.data_bytes > 0);
        let scatter_bytes = probe.data_bytes;

        // Built: transient state gone, marker answers the probe with
        // the same numbers as before the build.
        builder.plan().unwrap();
        builder.build(1, None).unwrap();
        let probe = builder.scatter_probe(0).unwrap().unwrap();
        assert_eq!(probe.n_keys, 1);
        assert_eq!(
            probe.data_bytes, scatter_bytes,
            "probe must be stable across a resume boundary"
        );
    }

    #[test]
    fn bucket_count_caps_open_files() {
        assert_eq!(bucket_count(0, 128), 1);
        assert_eq!(bucket_count(100, 128), 1);
        assert_eq!(bucket_count(1000, 128), 8);
        // Cap: tiny bucket target on a large partition must not demand
        // thousands of simultaneously open files.
        assert_eq!(bucket_count(u64::MAX, 1), MAX_RADIX_BUCKETS as usize);
        assert_eq!(bucket_count(1 << 40, 4096), MAX_RADIX_BUCKETS as usize);
        // Degenerate knob value: treated as a 1-byte target, capped.
        assert_eq!(bucket_count(100, 0), MAX_RADIX_BUCKETS as usize);
    }

    #[test]
    fn markers_are_published_atomically() {
        // write_marker must leave no .tmp sibling on success, and the
        // sweep must remove one orphaned by a crash.
        let dir = TempDir::new().unwrap();
        let marker = dir.path().join(BUILD_DONE);
        write_marker(&marker, "{}", MarkerMode::Rename).unwrap();
        assert!(marker.exists());
        assert!(!dir.path().join("build.done.tmp").exists());

        // DirectWrite: no temp sibling is ever created (object-store
        // FUSE mounts, where rename is unsupported and close is atomic).
        let direct = dir.path().join("index.done");
        write_marker(&direct, "{}", MarkerMode::DirectWrite).unwrap();
        assert!(direct.exists());
        assert!(!dir.path().join("index.done.tmp").exists());

        fs::write(dir.path().join("build.done.tmp"), b"orphan").unwrap();
        sweep_partition_transients(dir.path());
        assert!(!dir.path().join("build.done.tmp").exists());
        assert!(marker.exists(), "sweep must not touch the real marker");
    }

    #[test]
    fn build_resumes_from_scatter_state_in_fresh_process() {
        let data = pairs(200, 100);
        let dir = TempDir::new().unwrap();
        {
            let b1 = make_builder(dir.path(), 2, opts(4096));
            scatter(&b1, &data, 2);
        } // dropped without plan/build

        let b2 = make_builder(dir.path(), 2, opts(4096));
        b2.plan().unwrap();
        let done = b2.build(2, None).unwrap();
        assert_eq!(done.n_keys, 200);
        assert_all_present(dir.path(), 2, 4096, &data);
    }

    #[test]
    fn partition_marker_skips_rebuild_and_finishes_cleanup() {
        // Crash between build.done and transient cleanup: marker present,
        // arrival.bin still on disk. Resume must skip the rebuild and
        // finish the deletion.
        let data = pairs(100, 100);
        let (dir, _) = build_snapshot(&data, 1, opts(4096));
        let pdir = partition_dir(dir.path(), 1, 0);
        fs::write(pdir.join(ARRIVAL_BIN), b"leftover").unwrap();
        fs::remove_file(dir.path().join(INDEX_DONE)).unwrap();

        let blocks_before = fs::read(pdir.join(BLOCKS_BIN)).unwrap();
        let b = make_builder(dir.path(), 1, opts(4096));
        // No plan file and no arrival.stats: ensure_plan must not be
        // needed for a fully-marked partition set... but build() derives
        // the plan before partition dispatch, so re-write the plan file
        // as the crashed process would have left it.
        fs::write(
            dir.path().join(PLAN_FILE),
            serde_json::to_string(&Plan {
                block_size: 4096,
                inline_threshold: 2048,
                sketch: true,
            })
            .unwrap(),
        )
        .unwrap();
        let done = b.build(1, None).unwrap();
        assert_eq!(done.n_keys, 100);
        assert!(!pdir.join(ARRIVAL_BIN).exists(), "cleanup must finish");
        assert_eq!(
            fs::read(pdir.join(BLOCKS_BIN)).unwrap(),
            blocks_before,
            "skipped partition must not be rewritten"
        );
    }

    #[test]
    fn build_is_idempotent_and_finalize_works_after_skip() {
        let data = pairs(300, 100);
        let (dir, _) = build_snapshot(&data, 2, opts(4096));
        let pdir = partition_dir(dir.path(), 2, 0);
        let blocks_before = fs::read(pdir.join(BLOCKS_BIN)).unwrap();

        // Fresh builder, index.done exists -> sentinel-skip path.
        let b = make_builder(dir.path(), 2, opts(4096));
        let done = b.build(1, None).unwrap();
        assert_eq!(done.n_keys, 300);
        assert_eq!(fs::read(pdir.join(BLOCKS_BIN)).unwrap(), blocks_before);

        let stats = Stats {
            n_keys: done.n_keys,
            created_at: "2026-01-01T00:00:00Z".into(),
            scatter: None,
            index: None,
        };
        b.finalize(stats, None).unwrap();

        let json = fs::read_to_string(dir.path().join("meta.json")).unwrap();
        let meta: meta::Meta = serde_json::from_str(&json).unwrap();
        assert_eq!(meta.format_version, 5);
        assert_eq!(meta.block_size, 4096);
        assert_eq!(meta.inline_threshold, 2048);
        assert_eq!(meta.partitions.len(), 2);
        let fences_len = fs::metadata(pdir.join(FENCES_BIN)).unwrap().len();
        assert_eq!(meta.partitions[0].n_blocks, fences_len / 4);
        meta.layout().unwrap();
    }

    #[test]
    fn finalize_before_build_is_error() {
        let dir = TempDir::new().unwrap();
        let b = make_builder(dir.path(), 1, opts(4096));
        let stats = Stats {
            n_keys: 0,
            created_at: "2026-01-01T00:00:00Z".into(),
            scatter: None,
            index: None,
        };
        assert!(matches!(
            b.finalize(stats, None),
            Err(Error::FinalizeBeforeBuild)
        ));
    }

    #[test]
    fn auto_tune_small_records_resolve_to_4k() {
        let data = pairs(100, 100);
        let (dir, builder) = build_snapshot(&data, 1, V5Options::default());
        let guard = builder.done.lock().unwrap();
        assert_eq!(guard.as_ref().unwrap().block_size, 4096);
        drop(guard);
        assert_all_present(dir.path(), 1, 4096, &data);
    }

    #[test]
    fn auto_tune_from_histogram() {
        // All records in log2 bucket 13 (sizes 4096..8191): doubled and
        // clamped -> 16 KiB.
        let mut hist = vec![0u64; HIST_BUCKETS];
        hist[13] = 100;
        assert_eq!(auto_tune_block_size(&hist), 16 * 1024);
        // Tiny records clamp up to the 4 KiB floor.
        let mut hist = vec![0u64; HIST_BUCKETS];
        hist[5] = 100;
        assert_eq!(auto_tune_block_size(&hist), 4096);
        // Huge records clamp down to the 64 KiB auto cap.
        let mut hist = vec![0u64; HIST_BUCKETS];
        hist[20] = 100;
        assert_eq!(auto_tune_block_size(&hist), 64 * 1024);
        // Empty histogram (no keys) -> floor.
        assert_eq!(auto_tune_block_size(&vec![0u64; HIST_BUCKETS]), 4096);
    }

    #[test]
    fn invalid_block_size_override_rejected_at_construction() {
        let dir = TempDir::new().unwrap();
        let err = V5Builder::new(BuilderConfig {
            root: dir.path().to_path_buf(),
            n_partitions: 1,
            verify_seed: DEFAULT_VERIFY_SEED,
            data_buf_bytes: 1024,
            spill_buf_bytes: 1024,
            v5: opts(5000),
        });
        assert!(matches!(err, Err(Error::InvalidBlockSize(5000))));
    }

    #[test]
    fn sketch_decision_is_stable_across_resume() {
        // A sketchless build interrupted after plan() and resumed by a
        // process configured with the (default-on) sketch must stay
        // sketchless: the pinned plan wins, exactly like block_size.
        // Without the pin, a mixed partition set (some with sketch.bin,
        // some without) under meta.sketch = Some is unopenable.
        let dir = TempDir::new().unwrap();
        let b1 = make_builder(
            dir.path(),
            1,
            V5Options {
                block_size: Some(4096),
                sketch: false,
                ..Default::default()
            },
        );
        scatter(&b1, &pairs(50, 100), 1);
        b1.plan().unwrap();
        drop(b1);

        let b2 = make_builder(dir.path(), 1, opts(4096)); // sketch on by default
        b2.plan().unwrap();
        b2.build(1, None).unwrap();

        assert!(
            !partition_dir(dir.path(), 1, 0).join(SKETCH_BIN).exists(),
            "pinned sketchless plan must win over the resuming config"
        );
        let guard = b2.done.lock().unwrap();
        assert!(
            !guard.as_ref().unwrap().sketch,
            "sentinel must record what was built"
        );
    }

    #[test]
    fn plan_decision_is_stable_across_resume() {
        // The plan file pins block_size; a resumed process must reuse it
        // even though its own override differs (operator error) — the
        // on-disk decision is immutable.
        let dir = TempDir::new().unwrap();
        let b1 = make_builder(dir.path(), 1, opts(4096));
        scatter(&b1, &pairs(10, 100), 1);
        b1.plan().unwrap();
        drop(b1);

        let b2 = make_builder(dir.path(), 1, opts(8192));
        b2.plan().unwrap();
        let done = b2.build(1, None).unwrap();
        assert_eq!(done.n_keys, 10);
        let guard = b2.done.lock().unwrap();
        assert_eq!(guard.as_ref().unwrap().block_size, 4096);
    }
}
