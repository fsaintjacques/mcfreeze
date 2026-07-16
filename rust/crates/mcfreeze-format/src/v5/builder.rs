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
        compress::{self, Mode},
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
/// Root-level learned dictionary (zstd-dict mode): raw ZDICT output,
/// no framing. Ships with the snapshot — never swept.
const DICT_BIN: &str = "dict.bin";

const DEFAULT_BUCKET_BYTES: u64 = 128 * 1024 * 1024;
/// Dictionary sample pool: the leading bytes of partition 0's arrival
/// log. ZDICT wants ~100× the dictionary size (~6.4 MB for a 64 KiB
/// dictionary); 64 MB of headroom softens the prefix's time bias at a
/// one-off training cost that is noise next to the build.
const SAMPLE_PREFIX_BYTES: u64 = 64 * 1024 * 1024;
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
/// some without — an unopenable snapshot). Compression is pinned for
/// the same reason, and this file is the transient carrier `finalize`
/// copies into `meta.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Plan {
    block_size: u32,
    inline_threshold: u32,
    sketch: bool,
    /// Effective compression mode (after any training fallback).
    #[serde(default)]
    compression: Mode,
    #[serde(default = "default_min_compress_len")]
    min_compress_len: u32,
    /// `checksum32(dict.bin)`; `Some` iff mode is `zstd-dict`. The
    /// durability ordering (dict.bin fsynced before v5.plan publishes)
    /// makes a plan naming a checksum imply the dictionary is durable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dict_checksum: Option<u32>,
    /// Training provenance, carried into build stats: sample count,
    /// trained dictionary size, and whether a `zstd-dict` request fell
    /// back to plain `zstd` for lack of samples.
    #[serde(default)]
    n_samples: u64,
    #[serde(default)]
    dict_len: u64,
    #[serde(default)]
    dict_fallback: bool,
    /// Sample-measured compression: raw and stored bytes of the sample
    /// pool under the fresh codec (fallback rules and frame overhead
    /// included) — the ratio the stored-size auto-tune ran on.
    /// Divergence from the build-time achieved ratio is the signature
    /// of a biased (time-skewed) sample.
    #[serde(default)]
    sample_bytes_raw: u64,
    #[serde(default)]
    sample_bytes_stored: u64,
    /// The vendored libzstd that trained the dictionary (trained bytes
    /// are not reproducible across versions). `Some` iff compressing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    zstd_version: Option<String>,
}

fn default_min_compress_len() -> u32 {
    compress::DEFAULT_MIN_COMPRESS_LEN as u32
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
    /// Compression outcome: logical vs stored value bytes and how many
    /// records took each path. The achieved ratio is a first-class
    /// observable per partition, not something derived from `du`.
    #[serde(default)]
    pub value_bytes_raw: u64,
    #[serde(default)]
    pub value_bytes_stored: u64,
    #[serde(default)]
    pub n_compressed: u64,
    #[serde(default)]
    pub n_raw: u64,
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
    /// Compression provenance, copied from `v5.plan` (which does not
    /// survive the build) for `finalize` to place in `meta.json`.
    #[serde(default)]
    pub compression: Mode,
    #[serde(default)]
    pub min_compress_len: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dict_checksum: Option<u32>,
    #[serde(default)]
    pub n_samples: u64,
    #[serde(default)]
    pub dict_len: u64,
    #[serde(default)]
    pub dict_fallback: bool,
    /// Sample-measured bytes (plan-time) vs achieved bytes (build-time):
    /// divergence between the two ratios is the signature of a biased
    /// (time-skewed) sample.
    #[serde(default)]
    pub sample_bytes_raw: u64,
    #[serde(default)]
    pub sample_bytes_stored: u64,
    #[serde(default)]
    pub value_bytes_raw: u64,
    #[serde(default)]
    pub value_bytes_stored: u64,
    #[serde(default)]
    pub n_compressed: u64,
    #[serde(default)]
    pub n_raw: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zstd_version: Option<String>,
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
        // Enforce the format's 30-bit value limit at scatter time: an
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
    compression: Mode,
    min_compress_len: u32,
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
            compression: config.v5.compression,
            min_compress_len: config.v5.min_compress_len,
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
    /// block is cut). Otherwise: train the dictionary if requested,
    /// auto-tune `block_size` — from sampled **stored** sizes when
    /// compressing, from the merged raw scatter histograms otherwise —
    /// and persist before returning.
    fn ensure_plan(&self) -> Result<Plan> {
        let mut guard = self.plan.lock().expect("plan poisoned");
        if let Some(plan) = guard.as_ref() {
            return Ok(plan.clone());
        }

        let path = self.root.join(PLAN_FILE);
        let plan = if path.exists() {
            let plan: Plan = serde_json::from_str(&fs::read_to_string(&path)?)?;
            // Resume trusts the recorded checksum (COVER retraining is
            // not byte-stable across runs or libzstd versions). Under
            // the durability ordering — dict.bin fsynced before v5.plan
            // publishes — a missing or mismatched dictionary here is
            // genuine corruption, never a torn write; deleting v5.plan
            // and dict.bin retrains from scratch.
            if let Some(expected) = plan.dict_checksum {
                let dict = fs::read(self.root.join(DICT_BIN))
                    .map_err(|source| Error::DictMissing { expected, source })?;
                compress::verify_dict(&dict, expected)?;
            }
            plan
        } else {
            let mut hist = vec![0u64; HIST_BUCKETS];
            for p in 0..self.layout.n_partitions as usize {
                let stats_path = self.partition_dir(p).join(ARRIVAL_STATS);
                let stats: ArrivalStats = serde_json::from_str(&fs::read_to_string(stats_path)?)?;
                for (h, s) in hist.iter_mut().zip(stats.hist) {
                    *h += s;
                }
            }
            let mut plan = Plan {
                block_size: 0, // decided below, after sampling
                inline_threshold: 0,
                sketch: self.sketch,
                compression: self.compression,
                min_compress_len: self.min_compress_len,
                dict_checksum: None,
                n_samples: 0,
                dict_len: 0,
                dict_fallback: false,
                sample_bytes_raw: 0,
                sample_bytes_stored: 0,
                zstd_version: (self.compression != Mode::None).then(compress::zstd_version),
            };
            // A dict.bin orphaned by a crash before its v5.plan
            // published (or by an abandoned zstd-dict run) must not
            // outlive a plan that never references it.
            let _ = fs::remove_file(self.root.join(DICT_BIN));
            if self.compression != Mode::None {
                // Auto-tune must see stored sizes directly: scaling the
                // raw median by an average ratio mis-tunes whenever
                // compressibility correlates with size (it usually
                // does). Note the scope narrows with the substitution:
                // the raw histogram spans every partition, the stored
                // one only partition 0's sampled prefix — a hash-
                // unbiased but time-biased pool, the same tradeoff the
                // dictionary makes, observable via the recorded sample
                // stats. An empty sample pool (empty partition 0)
                // leaves the raw histogram in charge.
                let stored_hist = self.plan_compression(&mut plan)?;
                if plan.n_samples > 0 {
                    hist = stored_hist;
                }
            }
            plan.block_size = match self.block_size_override {
                Some(bs) => bs,
                None => auto_tune_block_size(&hist),
            };
            plan.inline_threshold = plan.block_size / 2;
            write_marker(&path, &serde_json::to_string(&plan)?, self.marker_mode)?;
            plan
        };
        *guard = Some(plan.clone());
        Ok(plan)
    }

    /// Compression planning: sample a prefix of partition 0's arrival
    /// log (partition routing is by hash, so any one partition is an
    /// unbiased sample of the key space — across keys; the prefix is
    /// biased across *time*, which the recorded sample stats make
    /// observable), train the `zstd-dict` dictionary if requested, then
    /// compress every sample with the fresh codec and return the
    /// stored-size histogram for the block-size rule.
    ///
    /// `dict.bin` is written durably **before** its checksum is pinned
    /// into the plan. Too few samples to train falls back to plain
    /// `zstd` with a warning, recorded in the plan.
    fn plan_compression(&self, plan: &mut Plan) -> Result<Vec<u64>> {
        // arrival.bin exists here by ordering: the histogram loop above
        // read every partition's arrival.stats, which the appender
        // publishes only after creating and syncing arrival.bin (empty
        // partitions included). An absent file is a mutilated tree and
        // fails loudly, exactly as build_partition would right after.
        let arrival = self.partition_dir(0).join(ARRIVAL_BIN);
        let samples = sample_arrival_prefix(&arrival, SAMPLE_PREFIX_BYTES)?;
        plan.n_samples = samples.len() as u64;

        let mut dict = None;
        if self.compression == Mode::ZstdDict {
            let trained = if samples.is_empty() {
                Err(Error::DictTrain(std::io::Error::other("no samples")))
            } else {
                compress::train_dict(&samples, compress::DEFAULT_DICT_LEN)
            };
            match trained {
                Ok(bytes) => {
                    // Data before marker: the dictionary is fsynced
                    // before v5.plan (and a fortiori meta.json) can
                    // name it.
                    write_data_file(&self.root.join(DICT_BIN), &bytes)?;
                    plan.dict_checksum = Some(checksum32(&bytes));
                    plan.dict_len = bytes.len() as u64;
                    dict = Some(bytes);
                }
                Err(e) => {
                    tracing::warn!(
                        n_samples = plan.n_samples,
                        error = %e,
                        "zstd-dict training failed; falling back to plain zstd"
                    );
                    plan.compression = Mode::Zstd;
                    plan.dict_fallback = true;
                }
            }
        }

        // Measure the samples under the effective codec (post-fallback),
        // exactly as build_partition will store them.
        let mut compressor = compress::Compressor::new(
            compress::DEFAULT_LEVEL,
            dict.as_deref(),
            plan.min_compress_len as usize,
        )?;
        let mut hist = vec![0u64; HIST_BUCKETS];
        for v in &samples {
            let stored = compressor.compress(v)?.map_or(v.len(), |f| f.len());
            let rec = encoded_len(stored) as u64;
            hist[(u64::BITS - rec.leading_zeros()) as usize] += 1;
            plan.sample_bytes_raw += v.len() as u64;
            plan.sample_bytes_stored += stored as u64;
        }
        Ok(hist)
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
        // The dictionary is shared read-only across build threads; each
        // partition digests its own compression context from it. Re-read
        // and re-verify here: in a resumed process the in-memory plan
        // came from disk, and the bytes about to be compiled into every
        // partition must be the ones the checksum pinned.
        let dict: Option<Vec<u8>> = match plan.dict_checksum {
            Some(expected) => {
                let bytes = fs::read(self.root.join(DICT_BIN))
                    .map_err(|source| Error::DictMissing { expected, source })?;
                compress::verify_dict(&bytes, expected)?;
                Some(bytes)
            }
            None => None,
        };
        let dict = dict.as_deref();
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
                        &plan,
                        dict,
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
            compression: plan.compression,
            min_compress_len: plan.min_compress_len,
            dict_checksum: plan.dict_checksum,
            n_samples: plan.n_samples,
            dict_len: plan.dict_len,
            dict_fallback: plan.dict_fallback,
            sample_bytes_raw: plan.sample_bytes_raw,
            sample_bytes_stored: plan.sample_bytes_stored,
            value_bytes_raw: partitions.iter().map(|p| p.value_bytes_raw).sum(),
            value_bytes_stored: partitions.iter().map(|p| p.value_bytes_stored).sum(),
            n_compressed: partitions.iter().map(|p| p.n_compressed).sum(),
            n_raw: partitions.iter().map(|p| p.n_raw).sum(),
            zstd_version: plan.zstd_version.clone(),
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
            compression: match done.compression {
                Mode::None => None,
                // `dict` derives from the checksum actually produced,
                // not the requested mode: training fallback downgrades
                // the mode, and the emitted section must be internally
                // coherent (layout() enforces it) either way.
                _ => Some(meta::CompressionMeta {
                    codec: meta::CODEC_ZSTD.to_string(),
                    dict: done.dict_checksum.is_some(),
                    dict_checksum: done.dict_checksum,
                    min_compress_len: done.min_compress_len,
                    zstd_version: done.zstd_version.clone(),
                }),
            },
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
    plan: &Plan,
    dict: Option<&[u8]>,
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
    let mut value_bytes_raw = 0u64;
    let mut value_bytes_stored = 0u64;
    let mut n_compressed = 0u64;
    let mut n_raw = 0u64;
    // One compression context per partition build: a CCtx is not
    // thread-safe, and creating it here digests the shared dictionary
    // once per partition instead of once per value.
    let mut compressor = match plan.compression {
        Mode::None => None,
        _ => Some(compress::Compressor::new(
            compress::DEFAULT_LEVEL,
            dict,
            plan.min_compress_len as usize,
        )?),
    };
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
            let frame = match compressor.as_mut() {
                Some(c) => c.compress(value)?,
                None => None,
            };
            let (stored, compressed) = match &frame {
                Some(frame) => (frame.as_slice(), true),
                None => (value, false),
            };
            value_bytes_raw += value.len() as u64;
            value_bytes_stored += stored.len() as u64;
            match compressed {
                true => n_compressed += 1,
                false => n_raw += 1,
            }
            // The inline/heap decision runs on the *stored* size —
            // that is what keeps fat-but-compressible values inline.
            if stored.len() as u32 <= plan.inline_threshold {
                asm.push_inline(f.fp, f.vfp, stored, compressed)?;
            } else {
                heap_out.write_all(stored)?;
                asm.push_stub(
                    f.fp,
                    f.vfp,
                    Stub {
                        heap_offset,
                        stored_len: stored.len() as u32,
                        value_checksum: checksum32(stored),
                    },
                    compressed,
                )?;
                heap_offset += stored.len() as u64;
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
        value_bytes_raw,
        value_bytes_stored,
        n_compressed,
        n_raw,
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

/// Dictionary sample pool: the values in the leading complete frames of
/// an arrival log's first `max_bytes`. The byte cut is not a frame
/// boundary in general, so the walk stops cleanly at the first frame
/// that overruns the prefix — unlike [`parse_frames`], a short tail
/// here is expected, not corruption. Empty values are skipped (they
/// teach ZDICT nothing).
fn sample_arrival_prefix(arrival: &Path, max_bytes: u64) -> Result<Vec<Vec<u8>>> {
    let mut buf = Vec::new();
    File::open(arrival)?.take(max_bytes).read_to_end(&mut buf)?;

    let mut samples = Vec::new();
    let mut pos = 0;
    while pos + FRAME_HEADER <= buf.len() {
        let len = u32::from_le_bytes(buf[pos + 16..pos + 20].try_into().unwrap()) as usize;
        let start = pos + FRAME_HEADER;
        let Some(end) = start.checked_add(len).filter(|&e| e <= buf.len()) else {
            break;
        };
        if len > 0 {
            samples.push(buf[start..end].to_vec());
        }
        pos = end;
    }
    Ok(samples)
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
    /// primitives: fences route, blocks scan, stubs follow into heap,
    /// compressed stored bytes decode through the snapshot's dictionary
    /// (`dict.bin`, when present).
    fn lookup(dir: &Path, n: u32, block_size: usize, key: &[u8]) -> Option<Vec<u8>> {
        let layout = Layout::new(n).unwrap();
        let fp = fingerprint(key);
        let vfp = verify_fingerprint(key, DEFAULT_VERIFY_SEED);
        let pdir = partition_dir(dir, n, layout.partition_of(fp));
        let codec = compress::Decompressor::new(fs::read(dir.join(DICT_BIN)).ok()).unwrap();

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
                Some(block::Record::Inline {
                    value, compressed, ..
                }) => {
                    return Some(
                        compress::decode(value, compressed, Some(&codec))
                            .unwrap()
                            .into_owned(),
                    )
                }
                Some(block::Record::Stub {
                    stub, compressed, ..
                }) => {
                    let heap = fs::read(pdir.join(HEAP_BIN)).unwrap();
                    let start = stub.heap_offset as usize;
                    let stored = &heap[start..start + stub.stored_len as usize];
                    assert_eq!(block::checksum32(stored), stub.value_checksum);
                    return Some(
                        compress::decode(stored, compressed, Some(&codec))
                            .unwrap()
                            .into_owned(),
                    );
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
                compression: Mode::None,
                min_compress_len: default_min_compress_len(),
                dict_checksum: None,
                n_samples: 0,
                dict_len: 0,
                dict_fallback: false,
                sample_bytes_raw: 0,
                sample_bytes_stored: 0,
                zstd_version: None,
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

    // -----------------------------------------------------------------
    // Dictionary lifecycle (doc/plan/V5_COMPRESSION.md stage 2)
    // -----------------------------------------------------------------

    fn dict_opts() -> V5Options {
        V5Options {
            block_size: Some(4096),
            compression: Mode::ZstdDict,
            ..Default::default()
        }
    }

    fn read_plan(root: &Path) -> Plan {
        serde_json::from_str(&fs::read_to_string(root.join(PLAN_FILE)).unwrap()).unwrap()
    }

    #[test]
    fn plan_trains_writes_and_pins_dictionary() {
        let data = pairs(2000, 100);
        let dir = TempDir::new().unwrap();
        let builder = make_builder(dir.path(), 2, dict_opts());
        scatter(&builder, &data, 2);
        builder.plan().unwrap();

        let dict = fs::read(dir.path().join(DICT_BIN)).unwrap();
        assert!(!dict.is_empty());
        let plan = read_plan(dir.path());
        assert_eq!(plan.compression, Mode::ZstdDict);
        assert_eq!(plan.dict_checksum, Some(block::checksum32(&dict)));
        assert_eq!(plan.dict_len, dict.len() as u64);
        assert!(plan.n_samples > 0);
        assert!(!plan.dict_fallback);
        assert!(plan.zstd_version.is_some());
    }

    #[test]
    fn resume_reuses_pinned_dictionary() {
        let data = pairs(2000, 100);
        let dir = TempDir::new().unwrap();
        {
            let b1 = make_builder(dir.path(), 1, dict_opts());
            scatter(&b1, &data, 1);
            b1.plan().unwrap();
        }
        let dict_before = fs::read(dir.path().join(DICT_BIN)).unwrap();
        let plan_before = fs::read_to_string(dir.path().join(PLAN_FILE)).unwrap();

        // Fresh process: the pinned plan wins; no retraining (COVER is
        // not assumed byte-stable, so retraining would break the pin).
        let b2 = make_builder(dir.path(), 1, dict_opts());
        b2.plan().unwrap();
        assert_eq!(fs::read(dir.path().join(DICT_BIN)).unwrap(), dict_before);
        assert_eq!(
            fs::read_to_string(dir.path().join(PLAN_FILE)).unwrap(),
            plan_before
        );
    }

    #[test]
    fn corrupt_dict_on_resume_fails_loudly() {
        let data = pairs(2000, 100);
        let dir = TempDir::new().unwrap();
        {
            let b1 = make_builder(dir.path(), 1, dict_opts());
            scatter(&b1, &data, 1);
            b1.plan().unwrap();
        }
        let dict_path = dir.path().join(DICT_BIN);
        let mut dict = fs::read(&dict_path).unwrap();
        let mid = dict.len() / 2;
        dict[mid] ^= 0xFF;
        fs::write(&dict_path, &dict).unwrap();

        let b2 = make_builder(dir.path(), 1, dict_opts());
        assert!(matches!(b2.plan(), Err(Error::DictChecksumMismatch { .. })));
    }

    #[test]
    fn missing_dict_on_resume_fails_loudly() {
        // Under the data-before-marker ordering a v5.plan naming a
        // dictionary implies dict.bin is durable — its absence is
        // corruption, not a state to silently retrain over.
        let data = pairs(2000, 100);
        let dir = TempDir::new().unwrap();
        {
            let b1 = make_builder(dir.path(), 1, dict_opts());
            scatter(&b1, &data, 1);
            b1.plan().unwrap();
        }
        fs::remove_file(dir.path().join(DICT_BIN)).unwrap();

        let b2 = make_builder(dir.path(), 1, dict_opts());
        assert!(matches!(b2.plan(), Err(Error::DictMissing { .. })));
    }

    #[test]
    fn deleting_plan_and_dict_retrains_from_scratch() {
        let data = pairs(2000, 100);
        let dir = TempDir::new().unwrap();
        {
            let b1 = make_builder(dir.path(), 1, dict_opts());
            scatter(&b1, &data, 1);
            b1.plan().unwrap();
        }
        // The documented recovery for dictionary corruption.
        fs::remove_file(dir.path().join(PLAN_FILE)).unwrap();
        fs::remove_file(dir.path().join(DICT_BIN)).unwrap();

        let b2 = make_builder(dir.path(), 1, dict_opts());
        b2.plan().unwrap();
        let dict = fs::read(dir.path().join(DICT_BIN)).unwrap();
        assert_eq!(
            read_plan(dir.path()).dict_checksum,
            Some(block::checksum32(&dict))
        );
    }

    #[test]
    fn torn_dict_without_plan_is_overwritten_by_retraining() {
        // Crash after dict.bin, before v5.plan: the orphan is garbage
        // (never referenced) and the fresh derivation replaces it.
        let data = pairs(2000, 100);
        let dir = TempDir::new().unwrap();
        let builder = make_builder(dir.path(), 1, dict_opts());
        scatter(&builder, &data, 1);
        fs::write(dir.path().join(DICT_BIN), b"torn garbage").unwrap();

        builder.plan().unwrap();
        let dict = fs::read(dir.path().join(DICT_BIN)).unwrap();
        assert_ne!(dict, b"torn garbage");
        assert_eq!(
            read_plan(dir.path()).dict_checksum,
            Some(block::checksum32(&dict))
        );
    }

    #[test]
    fn stale_dict_removed_when_plan_does_not_compress() {
        let data = pairs(10, 100);
        let dir = TempDir::new().unwrap();
        let builder = make_builder(dir.path(), 1, opts(4096));
        scatter(&builder, &data, 1);
        fs::write(dir.path().join(DICT_BIN), b"abandoned").unwrap();

        builder.plan().unwrap();
        assert!(
            !dir.path().join(DICT_BIN).exists(),
            "a plan that never references dict.bin must not leave one in the snapshot"
        );
    }

    #[test]
    fn too_few_samples_falls_back_to_plain_zstd() {
        // Empty partition: no samples at all — the deterministic floor
        // of the too-few-samples spectrum.
        let dir = TempDir::new().unwrap();
        let builder = make_builder(dir.path(), 1, dict_opts());
        scatter(&builder, &[], 1);
        builder.plan().unwrap();

        let plan = read_plan(dir.path());
        assert_eq!(plan.compression, Mode::Zstd);
        assert!(plan.dict_fallback);
        assert_eq!(plan.n_samples, 0);
        assert_eq!(plan.dict_checksum, None);
        assert!(!dir.path().join(DICT_BIN).exists());
    }

    #[test]
    fn build_carries_compression_into_index_done_and_dict_survives() {
        let data = pairs(2000, 100);
        let dir = TempDir::new().unwrap();
        let builder = make_builder(dir.path(), 2, dict_opts());
        scatter(&builder, &data, 2);
        builder.plan().unwrap();
        builder.build(2, None).unwrap();

        // v5.plan is gone; its compression decision must survive in the
        // phase sentinel for finalize.
        assert!(!dir.path().join(PLAN_FILE).exists());
        let done: IndexDone =
            serde_json::from_str(&fs::read_to_string(dir.path().join(INDEX_DONE)).unwrap())
                .unwrap();
        assert_eq!(done.compression, Mode::ZstdDict);
        assert!(done.dict_checksum.is_some());
        assert!(done.n_samples > 0);
        assert!(done.zstd_version.is_some());

        // dict.bin ships with the snapshot: the transient sweep must
        // not touch it.
        let dict = fs::read(dir.path().join(DICT_BIN)).unwrap();
        assert_eq!(done.dict_checksum, Some(block::checksum32(&dict)));
    }

    // -----------------------------------------------------------------
    // Emit-time compression (doc/plan/V5_COMPRESSION.md stage 3)
    // -----------------------------------------------------------------

    /// Deterministic incompressible bytes without a rand dependency.
    fn noise(len: usize, seed: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        let mut h = seed | 1;
        while out.len() < len {
            h = xxhash_rust::xxh64::xxh64(&h.to_le_bytes(), 0);
            out.extend_from_slice(&h.to_le_bytes());
        }
        out.truncate(len);
        out
    }

    #[test]
    fn zstd_dict_snapshot_roundtrips_and_reports_ratio() {
        let data = pairs(2000, 100);
        let dir = TempDir::new().unwrap();
        let builder = make_builder(dir.path(), 2, dict_opts());
        scatter(&builder, &data, 2);
        builder.plan().unwrap();
        builder.build(2, None).unwrap();
        assert_all_present(dir.path(), 2, 4096, &data);

        let guard = builder.done.lock().unwrap();
        let done = guard.as_ref().unwrap();
        assert_eq!(done.n_compressed + done.n_raw, 2000);
        assert!(done.n_compressed > 0, "dictionary must win on these values");
        assert!(done.value_bytes_stored < done.value_bytes_raw);
        assert!(done.sample_bytes_stored < done.sample_bytes_raw);
        // Aggregates are exactly the per-partition sums.
        assert_eq!(
            done.value_bytes_stored,
            done.partitions
                .iter()
                .map(|p| p.value_bytes_stored)
                .sum::<u64>()
        );
        assert_eq!(
            done.n_compressed,
            done.partitions.iter().map(|p| p.n_compressed).sum::<u64>()
        );
    }

    #[test]
    fn plain_zstd_snapshot_roundtrips() {
        // ≥1 KiB repetitive values: the tier plain zstd exists for.
        let data = pairs(300, 2000);
        let dir = TempDir::new().unwrap();
        let v5 = V5Options {
            block_size: Some(4096),
            compression: Mode::Zstd,
            ..Default::default()
        };
        let builder = make_builder(dir.path(), 1, v5);
        scatter(&builder, &data, 1);
        builder.plan().unwrap();
        builder.build(1, None).unwrap();
        assert_all_present(dir.path(), 1, 4096, &data);

        let guard = builder.done.lock().unwrap();
        let done = guard.as_ref().unwrap();
        assert!(done.n_compressed > 0);
        assert!(!dir.path().join(DICT_BIN).exists());
    }

    #[test]
    fn incompressible_values_store_identical_to_none_mode() {
        // Storage never expands: when nothing wins, a zstd-mode build's
        // partition artifacts are byte-identical to a none-mode build.
        let data: Vec<(Vec<u8>, Vec<u8>)> = (0..200)
            .map(|i| (format!("key-{i}").into_bytes(), noise(120, i)))
            .collect();
        let build = |compression| {
            let dir = TempDir::new().unwrap();
            let v5 = V5Options {
                block_size: Some(4096),
                compression,
                ..Default::default()
            };
            let builder = make_builder(dir.path(), 1, v5);
            scatter(&builder, &data, 1);
            builder.plan().unwrap();
            builder.build(1, None).unwrap();
            dir
        };
        let none = build(Mode::None);
        let zstd = build(Mode::Zstd);

        let done: IndexDone =
            serde_json::from_str(&fs::read_to_string(zstd.path().join(INDEX_DONE)).unwrap())
                .unwrap();
        assert_eq!(done.n_compressed, 0);
        assert_eq!(done.value_bytes_stored, done.value_bytes_raw);
        for f in [BLOCKS_BIN, HEAP_BIN, FENCES_BIN] {
            assert_eq!(
                fs::read(partition_dir(none.path(), 1, 0).join(f)).unwrap(),
                fs::read(partition_dir(zstd.path(), 1, 0).join(f)).unwrap(),
                "{f} must be byte-identical when no value compresses"
            );
        }
    }

    #[test]
    fn fat_but_compressible_values_stay_inline() {
        // Raw 3000 B (over the 2048 threshold), stored well under it:
        // the inline/heap decision runs on stored size, so no stubs.
        let data = pairs(200, 3000);
        let dir = TempDir::new().unwrap();
        let v5 = V5Options {
            block_size: Some(4096),
            compression: Mode::Zstd,
            ..Default::default()
        };
        let builder = make_builder(dir.path(), 1, v5);
        scatter(&builder, &data, 1);
        builder.plan().unwrap();
        builder.build(1, None).unwrap();
        assert_all_present(dir.path(), 1, 4096, &data);

        let guard = builder.done.lock().unwrap();
        let done = guard.as_ref().unwrap();
        assert_eq!(done.n_compressed, 200);
        assert_eq!(done.n_stubs, 0, "stored-size threshold keeps these inline");
        assert_eq!(done.heap_bytes, 0);
    }

    #[test]
    fn auto_tune_runs_on_stored_sizes() {
        // Raw ~6 KiB records auto-tune to 16 KiB blocks; stored (a few
        // dozen bytes after compression) must tune to the 4 KiB floor.
        // Scaling the raw median by an average ratio could get this
        // right too — the point is the stored histogram, measured with
        // frame overhead and fallback rules included.
        let data = pairs(500, 6000);
        let dir = TempDir::new().unwrap();
        let v5 = V5Options {
            compression: Mode::Zstd,
            ..Default::default()
        };
        let builder = make_builder(dir.path(), 1, v5);
        scatter(&builder, &data, 1);
        builder.plan().unwrap();

        let plan = read_plan(dir.path());
        assert_eq!(plan.block_size, 4096);
        assert!(plan.sample_bytes_stored < plan.sample_bytes_raw / 10);

        // Control: the same data without compression tunes larger.
        let dir2 = TempDir::new().unwrap();
        let b2 = make_builder(dir2.path(), 1, V5Options::default());
        scatter(&b2, &data, 1);
        b2.plan().unwrap();
        assert_eq!(read_plan(dir2.path()).block_size, 16 * 1024);
    }

    #[test]
    fn dict_fallback_still_measures_and_builds_with_plain_zstd() {
        // A sample pool far too small for ZDICT (one tiny value): the
        // plan must fall back to plain zstd AND still measure the
        // samples under the dictless codec it fell back to — then the
        // build must complete in that mode.
        let data = vec![(b"only-key".to_vec(), b"o".repeat(500))];
        let dir = TempDir::new().unwrap();
        let builder = make_builder(dir.path(), 1, dict_opts());
        scatter(&builder, &data, 1);
        builder.plan().unwrap();

        let plan = read_plan(dir.path());
        assert_eq!(plan.compression, Mode::Zstd);
        assert!(plan.dict_fallback);
        assert_eq!(plan.n_samples, 1);
        assert_eq!(plan.dict_checksum, None);
        assert!(!dir.path().join(DICT_BIN).exists());
        // Dictless framing measured: 500 repeated bytes compress.
        assert_eq!(plan.sample_bytes_raw, 500);
        assert!(plan.sample_bytes_stored > 0 && plan.sample_bytes_stored < 500);

        builder.build(1, None).unwrap();
        assert_all_present(dir.path(), 1, 4096, &data);
        let guard = builder.done.lock().unwrap();
        let done = guard.as_ref().unwrap();
        assert_eq!(done.compression, Mode::Zstd);
        assert_eq!(done.n_compressed, 1);
    }

    #[test]
    fn empty_partition_zero_leaves_raw_histogram_in_charge() {
        // All keys route to partition 1: the stored-size sample pool is
        // empty, so block sizing must fall back to the merged raw
        // histogram (16 KiB for ~6 KiB records), not the 4 KiB floor an
        // all-zero stored histogram would produce.
        let layout = Layout::new(2).unwrap();
        let data: Vec<(Vec<u8>, Vec<u8>)> = (0..200)
            .map(|i| format!("key-{i}").into_bytes())
            .filter(|k| layout.partition_of(fingerprint(k)) == 1)
            .map(|k| {
                let v = format!("{}-{}", String::from_utf8_lossy(&k), "x".repeat(6000));
                (k, v.into_bytes())
            })
            .collect();
        assert!(!data.is_empty());

        let dir = TempDir::new().unwrap();
        let v5 = V5Options {
            compression: Mode::Zstd,
            ..Default::default()
        };
        let builder = make_builder(dir.path(), 2, v5);
        scatter(&builder, &data, 2);
        builder.plan().unwrap();

        let plan = read_plan(dir.path());
        assert_eq!(plan.n_samples, 0);
        assert_eq!(plan.block_size, 16 * 1024);
    }

    #[test]
    fn sample_prefix_stops_at_incomplete_tail_and_skips_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(ARRIVAL_BIN);
        let mut buf = Vec::new();
        let mut frame = |value: &[u8]| {
            buf.extend_from_slice(&1u64.to_le_bytes()); // fp
            buf.extend_from_slice(&2u64.to_le_bytes()); // vfp
            buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
            buf.extend_from_slice(value);
        };
        frame(b"first value");
        frame(b""); // teaches ZDICT nothing; skipped
        frame(b"second value");
        fs::write(&path, &buf).unwrap();

        // Whole file: both non-empty values.
        let samples = sample_arrival_prefix(&path, u64::MAX).unwrap();
        assert_eq!(
            samples,
            vec![b"first value".to_vec(), b"second value".to_vec()]
        );

        // Prefix cutting the last frame mid-value: clean stop, no error
        // — unlike parse_frames, a short tail here is expected.
        let samples = sample_arrival_prefix(&path, (buf.len() - 3) as u64).unwrap();
        assert_eq!(samples, vec![b"first value".to_vec()]);

        // Prefix cutting a header mid-way: same.
        let samples = sample_arrival_prefix(&path, (FRAME_HEADER + 11 + 5) as u64).unwrap();
        assert_eq!(samples, vec![b"first value".to_vec()]);
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
