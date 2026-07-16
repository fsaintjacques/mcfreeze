// SPDX-License-Identifier: Apache-2.0

//! The `FormatBuilder` seam: format-erased snapshot construction.
//!
//! The loader owns orchestration — sources, fingerprint derivation and
//! partition routing, scatter concurrency, progress, the phase-sentinel
//! protocol — and drives one of these per load. What varies per format is
//! what gets written per record and what "build the index phase" means;
//! that is this trait. See `doc/plan/FORMAT_INTERFACE.md`.

use std::path::PathBuf;
use std::sync::Arc;

use crate::{desc::FormatId, meta::Stats, v4, v5, Result};

/// Per-partition scatter sink. One appender exists per partition, driven
/// from a single blocking task — no locking on the hot path.
pub trait PartitionAppender: Send {
    /// Append one record. `fp` is the loader-derived full fingerprint;
    /// storage details (verify fingerprint, alignment, transient files)
    /// belong to the format.
    fn append(&mut self, key: &[u8], fp: u64, value: &[u8]) -> Result<()>;

    /// Flush and seal this partition's transient scatter state.
    fn finish(self: Box<Self>) -> Result<()>;
}

/// Result of the format's build phase, for the loader's stats plumbing.
/// The format's detailed build statistics live in its `index.done`
/// sentinel (opaque JSON), which the loader embeds into `meta.json`.
pub struct BuildDone {
    pub n_keys: u64,
}

/// What a [`FormatBuilder::scatter_probe`] found for one partition.
pub struct ScatterProbe {
    pub n_keys: u64,
    pub data_bytes: u64,
}

/// Format-erased snapshot construction. `Send + Sync`: the loader shares
/// it across scatter tasks and the build pool via `Arc`.
pub trait FormatBuilder: Send + Sync {
    fn format(&self) -> FormatId;

    /// Create partition `p`'s scatter sink. Called once per partition at
    /// scatter start; never called for a resumed (already-scattered) run.
    fn appender(&self, p: usize) -> Result<Box<dyn PartitionAppender>>;

    /// Crash-recovery probe: `Some` if partition `p`'s transient scatter
    /// state (or final artifacts) prove scatter completed for it, `None`
    /// otherwise. Key count may be 0 when only final artifacts remain and
    /// the count is unknowable without the phase sentinel.
    fn scatter_probe(&self, p: usize) -> Result<Option<ScatterProbe>>;

    /// Barrier between scatter and build. Global, whole-snapshot
    /// decisions belong here (a future format derives `block_size` from
    /// the observed size distribution). V4: no-op.
    fn plan(&self) -> Result<()>;

    /// Run the entire index phase: per-partition builds (bounded by
    /// `parallelism`), cross-partition artifacts, the format's
    /// `index.done` sentinel (idempotent: skipped if present), and
    /// transient-state cleanup. Blocking; the loader calls it from a
    /// blocking context. `progress` is invoked `(1, 0)` per partition.
    fn build(
        &self,
        parallelism: usize,
        progress: Option<Arc<dyn Fn(u64, u64) + Send + Sync>>,
    ) -> Result<BuildDone>;

    /// Write `meta.json` — last, as the snapshot completeness sentinel.
    fn finalize(&self, stats: Stats, encoding: Option<serde_json::Value>) -> Result<()>;
}

/// Construction parameters shared by every format.
pub struct BuilderConfig {
    pub root: PathBuf,
    pub n_partitions: u32,
    pub verify_seed: u64,
    /// `BufWriter` capacity for each partition's primary data stream.
    pub data_buf_bytes: usize,
    /// `BufWriter` capacity for each partition's secondary stream
    /// (V4: spill.bin).
    pub spill_buf_bytes: usize,
    /// V5-specific knobs; ignored by other formats.
    pub v5: V5Options,
}

/// V5-specific construction knobs (`doc/plan/FORMAT_V5_SPARSE_INDEX.md`).
#[derive(Debug, Clone)]
pub struct V5Options {
    /// Override the auto-tuned `block_size`. Power of two, ≥ 4 KiB.
    pub block_size: Option<u32>,
    /// Target radix bucket size in bytes (default 128 MiB). Peak build
    /// RAM ≈ `index_parallelism × (bucket_bytes + sketch build cost)`,
    /// where the sketch (when on, the default) transiently needs
    /// ~20 B × keys-per-partition — the filter is constructed from the
    /// partition's full fingerprint set, independent of `bucket_bytes`.
    pub bucket_bytes: Option<u64>,
    /// Build a per-partition binary-fuse-8 sketch (~9 bits/key,
    /// < 0.4% false-positive rate) restoring zero-I/O misses.
    /// **On by default**: free misses are the V4 behavior operators
    /// depend on; disable only for hit-dominated workloads where the
    /// sketch RAM buys nothing.
    pub sketch: bool,
    /// How completion markers are published (see [`MarkerMode`]).
    pub marker_mode: MarkerMode,
}

impl Default for V5Options {
    fn default() -> Self {
        Self {
            block_size: None,
            bucket_bytes: None,
            sketch: true,
            marker_mode: MarkerMode::Rename,
        }
    }
}

/// How completion markers (`arrival.stats`, `v5.plan`, `build.done`,
/// `index.done`, `meta.json`) are made atomic. Marker *presence* is a
/// completion signal, so a marker must never exist truncated — but the
/// mechanism that guarantees that differs by storage backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MarkerMode {
    /// POSIX filesystems: write + fsync a temp sibling, then rename it
    /// into place. The default.
    #[default]
    Rename,
    /// Object-store FUSE mounts (gcsfuse, mountpoint-s3): plain
    /// create + write + close. The object materializes atomically on
    /// close, and `rename(2)` on these mounts is unsupported or
    /// non-atomic — the temp+rename dance would *reintroduce* the torn-
    /// marker hazard it exists to prevent.
    DirectWrite,
}

/// Instantiate the builder for `format`. The single dispatch point on the
/// write path, mirroring `SnapshotDesc::load` on the read path.
pub fn builder_for(format: FormatId, config: BuilderConfig) -> Result<Arc<dyn FormatBuilder>> {
    match format {
        FormatId::V4 => Ok(Arc::new(v4::builder::V4Builder::new(config)?)),
        FormatId::V5 => Ok(Arc::new(v5::builder::V5Builder::new(config)?)),
    }
}
