// SPDX-License-Identifier: Apache-2.0

//! The `Snapshot` facade: format-erased read access to a snapshot.
//!
//! Contracts every format behind this facade honors
//! (`doc/plan/FORMAT_INTERFACE.md`):
//!
//! 1. [`Snapshot::get`] is sync and thread-safe; it may block on I/O and
//!    callers own offloading (`spawn_blocking`).
//! 2. Two stages, each total: [`SnapshotDesc::load`] does all metadata
//!    parsing; [`Snapshot::open`] does all residency work and returns a
//!    reader at steady-state latency. No warm-up step, no lazy loading.
//!    Where the platform cannot report residency (no
//!    `MADV_POPULATE_READ`), open falls back to lazy faulting rather
//!    than failing — the guarantee is "fails loudly wherever failure is
//!    observable", not stronger than the kernel can promise.
//! 3. Dropping a `Snapshot` releases everything — mmaps, owned memory,
//!    file descriptors. Teardown is release-only and infallible, and may
//!    be expensive (page-table teardown); drop final references from a
//!    blocking context.

use std::path::PathBuf;

use crate::{
    desc::{SnapshotDesc, VersionedMeta},
    v4, v5, Result,
};

/// Result of a successful (non-Err) lookup.
///
/// Cost-defined, not mechanism-defined, so it means the same thing for
/// every format behind the facade:
///
/// - `Miss { io: false }` — absence concluded without touching disk.
/// - `Miss { io: true }` — one or more `pread`s were paid to conclude
///   absence. V4 reaches this through a compact-fingerprint collision
///   (expected rate ≈ 0; a sustained rise signals a hashing anomaly).
///   Formats with probabilistic filters reach it at their configured
///   false-positive rate. Alert on divergence from
///   [`Snapshot::expected_miss_io_rate`], not on absolute counts.
#[derive(Debug, PartialEq, Eq)]
pub enum GetOutcome {
    Hit(Vec<u8>),
    Miss { io: bool },
}

/// Process-local choices about *how* to open, as opposed to *what/where*
/// (the [`SnapshotDesc`]). Deliberately empty for now: the extension slot
/// for future knobs (huge-page policy, mlock, derived search structures)
/// that must not live in `meta.json`.
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct OpenOptions {}

/// An opened snapshot, ready to serve at steady-state latency.
///
/// The concrete format is erased behind a closed set: dispatch happened
/// once, when [`SnapshotDesc::load`] parsed `format_version`.
pub struct Snapshot {
    desc: SnapshotDesc,
    inner: Inner,
}

enum Inner {
    V4(v4::reader::SnapshotReader),
    V5(v5::reader::SnapshotReader),
}

impl Snapshot {
    /// Perform all residency work for the described snapshot (V4: mmap
    /// `index.all`, advise, and synchronously populate the page cache).
    ///
    /// Blocking and potentially heavy — call from a blocking context.
    /// Residency failures are fatal where the platform can report them
    /// (a failed `MADV_POPULATE_READ` fails the open); platforms without
    /// the hint (non-Linux, kernels < 5.14) fall back to lazy faulting,
    /// so the steady-state guarantee is as strong as the kernel allows,
    /// never silently weaker than reported.
    pub fn open(desc: SnapshotDesc, _opts: &OpenOptions) -> Result<Self> {
        let inner = match desc.meta() {
            VersionedMeta::V4(m) => Inner::V4(v4::reader::SnapshotReader::open(desc.root(), m)?),
            VersionedMeta::V5(m) => Inner::V5(v5::reader::SnapshotReader::open(desc.root(), m)?),
        };
        Ok(Self { desc, inner })
    }

    /// Convenience: [`SnapshotDesc::load`] + default options.
    pub fn open_path(root: impl Into<PathBuf>) -> Result<Self> {
        Self::open(SnapshotDesc::load(root)?, &OpenOptions::default())
    }

    /// Look up `key`. Sync; may block on `pread` — callers offload.
    pub fn get(&self, key: &[u8]) -> Result<GetOutcome> {
        match &self.inner {
            Inner::V4(r) => r.get(key),
            Inner::V5(r) => r.get(key),
        }
    }

    pub fn desc(&self) -> &SnapshotDesc {
        &self.desc
    }

    /// Expected fraction of misses that pay I/O for this snapshot's
    /// format and configuration. V4: ≈0 (32-bit fingerprint collisions
    /// only). V5 without a sketch: ≈1 (nearly every miss scans a
    /// block); with the binary-fuse-8 sketch: its ~0.4% false-positive
    /// rate. Exported as `mcf_expected_miss_io_rate`; dashboards
    /// compare the observed `miss_io / (miss + miss_io)` ratio against
    /// it.
    pub fn expected_miss_io_rate(&self) -> f64 {
        match &self.inner {
            Inner::V4(_) => 0.0,
            Inner::V5(r) => r.expected_miss_io_rate(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v4::writer::SnapshotWriter;
    use tempfile::TempDir;

    #[test]
    fn open_path_roundtrip() {
        let dir = TempDir::new().unwrap();
        let mut w = SnapshotWriter::new(dir.path(), 4).unwrap();
        w.write(b"k", b"v").unwrap();
        w.finish().unwrap();

        let snap = Snapshot::open_path(dir.path()).unwrap();
        assert_eq!(snap.desc().format(), crate::FormatId::V4);
        match snap.get(b"k").unwrap() {
            GetOutcome::Hit(v) => assert_eq!(v, b"v"),
            other => panic!("expected hit, got {other:?}"),
        }
        assert_eq!(snap.get(b"absent").unwrap(), GetOutcome::Miss { io: false });
    }

    #[test]
    fn open_missing_dir_fails() {
        assert!(Snapshot::open_path("/nonexistent/snapshot").is_err());
    }

    #[test]
    fn valid_desc_but_missing_data_fails_at_open() {
        // The failure mode the two-stage split exists to distinguish:
        // metadata parses fine, residency work cannot complete.
        let dir = TempDir::new().unwrap();
        let mut w = SnapshotWriter::new(dir.path(), 1).unwrap();
        w.write(b"k", b"v").unwrap();
        w.finish().unwrap();

        let desc = SnapshotDesc::load(dir.path()).unwrap();
        std::fs::remove_file(dir.path().join("index.all")).unwrap();

        assert!(Snapshot::open(desc, &OpenOptions::default()).is_err());
    }
}
