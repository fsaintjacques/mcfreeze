// SPDX-License-Identifier: Apache-2.0

//! Snapshot description: the cheap stage of opening a snapshot.
//!
//! [`SnapshotDesc::load`] reads and parses `meta.json` only — no data
//! file descriptors, no mmaps, no residency work. All format-metadata
//! parsing and validation lives here; `Snapshot::open` consumes the
//! result and cannot fail on syntax. Because `meta.json` is written
//! last during construction, a successful `load` is also the
//! completeness check for the snapshot directory.
//!
//! See `doc/plan/FORMAT_INTERFACE.md`.

use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::{
    meta::{self, Stats},
    Error, Result,
};

// ---------------------------------------------------------------------------
// FormatId
// ---------------------------------------------------------------------------

/// Stable identifier for an on-disk snapshot format.
///
/// Serializes as its `as_str` form (`"v4"`); used in transient phase
/// sentinels (`scatter.done`) so resume paths can detect a `--format`
/// mismatch before touching any data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FormatId {
    V4,
    V5,
}

impl Default for FormatId {
    fn default() -> Self {
        FormatId::DEFAULT
    }
}

impl FormatId {
    /// The format `mcf load` writes when `--format` is not given.
    pub const DEFAULT: FormatId = FormatId::V4;

    /// Every format this binary knows *end to end*. Single source of
    /// truth for `FromStr`, error messages, and conformance coverage.
    /// V5 joins once its reader lands behind the `Snapshot` facade —
    /// until then the builder exists but the format is not selectable.
    pub const ALL: &'static [FormatId] = &[FormatId::V4];

    pub fn as_str(&self) -> &'static str {
        match self {
            FormatId::V4 => "v4",
            FormatId::V5 => "v5",
        }
    }

    fn expected_list() -> String {
        Self::ALL
            .iter()
            .map(FormatId::as_str)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl std::fmt::Display for FormatId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for FormatId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        FormatId::ALL
            .iter()
            .find(|id| id.as_str() == s)
            .copied()
            .ok_or_else(|| Error::UnknownFormat {
                got: s.to_string(),
                expected: FormatId::expected_list(),
            })
    }
}

// ---------------------------------------------------------------------------
// SnapshotDesc
// ---------------------------------------------------------------------------

/// First stage of the two-stage `meta.json` parse: read only the
/// version tag, then commit to the matching payload type.
#[derive(Deserialize)]
struct VersionProbe {
    format_version: u32,
}

/// Parsed per-version metadata. Private: consumers see only the
/// [`SnapshotDesc`] accessors, so format-internal fields never leak
/// into server or CLI code.
#[derive(Debug, Clone)]
pub(crate) enum VersionedMeta {
    V4(meta::Meta),
}

/// Parsed, validated description of a snapshot directory.
///
/// Cheap to construct: reads and parses `meta.json` only. Holds no file
/// descriptors and no mappings; dropping it releases nothing but heap.
#[derive(Debug, Clone)]
pub struct SnapshotDesc {
    root: PathBuf,
    meta: VersionedMeta,
}

impl SnapshotDesc {
    /// Read and parse `<root>/meta.json`.
    ///
    /// # Errors
    /// - [`Error::Io`] when `meta.json` is missing or unreadable — for a
    ///   freshly written snapshot this doubles as "not complete yet",
    ///   since `meta.json` is written last.
    /// - [`Error::Json`] on malformed JSON.
    /// - [`Error::UnsupportedFormatVersion`] when the version tag names
    ///   a format this binary does not know.
    /// - Payload validation errors (hash algorithm, partition count).
    pub fn load(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        let json = fs::read_to_string(root.join("meta.json"))?;

        let probe: VersionProbe = serde_json::from_str(&json)?;
        let meta = match probe.format_version {
            meta::FORMAT_VERSION => {
                let m: meta::Meta = serde_json::from_str(&json)?;
                m.layout()?; // validates hash algorithm + partition count
                VersionedMeta::V4(m)
            }
            v => return Err(Error::UnsupportedFormatVersion(v)),
        };

        Ok(Self { root, meta })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn format(&self) -> FormatId {
        match &self.meta {
            VersionedMeta::V4(_) => FormatId::V4,
        }
    }

    /// Build-time statistics, if the builder recorded them.
    pub fn stats(&self) -> Option<&Stats> {
        match &self.meta {
            VersionedMeta::V4(m) => m.stats.as_ref(),
        }
    }

    /// Encoding metadata (e.g. protobuf descriptor), if present.
    pub fn encoding(&self) -> Option<&serde_json::Value> {
        match &self.meta {
            VersionedMeta::V4(m) => m.encoding.as_ref(),
        }
    }

    /// Number of partitions in the snapshot.
    pub fn n_partitions(&self) -> u32 {
        match &self.meta {
            VersionedMeta::V4(m) => m.partitions.len() as u32,
        }
    }

    pub(crate) fn meta(&self) -> &VersionedMeta {
        &self.meta
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v4::writer::SnapshotWriter;
    use tempfile::TempDir;

    fn v4_snapshot() -> TempDir {
        let dir = TempDir::new().unwrap();
        let mut w = SnapshotWriter::new(dir.path(), 4).unwrap();
        w.write(b"key", b"value").unwrap();
        w.finish().unwrap();
        dir
    }

    #[test]
    fn load_v4_snapshot() {
        let dir = v4_snapshot();
        let desc = SnapshotDesc::load(dir.path()).unwrap();
        assert_eq!(desc.format(), FormatId::V4);
        assert_eq!(desc.root(), dir.path());
        assert_eq!(desc.n_partitions(), 4);
        assert_eq!(desc.stats().unwrap().n_keys, 1);
        assert!(desc.encoding().is_none());
    }

    #[test]
    fn load_missing_meta_is_io_error() {
        let dir = TempDir::new().unwrap();
        assert!(matches!(SnapshotDesc::load(dir.path()), Err(Error::Io(_))));
    }

    #[test]
    fn load_malformed_json() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("meta.json"), "{ not json").unwrap();
        assert!(matches!(
            SnapshotDesc::load(dir.path()),
            Err(Error::Json(_))
        ));
    }

    #[test]
    fn load_unsupported_version() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("meta.json"),
            r#"{ "format_version": 99, "hash_algorithm": "xxhash64",
                 "verify_seed": 1, "partitions": [] }"#,
        )
        .unwrap();
        assert!(matches!(
            SnapshotDesc::load(dir.path()),
            Err(Error::UnsupportedFormatVersion(99))
        ));
    }

    #[test]
    fn load_rejects_invalid_payload() {
        // Valid version tag, invalid partition count (3 is not a power
        // of two) — payload validation must run at load, not at open.
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("meta.json"),
            r#"{ "format_version": 4, "hash_algorithm": "xxhash64",
                 "verify_seed": 1, "partitions": [
                   { "index_offset": 0, "index_n_buckets": 0 },
                   { "index_offset": 0, "index_n_buckets": 0 },
                   { "index_offset": 0, "index_n_buckets": 0 } ] }"#,
        )
        .unwrap();
        assert!(matches!(
            SnapshotDesc::load(dir.path()),
            Err(Error::InvalidPartitionCount(3))
        ));
    }

    #[test]
    fn load_missing_version_field_is_json_error() {
        // Well-formed JSON without format_version: the probe stage
        // itself fails — the failure mode unique to two-stage parsing.
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("meta.json"), r#"{ "partitions": [] }"#).unwrap();
        assert!(matches!(
            SnapshotDesc::load(dir.path()),
            Err(Error::Json(_))
        ));
    }

    #[test]
    fn unknown_format_error_lists_all_formats() {
        let err = "v9".parse::<FormatId>().unwrap_err().to_string();
        for id in FormatId::ALL {
            assert!(err.contains(id.as_str()), "{err:?} missing {id}");
        }
    }

    #[test]
    fn format_id_roundtrip() {
        assert_eq!("v4".parse::<FormatId>().unwrap(), FormatId::V4);
        assert_eq!(FormatId::V4.to_string(), "v4");
        assert!("v9".parse::<FormatId>().is_err());
        assert_eq!(FormatId::DEFAULT, FormatId::V4);
    }
}
