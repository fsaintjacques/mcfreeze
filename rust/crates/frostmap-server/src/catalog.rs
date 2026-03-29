//! Catalog file format deserialized from `catalog.json`.
//!
//! The Lifecycle Manager writes an atomic `rename(2)` of this file when
//! datasets are updated.  [`CatalogFile::load`] reads and parses it.

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::ServeError;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// One entry in the catalog — a single dataset version and its snapshot dir.
#[derive(Debug, Deserialize)]
pub struct CatalogEntry {
    /// Dataset name, e.g. `"my-ds"`.
    pub dataset:      String,
    /// Opaque version string, e.g. `"v42"`.
    pub version:      String,
    /// Absolute path to the snapshot directory, e.g. `/mnt/kv/my-ds/v42`.
    pub snapshot_dir: PathBuf,
}

/// Top-level catalog document.
#[derive(Debug, Deserialize)]
pub struct CatalogFile {
    pub entries: Vec<CatalogEntry>,
}

impl CatalogFile {
    /// Read and parse `path` synchronously.  Suitable for use inside
    /// `spawn_blocking` or at startup before the async runtime is loaded.
    pub fn load(path: &Path) -> Result<Self, ServeError> {
        let data = std::fs::read(path)?;
        serde_json::from_slice(&data)
            .map_err(|e| ServeError::CatalogParse(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_catalog(json: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(json.as_bytes()).unwrap();
        f
    }

    #[test]
    fn parse_valid_catalog() {
        let f = write_catalog(r#"{"entries":[
            {"dataset":"ds1","version":"v1","snapshot_dir":"/mnt/kv/ds1/v1"},
            {"dataset":"ds2","version":"v2","snapshot_dir":"/mnt/kv/ds2/v2"}
        ]}"#);
        let cat = CatalogFile::load(f.path()).unwrap();
        assert_eq!(cat.entries.len(), 2);
        assert_eq!(cat.entries[0].dataset, "ds1");
        assert_eq!(cat.entries[1].version, "v2");
    }

    #[test]
    fn parse_empty_entries() {
        let f = write_catalog(r#"{"entries":[]}"#);
        let cat = CatalogFile::load(f.path()).unwrap();
        assert!(cat.entries.is_empty());
    }

    #[test]
    fn invalid_json_returns_catalog_parse_error() {
        let f = write_catalog("not json");
        let err = CatalogFile::load(f.path()).unwrap_err();
        assert!(matches!(err, ServeError::CatalogParse(_)));
    }

    #[test]
    fn missing_file_returns_io_error() {
        let err = CatalogFile::load("/nonexistent/catalog.json".as_ref()).unwrap_err();
        assert!(matches!(err, ServeError::Io(_)));
    }
}
