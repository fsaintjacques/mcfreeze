//! Snapshot mode: serve a single static snapshot directory.
//!
//! Opens one [`SnapshotReader`] at startup and passes it to the listener
//! stack as a [`SnapshotLookup`].  No inotify, no catalog, no hot-swap.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use frostmap_format::reader::SnapshotReader;

use crate::listener::run_listeners;
use crate::lookup::SnapshotLookup;
use crate::ServeError;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

pub struct SnapshotConfig {
    /// Directory containing the snapshot (`index.bin`, `data/`, …).
    pub dir: PathBuf,
    /// Unix-domain socket path to bind, if any.
    pub uds_path: Option<PathBuf>,
    /// TCP address to bind, if any.
    pub tcp_addr: Option<SocketAddr>,
    /// Semver string returned by the `version` command.
    pub semver: String,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Run snapshot mode until all listeners exit.
pub async fn run(cfg: SnapshotConfig) -> Result<(), ServeError> {
    let reader = SnapshotReader::open(&cfg.dir)?;
    tracing::info!(dir = %cfg.dir.display(), "snapshot opened");

    let lookup = Arc::new(SnapshotLookup::new(Arc::new(reader)));

    // generation is always 0 in snapshot mode
    run_listeners(lookup, cfg.uds_path, cfg.tcp_addr, cfg.semver, 0).await?;
    Ok(())
}
