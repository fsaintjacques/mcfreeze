//! Snapshot mode: serve a single static snapshot directory.
//!
//! Opens one [`SnapshotReader`] at startup and passes it to the listener
//! stack as a [`SnapshotLookup`].  No inotify, no catalog, no hot-swap.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use frostmap_format::reader::SnapshotReader;
use prometheus_client::registry::Registry;

use crate::listener::run_listeners;
use crate::lookup::SnapshotLookup;
use crate::metrics::Metrics;
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
    /// Address to expose Prometheus `/metrics` on, if any.
    pub metrics_addr: Option<SocketAddr>,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Run snapshot mode until all listeners exit.
pub async fn run(cfg: SnapshotConfig) -> Result<(), ServeError> {
    let mut registry = Registry::default();
    let metrics = Metrics::new(&mut registry);
    let registry = Arc::new(registry);

    // Snapshot mode: always 1 active dataset, generation always 0.
    metrics.active_datasets.set(1);
    metrics.catalog_generation.set(0);

    let reader = SnapshotReader::open(&cfg.dir)?;
    tracing::info!(dir = %cfg.dir.display(), "snapshot opened");

    let lookup: Arc<dyn crate::lookup::Lookup> =
        Arc::new(SnapshotLookup::new(Arc::new(reader)));

    // Metrics server is secondary: fire-and-forget, never blocks startup.
    if let Some(addr) = cfg.metrics_addr {
        let registry = Arc::clone(&registry);
        tokio::spawn(async move {
            if let Err(e) = Metrics::run_server(registry, addr).await {
                tracing::error!("metrics server error: {e}");
            }
        });
    }

    // Propagate bind / accept errors so process supervisors see a non-zero exit.
    run_listeners(lookup, cfg.uds_path, cfg.tcp_addr, cfg.semver, 0, metrics).await?;
    Ok(())
}
