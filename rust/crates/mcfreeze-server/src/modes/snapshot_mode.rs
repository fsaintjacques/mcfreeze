//! Snapshot mode: serve a single static snapshot directory.
//!
//! Opens one [`Snapshot`] at startup and passes it to the listener
//! stack as a [`SnapshotLookup`].  No inotify, no catalog, no hot-swap.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use mcfreeze_format::Snapshot;
use prometheus_client::registry::Registry;

use crate::listener::run_listeners;
use crate::lookup::{LookupFactory, SnapshotLookup};
use crate::metrics::{DatasetLabels, LookupCounters, Metrics};
use crate::ServeError;

/// The `dataset` label value for snapshot mode's single dataset.
const SNAPSHOT_DATASET: &str = "snapshot";

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
    /// Close connections that have been idle (no bytes from the client) for
    /// longer than this. `None` disables the timeout.
    pub idle_timeout: Option<Duration>,
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

    // Blocking (residency work included), but this is startup: listeners
    // have not been bound yet, so nothing is being starved.
    let reader = Snapshot::open_path(&cfg.dir)?;
    // Snapshot mode has exactly one (anonymous) dataset: "snapshot".
    metrics
        .expected_miss_io_rate
        .get_or_create(&DatasetLabels {
            dataset: SNAPSHOT_DATASET.to_string(),
        })
        .set(reader.expected_miss_io_rate());
    tracing::info!(dir = %cfg.dir.display(), "snapshot opened");

    let counters = LookupCounters::new(&metrics.lookup_total, SNAPSHOT_DATASET);
    let factory: Arc<dyn LookupFactory> = Arc::new(SnapshotLookup::new(Arc::new(reader), counters));

    // Metrics server is secondary: fire-and-forget, never blocks startup.
    if let Some(addr) = cfg.metrics_addr {
        let registry = Arc::clone(&registry);
        tokio::spawn(async move {
            if let Err(e) = Metrics::run_server(registry, None, addr).await {
                tracing::error!("metrics server error: {e}");
            }
        });
    }

    // Propagate bind / accept errors so process supervisors see a non-zero exit.
    let generation = Arc::new(AtomicU64::new(0));
    run_listeners(
        factory,
        cfg.uds_path,
        cfg.tcp_addr,
        cfg.semver,
        generation,
        metrics,
        cfg.idle_timeout,
    )
    .await?;
    Ok(())
}
