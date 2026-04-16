//! Catalog mode: watch `catalog.json` and hot-swap on each atomic rename.
//!
//! ## Lifecycle
//!
//! 1. Load the initial `catalog.json` at startup; build an [`ActiveCatalog`].
//! 2. Spawn an inotify watcher task on the catalog file's parent directory.
//! 3. On each `IN_MOVED_TO` event matching the catalog filename:
//!    a. Open new [`SnapshotReader`]s and build a fresh [`ActiveCatalog`].
//!    b. Atomically swap it into the [`Registry`].
//!    c. Drop the old [`Arc<ActiveCatalog>`] (releases mmaps and fds).
//!    d. Write the ack file so the Lifecycle Manager can proceed with unmount.
//! 4. Listeners run forever; their [`PerConnectionCatalogLookup`]s pick up the
//!    new generation on the next request without any coordination.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use prometheus_client::registry::Registry;
use tokio::sync::{mpsc, oneshot};

use crate::catalog::CatalogFile;
use crate::listener::run_listeners;
use crate::lookup::{CatalogLookup, LookupFactory};
use crate::metrics::Metrics;
use crate::registry::{ActiveCatalog, DatasetHandle, Registry as DataRegistry};
use crate::ServeError;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

pub struct CatalogConfig {
    /// Path to `catalog.json` watched for changes.
    pub catalog_path: PathBuf,
    /// Unix-domain socket path to bind, if any.
    pub uds_path: Option<PathBuf>,
    /// TCP address to bind, if any.
    pub tcp_addr: Option<SocketAddr>,
    /// Semver string returned by the `version` command.
    pub semver: String,
    /// Address to expose Prometheus `/metrics` on, if any.
    pub metrics_addr: Option<SocketAddr>,
    /// Close connections that have been idle (no bytes from the client) for
    /// longer than this. Bounds how long a silent client may pin the
    /// `Arc<ActiveCatalog>` from a prior generation (and its index mmap).
    /// `None` disables the timeout.
    pub idle_timeout: Option<Duration>,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Run catalog mode until all listeners exit.
pub async fn run(cfg: CatalogConfig) -> Result<(), ServeError> {
    let mut prom = Registry::default();
    let metrics = Metrics::new(&mut prom);
    let prom = Arc::new(prom);

    // Load initial catalog synchronously.  If the file does not exist yet
    // (e.g. the node-agent hasn't written it), start with an empty catalog;
    // the watcher will pick up the first write.
    let initial = match build_catalog_blocking(cfg.catalog_path.clone(), 0).await {
        Ok(cat) => cat,
        Err(ServeError::Io(ref e)) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::info!("catalog file not found; starting with empty catalog");
            ActiveCatalog::new(HashMap::new(), 0, std::time::SystemTime::now())
        }
        Err(e) => return Err(e),
    };
    let empty = ActiveCatalog::new(HashMap::new(), 0, std::time::SystemTime::now());
    log_catalog_diff(&empty, &initial, 0);
    let registry = DataRegistry::new(initial);
    let n_ds = registry.load().dataset_count() as i64;

    metrics.active_datasets.set(n_ds);
    metrics.catalog_generation.set(0);

    let generation: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let factory: Arc<dyn LookupFactory> = Arc::new(CatalogLookup::new(Arc::clone(&registry)));

    // HTTP server (serves /metrics and /version): fire-and-forget.
    if let Some(addr) = cfg.metrics_addr {
        let prom = Arc::clone(&prom);
        let data_reg = Arc::clone(&registry);
        tokio::spawn(async move {
            if let Err(e) = Metrics::run_server(prom, Some(data_reg), addr).await {
                tracing::error!("HTTP server error: {e}");
            }
        });
    }

    // Catalog watcher task.  A oneshot carries the startup result back so that
    // a watcher init failure (e.g. inotify unavailable) is fatal rather than
    // silently leaving the catalog permanently stale.
    let (startup_tx, startup_rx) = oneshot::channel::<Result<(), String>>();
    {
        let catalog_path = cfg.catalog_path.clone();
        let registry = Arc::clone(&registry);
        let metrics = Arc::clone(&metrics);
        let gen_watcher = Arc::clone(&generation);
        tokio::spawn(async move {
            watch_catalog(catalog_path, registry, metrics, gen_watcher, startup_tx).await;
        });
    }
    // Wait for the watcher to confirm it started before accepting connections.
    match startup_rx.await {
        Ok(Ok(())) => {}
        Ok(Err(msg)) => return Err(ServeError::Io(std::io::Error::other(msg))),
        Err(_) => {
            return Err(ServeError::Io(std::io::Error::other(
                "catalog watcher task died during init",
            )))
        }
    }

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

// ---------------------------------------------------------------------------
// Catalog watcher
// ---------------------------------------------------------------------------

/// Watches the catalog file for atomic renames and reloads on each change.
///
/// Runs a blocking inotify/FSEvents loop in [`tokio::task::spawn_blocking`]
/// and processes reload requests on the async side.
async fn watch_catalog(
    catalog_path: PathBuf,
    registry: Arc<DataRegistry>,
    metrics: Arc<Metrics>,
    generation: Arc<AtomicU64>,
    startup_tx: oneshot::Sender<Result<(), String>>,
) {
    let parent = match catalog_path.parent() {
        Some(p) => p.to_owned(),
        None => {
            let _ = startup_tx.send(Err("catalog path has no parent directory".into()));
            return;
        }
    };

    let (tx, mut rx) = mpsc::channel::<()>(4);

    // Blocking task: watches filesystem events and sends () when the
    // catalog file is replaced.  Runs in tokio's blocking thread pool.
    // Sends startup result via `startup_tx_bg` so run() can fail fast.
    let catalog_path_bg = catalog_path.clone();
    let (startup_tx_bg, startup_rx_bg) = std::sync::mpsc::sync_channel::<Result<(), String>>(1);
    tokio::task::spawn_blocking(move || {
        use notify::{Config, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
        use std::sync::mpsc as std_mpsc;

        let (std_tx, std_rx) = std_mpsc::channel::<notify::Result<notify::Event>>();

        let mut watcher = match RecommendedWatcher::new(std_tx, Config::default()) {
            Ok(w) => w,
            Err(e) => {
                let _ =
                    startup_tx_bg.send(Err(format!("failed to create filesystem watcher: {e}")));
                return;
            }
        };
        if let Err(e) = watcher.watch(&parent, RecursiveMode::NonRecursive) {
            let _ = startup_tx_bg.send(Err(format!("failed to watch {}: {e}", parent.display())));
            return;
        }
        let _ = startup_tx_bg.send(Ok(()));
        tracing::info!(path = %catalog_path_bg.display(), "catalog watcher started");

        while let Ok(result) = std_rx.recv() {
            match result {
                Ok(event) => {
                    // React to create/modify/rename events on the catalog path only.
                    let is_relevant =
                        matches!(event.kind, EventKind::Create(_) | EventKind::Modify(_));
                    if is_relevant
                        && event.paths.iter().any(|p| p == &catalog_path_bg)
                        && tx.blocking_send(()).is_err()
                    {
                        return; // receiver dropped — shut down
                    }
                }
                Err(e) => tracing::error!("catalog watch error: {e}"),
            }
        }
        tracing::error!("catalog watcher exited unexpectedly; catalog will not update");
    });

    // Forward the blocking startup result to the async caller.
    let startup_result = match startup_rx_bg.recv() {
        Ok(r) => r,
        Err(_) => Err("watcher blocking task died before signalling startup".into()),
    };
    let _ = startup_tx.send(startup_result);

    // Async reload loop.
    while rx.recv().await.is_some() {
        let next_gen = generation.load(Ordering::Relaxed) + 1;

        match build_catalog_blocking(catalog_path.clone(), next_gen).await {
            Err(e) => {
                tracing::error!("catalog reload failed (keeping current catalog): {e}");
            }
            Ok(new_catalog) => {
                let n_ds = new_catalog.dataset_count() as i64;
                let new_arc = Arc::new(new_catalog);
                let old = registry.swap(Arc::clone(&new_arc));

                log_catalog_diff(&old, &new_arc, next_gen);
                drop(old); // release mmaps and file descriptors

                generation.store(next_gen, Ordering::Relaxed);
                metrics.catalog_generation.set(next_gen as i64);
                metrics.active_datasets.set(n_ds);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Catalog diff logging
// ---------------------------------------------------------------------------

/// Log a human-readable diff between the old and new catalog.
fn log_catalog_diff(old: &ActiveCatalog, new: &ActiveCatalog, generation: u64) {
    use std::collections::HashMap;

    let old_map: HashMap<&str, (&str, &str)> = old
        .datasets()
        .map(|(prefix, name, ver)| (prefix, (name, ver)))
        .collect();
    let new_map: HashMap<&str, (&str, &str)> = new
        .datasets()
        .map(|(prefix, name, ver)| (prefix, (name, ver)))
        .collect();

    let mut changed = false;

    for (prefix, (name, new_ver)) in &new_map {
        match old_map.get(prefix) {
            Some((_, old_ver)) if old_ver != new_ver => {
                tracing::info!(
                    generation,
                    dataset = name,
                    version = new_ver,
                    prev = *old_ver,
                    "dataset updated"
                );
                changed = true;
            }
            Some(_) => {} // unchanged
            None => {
                tracing::info!(
                    generation,
                    dataset = name,
                    version = new_ver,
                    "dataset added"
                );
                changed = true;
            }
        }
    }
    for (prefix, (name, old_ver)) in &old_map {
        if !new_map.contains_key(prefix) {
            tracing::info!(
                generation,
                dataset = name,
                version = old_ver,
                "dataset removed"
            );
            changed = true;
        }
    }

    if !changed {
        tracing::info!(
            generation,
            datasets = new_map.len(),
            "catalog reloaded (no changes)"
        );
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Open all datasets listed in `catalog_path` and build an [`ActiveCatalog`].
/// Runs in the blocking thread pool since `SnapshotReader::open` does file I/O.
async fn build_catalog_blocking(
    catalog_path: PathBuf,
    generation: u64,
) -> Result<ActiveCatalog, ServeError> {
    tokio::task::spawn_blocking(move || build_catalog_sync(&catalog_path, generation))
        .await
        .map_err(|e| ServeError::BlockingTaskPanicked(e.to_string()))?
}

fn build_catalog_sync(path: &Path, generation: u64) -> Result<ActiveCatalog, ServeError> {
    let file = CatalogFile::load(path)?;
    let mut datasets = HashMap::new();
    for entry in file.entries {
        if datasets.contains_key(&entry.key_prefix) {
            return Err(ServeError::CatalogParse(format!(
                "duplicate key_prefix {:?} in catalog",
                entry.key_prefix
            )));
        }
        let handle = DatasetHandle::open(entry.dataset, entry.version_id, &entry.mount_path)?;
        datasets.insert(entry.key_prefix, handle);
    }
    Ok(ActiveCatalog::new(
        datasets,
        generation,
        std::time::SystemTime::now(),
    ))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use mcfreeze_format::writer::SnapshotWriter;
    use std::io::Write;
    use tempfile::{NamedTempFile, TempDir};

    fn write_catalog(json: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(json.as_bytes()).unwrap();
        f
    }

    fn build_snapshot() -> TempDir {
        let dir = TempDir::new().unwrap();
        let mut w = SnapshotWriter::new(dir.path(), 1).unwrap();
        w.write(b"k", b"v").unwrap();
        w.finish().unwrap();
        dir
    }

    #[test]
    fn duplicate_key_prefix_returns_catalog_parse_error() {
        // Two entries sharing a key_prefix must be rejected even if they
        // have distinct dataset names — key_prefix is the routing key.
        // The first entry needs a valid snapshot so open() succeeds before
        // the second entry's duplicate check fires.
        let snap = build_snapshot();
        let snap_path = snap.path().to_str().unwrap();
        let json = format!(
            r#"{{"entries":[
            {{"dataset":"ds-a","key_prefix":"shared","version_id":"v1","mount_path":"{snap_path}"}},
            {{"dataset":"ds-b","key_prefix":"shared","version_id":"v1","mount_path":"{snap_path}"}}
        ]}}"#
        );
        let f = write_catalog(&json);
        match build_catalog_sync(f.path(), 0) {
            Err(ServeError::CatalogParse(msg)) => {
                assert!(
                    msg.contains("shared"),
                    "error should mention the duplicate key_prefix: {msg}"
                );
            }
            Err(other) => panic!("expected CatalogParse, got: {other}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }
}
