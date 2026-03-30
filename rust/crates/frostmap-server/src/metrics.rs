//! Prometheus metric definitions and the axum `/metrics` HTTP handler.
//!
//! [`Metrics`] holds every metric handle shared between connection handlers
//! and the catalog-watcher task.  Construct it once via [`Metrics::new`],
//! pass the returned `Arc<Metrics>` through the serving stack, and call
//! [`run_metrics_server`] to expose the Prometheus text exposition format.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::time::SystemTime;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::routing::get;
use serde::Serialize;
use crate::registry::Registry as DataRegistry;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

// ---------------------------------------------------------------------------
// Label types
// ---------------------------------------------------------------------------

/// Label for `fm_request_duration_seconds` and `fm_catalog_swap_total`.
/// Values: `"hit"`, `"miss"`, `"error"`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ResultLabels {
    pub result: &'static str,
}

/// Label for `fm_connections_active` and `fm_connections_total`.
/// Values: `"uds"`, `"tcp"`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TransportLabels {
    pub transport: &'static str,
}

// ---------------------------------------------------------------------------
// Histogram buckets
// ---------------------------------------------------------------------------

/// Per-key lookup latency: 50µs – 100ms.
const REQUEST_DURATION_BUCKETS: [f64; 10] = [
    0.000_050, 0.000_100, 0.000_200, 0.000_500,
    0.001, 0.002, 0.005, 0.010, 0.050, 0.100,
];

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// All Prometheus metrics for the server.  Shared via `Arc<Metrics>`.
pub struct Metrics {
    // --- request ---
    /// End-to-end per-key latency; label `result` ∈ {hit, miss, error}.
    pub request_duration_seconds: Family<ResultLabels, Histogram>,
    /// Total individual key lookups (all results).
    pub keys_requested_total: Counter,
    /// Individual key hits.
    pub keys_hit_total: Counter,
    /// Individual key misses.
    pub keys_miss_total: Counter,
    /// Total value bytes written to clients.
    pub response_bytes_total: Counter,

    // --- catalog (always present; snapshot mode uses static values) ---
    /// Current catalog generation; always 0 in snapshot mode.
    pub catalog_generation: Gauge<i64, AtomicI64>,
    /// Catalog hot-swaps; label `result` ∈ {ok, error}; always 0 in snapshot mode.
    pub catalog_swap_total: Family<ResultLabels, Counter>,
    /// Active dataset count; always 1 in snapshot mode.
    pub active_datasets: Gauge<i64, AtomicI64>,

    // --- connections ---
    /// Currently open connections by transport.
    pub connections_active: Family<TransportLabels, Gauge<i64, AtomicI64>>,
    /// Total connections accepted by transport.
    pub connections_total: Family<TransportLabels, Counter>,
}

impl Metrics {
    /// Register all metrics in `registry` and return shared handles.
    ///
    /// The caller owns the `Registry`; pass `Arc<Registry>` to
    /// [`run_metrics_server`] to expose the `/metrics` endpoint.
    pub fn new(registry: &mut Registry) -> Arc<Self> {
        macro_rules! reg {
            ($registry:expr, $name:expr, $help:expr, $metric:expr) => {{
                $registry.register($name, $help, $metric.clone());
                $metric
            }};
        }

        let request_duration_seconds = reg!(
            registry,
            "fm_request_duration_seconds",
            "End-to-end per-key request latency",
            Family::<ResultLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(REQUEST_DURATION_BUCKETS.iter().copied())
            })
        );
        let keys_requested_total = reg!(
            registry,
            "fm_keys_requested_total",
            "Total individual key lookups",
            Counter::<u64, std::sync::atomic::AtomicU64>::default()
        );
        let keys_hit_total = reg!(
            registry,
            "fm_keys_hit_total",
            "Individual key hits",
            Counter::<u64, std::sync::atomic::AtomicU64>::default()
        );
        let keys_miss_total = reg!(
            registry,
            "fm_keys_miss_total",
            "Individual key misses",
            Counter::<u64, std::sync::atomic::AtomicU64>::default()
        );
        let response_bytes_total = reg!(
            registry,
            "fm_response_bytes_total",
            "Total value bytes sent to clients",
            Counter::<u64, std::sync::atomic::AtomicU64>::default()
        );
        let catalog_generation = reg!(
            registry,
            "fm_catalog_generation",
            "Current catalog generation; 0 in snapshot mode",
            Gauge::<i64, AtomicI64>::default()
        );
        let catalog_swap_total = reg!(
            registry,
            "fm_catalog_swap_total",
            "Catalog hot-swaps by result; always 0 in snapshot mode",
            Family::<ResultLabels, Counter>::default()
        );
        let active_datasets = reg!(
            registry,
            "fm_active_datasets",
            "Active dataset count; always 1 in snapshot mode",
            Gauge::<i64, AtomicI64>::default()
        );
        let connections_active = reg!(
            registry,
            "fm_connections_active",
            "Currently open connections by transport",
            Family::<TransportLabels, Gauge<i64, AtomicI64>>::default()
        );
        let connections_total = reg!(
            registry,
            "fm_connections_total",
            "Total connections accepted by transport",
            Family::<TransportLabels, Counter>::default()
        );

        Arc::new(Self {
            request_duration_seconds,
            keys_requested_total,
            keys_hit_total,
            keys_miss_total,
            response_bytes_total,
            catalog_generation,
            catalog_swap_total,
            active_datasets,
            connections_active,
            connections_total,
        })
    }

    /// Bind `addr` and serve `/metrics` (Prometheus) and `GET /version` (KV
    /// version state for the node-agent convergence poll).
    ///
    /// Returns when the HTTP server exits (it normally runs forever).
    pub async fn run_server(
        prom:     Arc<Registry>,
        data_reg: Option<Arc<DataRegistry>>,
        addr:     SocketAddr,
    ) -> std::io::Result<()> {
        let state = HttpState { prom, data_reg };
        let router = Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/version", get(version_handler))
            .with_state(state);

        let listener = tokio::net::TcpListener::bind(addr).await?;
        tracing::info!(%addr, "HTTP server listening");

        axum::serve(listener, router)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }
}

// ---------------------------------------------------------------------------
// Shared HTTP state
// ---------------------------------------------------------------------------

/// State shared across all HTTP handlers.
#[derive(Clone)]
struct HttpState {
    prom:     Arc<Registry>,
    /// `None` in snapshot mode, which has no catalog registry.
    data_reg: Option<Arc<DataRegistry>>,
}

// ---------------------------------------------------------------------------
// Response types for GET /version
// ---------------------------------------------------------------------------

/// Wire type for one dataset entry in the `GET /version` response.
/// Matches the Go `api.KVDatasetVersion` JSON shape.
#[derive(Serialize)]
struct KVDatasetVersion {
    dataset:    String,
    version_id: String,
    loaded_at:  String, // RFC3339 UTC
}

/// Wire type for the `GET /version` response body.
/// Matches the Go `api.KVVersionResponse` JSON shape.
#[derive(Serialize)]
struct KVVersionResponse {
    datasets: Vec<KVDatasetVersion>,
}

// ---------------------------------------------------------------------------
// HTTP handlers
// ---------------------------------------------------------------------------

const METRICS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

async fn metrics_handler(State(state): State<HttpState>) -> impl IntoResponse {
    let mut buf = String::new();
    encode(&mut buf, &state.prom).unwrap();
    ([(CONTENT_TYPE, METRICS_CONTENT_TYPE)], buf)
}

async fn version_handler(State(state): State<HttpState>) -> Json<KVVersionResponse> {
    let datasets = match &state.data_reg {
        None => vec![],
        Some(reg) => {
            let catalog = Arc::clone(&*reg.load());
            catalog.version_snapshot().into_iter().map(|e| KVDatasetVersion {
                dataset:    e.dataset,
                version_id: e.version_id,
                loaded_at:  format_rfc3339(e.loaded_at),
            }).collect()
        }
    };
    Json(KVVersionResponse { datasets })
}

/// Format a [`SystemTime`] as an RFC3339 UTC string (e.g. `"2024-01-15T10:30:00Z"`).
fn format_rfc3339(t: SystemTime) -> String {
    humantime::format_rfc3339_seconds(t).to_string()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_metrics_register_without_panic() {
        let mut reg = Registry::default();
        let m = Metrics::new(&mut reg);
        // spot-check: recording should not panic
        m.keys_requested_total.inc();
        m.keys_hit_total.inc();
        m.connections_active.get_or_create(&TransportLabels { transport: "tcp" }).inc();
        m.connections_active.get_or_create(&TransportLabels { transport: "tcp" }).dec();
        m.active_datasets.set(1);
        m.catalog_generation.set(0);
    }

    #[test]
    fn metrics_encodes_to_text() {
        let mut reg = Registry::default();
        let m = Metrics::new(&mut reg);
        m.keys_hit_total.inc_by(3);
        let mut buf = String::new();
        encode(&mut buf, &reg).unwrap();
        assert!(buf.contains("fm_keys_hit_total"));
    }

    #[test]
    fn format_rfc3339_unix_epoch() {
        assert_eq!(format_rfc3339(SystemTime::UNIX_EPOCH), "1970-01-01T00:00:00Z");
    }

    // --- version handler ---

    use crate::registry::{ActiveCatalog, DatasetHandle};
    use axum::extract::State;
    use frostmap_format::writer::SnapshotWriter;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn build_snapshot(pairs: &[(&[u8], &[u8])]) -> TempDir {
        let dir = TempDir::new().unwrap();
        let mut w = SnapshotWriter::new(dir.path(), 1).unwrap();
        for &(k, v) in pairs { w.write(k, v).unwrap(); }
        w.finish(dir.path()).unwrap();
        dir
    }

    fn make_state(data_reg: Option<Arc<DataRegistry>>) -> HttpState {
        HttpState {
            prom: Arc::new(Registry::default()),
            data_reg,
        }
    }

    #[tokio::test]
    async fn version_handler_none_returns_empty_datasets() {
        let Json(resp) = version_handler(State(make_state(None))).await;
        assert!(resp.datasets.is_empty());
    }

    #[tokio::test]
    async fn version_handler_returns_loaded_datasets() {
        let dir = build_snapshot(&[(b"k", b"v")]);
        let mut ds = HashMap::new();
        ds.insert(
            "pfx".into(),
            DatasetHandle::open("my-ds".into(), "v42".into(), dir.path()).unwrap(),
        );
        let loaded_at = SystemTime::UNIX_EPOCH;
        let catalog = ActiveCatalog::new(ds, 1, loaded_at);
        let reg = DataRegistry::new(catalog);

        let Json(resp) = version_handler(State(make_state(Some(reg)))).await;

        assert_eq!(resp.datasets.len(), 1);
        assert_eq!(resp.datasets[0].dataset, "my-ds");
        assert_eq!(resp.datasets[0].version_id, "v42");
        assert_eq!(resp.datasets[0].loaded_at, "1970-01-01T00:00:00Z");
    }
}
