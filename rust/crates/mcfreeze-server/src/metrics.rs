//! Prometheus metric definitions and the axum `/metrics` HTTP handler.
//!
//! [`Metrics`] holds every metric handle shared between connection handlers
//! and the catalog-watcher task.  Construct it once via [`Metrics::new`],
//! pass the returned `Arc<Metrics>` through the serving stack, and call
//! [`run_metrics_server`] to expose the Prometheus text exposition format.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::Arc;
use std::time::SystemTime;

use crate::registry::Registry as DataRegistry;
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Json;
use axum::Router;
use prometheus_client::encoding::text::encode;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;
use serde::Serialize;

// ---------------------------------------------------------------------------
// Label types
// ---------------------------------------------------------------------------

/// Label for `mcf_request_duration_seconds` and `mcf_catalog_swap_total`.
///
/// Values on `mcf_request_duration_seconds`: `"hit"`, `"miss"`,
/// `"miss_io"`, `"error"`.  `miss_io` is a miss that paid one or more
/// `pread`s to conclude absence — a fingerprint or filter false positive.
/// Its expected rate is format-dependent: compare the observed
/// `miss_io / (miss + miss_io)` ratio against `mcf_expected_miss_io_rate`
/// rather than alerting on absolute counts. Wire response is identical
/// to a miss.
///
/// Values on `mcf_catalog_swap_total`: `"ok"`, `"error"`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ResultLabels {
    pub result: &'static str,
}

/// Label for `mcf_connections_active` and `mcf_connections_total`.
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
    0.000_050, 0.000_100, 0.000_200, 0.000_500, 0.001, 0.002, 0.005, 0.010, 0.050, 0.100,
];

/// Per-response value size: 64B – 1MiB (powers of 4).
const RESPONSE_BYTES_BUCKETS: [f64; 8] = [
    64.0,
    256.0,
    1_024.0,
    4_096.0,
    16_384.0,
    65_536.0,
    262_144.0,
    1_048_576.0,
];

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// All Prometheus metrics for the server.  Shared via `Arc<Metrics>`.
pub struct Metrics {
    // --- request ---
    /// End-to-end per-key latency; label `result` ∈ {hit, miss, miss_io,
    /// error}.  The histogram's `_count` series gives per-result and total
    /// lookup counts.
    pub request_duration_seconds: Family<ResultLabels, Histogram>,
    /// Per-response value-size distribution (hits only); `_sum` is total bytes sent.
    pub response_bytes: Histogram,
    /// Expected fraction of misses that pay I/O, from the active
    /// snapshots' formats (max across datasets; ≈0 for V4). The observed
    /// counterpart is `miss_io / (miss + miss_io)` from
    /// `mcf_request_duration_seconds_count`.
    pub expected_miss_io_rate: Gauge<f64, AtomicU64>,

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
                let m = $metric;
                $registry.register($name, $help, m.clone());
                m
            }};
        }

        let request_duration_seconds = reg!(
            registry,
            "mcf_request_duration_seconds",
            "End-to-end per-key request latency",
            Family::<ResultLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(REQUEST_DURATION_BUCKETS.iter().copied())
            })
        );
        let response_bytes = reg!(
            registry,
            "mcf_response_bytes",
            "Per-response value size in bytes (hits only)",
            Histogram::new(RESPONSE_BYTES_BUCKETS.iter().copied())
        );
        let expected_miss_io_rate = reg!(
            registry,
            "mcf_expected_miss_io_rate",
            "Expected fraction of misses paying I/O for the active formats",
            Gauge::<f64, AtomicU64>::default()
        );
        let catalog_generation = reg!(
            registry,
            "mcf_catalog_generation",
            "Current catalog generation; 0 in snapshot mode",
            Gauge::<i64, AtomicI64>::default()
        );
        let catalog_swap_total = reg!(
            registry,
            "mcf_catalog_swap_total",
            "Catalog hot-swaps by result; always 0 in snapshot mode",
            Family::<ResultLabels, Counter>::default()
        );
        let active_datasets = reg!(
            registry,
            "mcf_active_datasets",
            "Active dataset count; always 1 in snapshot mode",
            Gauge::<i64, AtomicI64>::default()
        );
        let connections_active = reg!(
            registry,
            "mcf_connections_active",
            "Currently open connections by transport",
            Family::<TransportLabels, Gauge<i64, AtomicI64>>::default()
        );
        let connections_total = reg!(
            registry,
            "mcf_connections_total",
            "Total connections accepted by transport",
            Family::<TransportLabels, Counter>::default()
        );

        Arc::new(Self {
            request_duration_seconds,
            response_bytes,
            expected_miss_io_rate,
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
        prom: Arc<Registry>,
        data_reg: Option<Arc<DataRegistry>>,
        addr: SocketAddr,
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
            .map_err(std::io::Error::other)
    }
}

// ---------------------------------------------------------------------------
// Shared HTTP state
// ---------------------------------------------------------------------------

/// State shared across all HTTP handlers.
#[derive(Clone)]
struct HttpState {
    prom: Arc<Registry>,
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
    dataset: String,
    version_id: String,
    loaded_at: String, // RFC3339 UTC
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
            catalog
                .version_snapshot()
                .into_iter()
                .map(|e| KVDatasetVersion {
                    dataset: e.dataset,
                    version_id: e.version_id,
                    loaded_at: format_rfc3339(e.loaded_at),
                })
                .collect()
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
        m.request_duration_seconds
            .get_or_create(&ResultLabels { result: "hit" })
            .observe(0.001);
        m.response_bytes.observe(128.0);
        m.connections_active
            .get_or_create(&TransportLabels { transport: "tcp" })
            .inc();
        m.connections_active
            .get_or_create(&TransportLabels { transport: "tcp" })
            .dec();
        m.active_datasets.set(1);
        m.catalog_generation.set(0);
    }

    #[test]
    fn metrics_encodes_to_text() {
        let mut reg = Registry::default();
        let m = Metrics::new(&mut reg);
        m.request_duration_seconds
            .get_or_create(&ResultLabels { result: "hit" })
            .observe(0.001);
        let mut buf = String::new();
        encode(&mut buf, &reg).unwrap();
        assert!(buf.contains("mcf_request_duration_seconds"));
    }

    #[test]
    fn format_rfc3339_unix_epoch() {
        assert_eq!(
            format_rfc3339(SystemTime::UNIX_EPOCH),
            "1970-01-01T00:00:00Z"
        );
    }

    // --- version handler ---

    use crate::registry::{ActiveCatalog, DatasetHandle};
    use axum::extract::State;
    use mcfreeze_format::writer::SnapshotWriter;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn build_snapshot(pairs: &[(&[u8], &[u8])]) -> TempDir {
        let dir = TempDir::new().unwrap();
        let mut w = SnapshotWriter::new(dir.path(), 1).unwrap();
        for &(k, v) in pairs {
            w.write(k, v).unwrap();
        }
        w.finish().unwrap();
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
