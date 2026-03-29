//! Accept loops and connection handler.
//!
//! [`run_listeners`] binds a UDS path and/or TCP address, spawns an accept
//! task for each, and returns when all accept tasks exit.
//!
//! [`handle_connection`] is the per-connection task.  It owns a
//! `BytesMut` read buffer and a `BytesMut` write buffer, drives the
//! [`parse_command`] / [`dispatch`] pipeline, and flushes the write buffer
//! at the end of each pipeline batch (when the read buffer is drained).
//!
//! The function is generic over `AsyncRead + AsyncWrite + Unpin` so it can
//! be tested with `tokio::io::duplex` without binding real sockets.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::time::Duration;

use bytes::{Buf, BytesMut};
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, UnixListener};

use crate::lookup::Lookup;
use crate::metrics::{Metrics, TransportLabels};
use crate::protocol::commands::{Disposition, dispatch};
use crate::protocol::meta::parse_command;

// ---------------------------------------------------------------------------
// RAII guard for connections_active gauge
// ---------------------------------------------------------------------------

/// Increments the active-connection gauge on construction and decrements it
/// on drop — including on panic — so the gauge never leaks.
struct ActiveConnectionGuard {
    family: Family<TransportLabels, Gauge<i64, AtomicI64>>,
    labels: TransportLabels,
}

impl ActiveConnectionGuard {
    fn new(
        family: &Family<TransportLabels, Gauge<i64, AtomicI64>>,
        labels: TransportLabels,
    ) -> Self {
        family.get_or_create(&labels).inc();
        Self { family: family.clone(), labels }
    }
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        self.family.get_or_create(&self.labels).dec();
    }
}

// Read buffer initial capacity: one typical MTU worth of pipelined commands.
const READ_BUF_INIT: usize = 8 * 1024;
// Write buffer initial capacity: sized for a moderate pipeline batch.
const WRITE_BUF_INIT: usize = 64 * 1024;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Bind listeners and serve until all accept loops exit.
///
/// At least one of `uds_path` / `tcp_addr` should be `Some`; if both are
/// `None` the function returns immediately with `Ok(())`.
pub async fn run_listeners(
    lookup:     Arc<dyn Lookup>,
    uds_path:   Option<PathBuf>,
    tcp_addr:   Option<SocketAddr>,
    semver:     String,
    generation: u64,
    metrics:    Arc<Metrics>,
) -> std::io::Result<()> {
    let mut tasks = tokio::task::JoinSet::new();

    if let Some(path) = uds_path {
        // Remove a stale socket file so that restart after a crash succeeds.
        std::fs::remove_file(&path).ok();
        let listener = UnixListener::bind(&path)?;
        tracing::info!(path = %path.display(), "UDS listener bound");
        let lookup  = Arc::clone(&lookup);
        let semver  = semver.clone();
        let metrics = Arc::clone(&metrics);
        tasks.spawn(async move {
            accept_uds(listener, lookup, semver, generation, metrics).await;
        });
    }

    if let Some(addr) = tcp_addr {
        let listener = TcpListener::bind(addr).await?;
        tracing::info!(%addr, "TCP listener bound");
        let lookup  = Arc::clone(&lookup);
        let semver  = semver.clone();
        let metrics = Arc::clone(&metrics);
        tasks.spawn(async move {
            accept_tcp(listener, lookup, semver, generation, metrics).await;
        });
    }

    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result {
            tracing::error!("listener task panicked: {e}");
        }
    }

    Ok(())
}

/// Handle one connection end-to-end.
///
/// Reads commands from `io`, dispatches them through `lookup`, and writes
/// responses back.  Returns when the client sends `quit`, closes the
/// connection, or an unrecoverable error occurs.
///
/// `transport` is `"uds"` or `"tcp"` and is used solely for metric labels.
pub async fn handle_connection<IO>(
    mut io:     IO,
    lookup:     Arc<dyn Lookup>,
    semver:     String,
    generation: u64,
    metrics:    Arc<Metrics>,
    transport:  &'static str,
) where IO: AsyncRead + AsyncWrite + Unpin {
    let labels = TransportLabels { transport };
    metrics.connections_total.get_or_create(&labels).inc();
    let _active_guard = ActiveConnectionGuard::new(&metrics.connections_active, labels.clone());

    let mut read_buf  = BytesMut::with_capacity(READ_BUF_INIT);
    let mut write_buf = BytesMut::with_capacity(WRITE_BUF_INIT);

    'outer: loop {
        // Block until at least one byte arrives (or EOF / error).
        match io.read_buf(&mut read_buf).await {
            Ok(0) => break,  // clean EOF
            Ok(_) => {}
            Err(e) => {
                tracing::debug!("connection read error: {e}");
                break;
            }
        }

        // Dispatch every complete command in the read buffer.
        loop {
            match parse_command(&mut read_buf) {
                Err(e) => {
                    tracing::debug!("parse error: {e}");
                    write_buf.extend_from_slice(b"ERROR\r\n");
                    let _ = io.write_all(&write_buf).await;
                    break 'outer;
                }
                Ok(None) => break, // incomplete — flush then read more
                Ok(Some(cmd)) => {
                    match dispatch(cmd, &*lookup, &mut write_buf, &semver, generation, &metrics).await {
                        Disposition::Continue => {}
                        Disposition::Close => {
                            let _ = io.write_all(&write_buf).await;
                            break 'outer;
                        }
                        Disposition::Drain(n) => {
                            if !drain(&mut io, &mut read_buf, n).await {
                                break 'outer;
                            }
                        }
                    }
                }
            }
        }

        // Flush at end of each pipeline batch.
        if !write_buf.is_empty() {
            if io.write_all(&write_buf).await.is_err() {
                break;
            }
            write_buf.clear();
        }
    }

}

// ---------------------------------------------------------------------------
// Accept loops
// ---------------------------------------------------------------------------

async fn accept_uds(
    listener:   UnixListener,
    lookup:     Arc<dyn Lookup>,
    semver:     String,
    generation: u64,
    metrics:    Arc<Metrics>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                tracing::debug!("UDS connection accepted");
                let lookup  = Arc::clone(&lookup);
                let semver  = semver.clone();
                let metrics = Arc::clone(&metrics);
                tokio::spawn(async move {
                    handle_connection(stream, lookup, semver, generation, metrics, "uds").await;
                });
            }
            Err(e) => {
                tracing::error!("UDS accept error: {e}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

async fn accept_tcp(
    listener:   TcpListener,
    lookup:     Arc<dyn Lookup>,
    semver:     String,
    generation: u64,
    metrics:    Arc<Metrics>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                tracing::debug!(%addr, "TCP connection accepted");
                let lookup  = Arc::clone(&lookup);
                let semver  = semver.clone();
                let metrics = Arc::clone(&metrics);
                tokio::spawn(async move {
                    handle_connection(stream, lookup, semver, generation, metrics, "tcp").await;
                });
            }
            Err(e) => {
                tracing::error!("TCP accept error: {e}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Drain helper
// ---------------------------------------------------------------------------

/// Discard exactly `n` bytes from `io`/`buf`. Returns `false` on EOF or error.
async fn drain<IO: AsyncRead + Unpin>(
    io:  &mut IO,
    buf: &mut BytesMut,
    n:   usize,
) -> bool {
    while buf.len() < n {
        match io.read_buf(buf).await {
            Ok(0) | Err(_) => return false,
            Ok(_) => {}
        }
    }
    buf.advance(n);
    true
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Metrics, ServeError, lookup::Lookup};
    use async_trait::async_trait;
    use bytes::Bytes;
    use prometheus_client::registry::Registry;
    use std::collections::HashMap;
    use tokio::io::AsyncWriteExt;

    fn noop_metrics() -> Arc<Metrics> {
        Metrics::new(&mut Registry::default())
    }

    struct MockLookup(HashMap<&'static [u8], &'static [u8]>);

    impl MockLookup {
        fn new(entries: &[(&'static [u8], &'static [u8])]) -> Arc<Self> {
            Arc::new(Self(entries.iter().copied().collect()))
        }
    }

    #[async_trait]
    impl Lookup for MockLookup {
        async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, ServeError> {
            Ok(self.0.get(key).map(|&v| Bytes::from_static(v)))
        }
    }

    /// Run `handle_connection` over a duplex pair, send `input` followed by
    /// `quit\r\n`, and collect all bytes written by the server.
    ///
    /// The server closes the connection on `quit`, which causes the client
    /// read side to see EOF — ending `read_to_end`.  Dropping the write half
    /// is not sufficient because `tokio::io::split` keeps the underlying
    /// `DuplexStream` alive through the read half.
    async fn roundtrip(
        lookup: Arc<dyn Lookup>,
        input:  &[u8],
    ) -> Vec<u8> {
        let (client, server) = tokio::io::duplex(64 * 1024);
        tokio::spawn(handle_connection(
            server, lookup, "0.1.0".into(), 0, noop_metrics(), "tcp",
        ));

        let (mut rd, mut wr) = tokio::io::split(client);
        wr.write_all(input).await.unwrap();
        wr.write_all(b"quit\r\n").await.unwrap();

        let mut out = Vec::new();
        rd.read_to_end(&mut out).await.unwrap();
        out
    }

    // --- basic commands ---

    #[tokio::test]
    async fn mg_hit() {
        let lookup = MockLookup::new(&[(b"hello", b"world")]);
        let out = roundtrip(lookup, b"mg hello\r\n").await;
        assert_eq!(out, b"VA 5\r\nworld\r\n");
    }

    #[tokio::test]
    async fn mg_miss() {
        let lookup = MockLookup::new(&[]);
        let out = roundtrip(lookup, b"mg absent\r\n").await;
        assert_eq!(out, b"EN\r\n");
    }

    #[tokio::test]
    async fn mg_with_key_flag() {
        let lookup = MockLookup::new(&[(b"k", b"v")]);
        let out = roundtrip(lookup, b"mg k k\r\n").await;
        assert_eq!(out, b"VA 1 kk\r\nv\r\n");
    }

    #[tokio::test]
    async fn version_command() {
        let lookup = MockLookup::new(&[]);
        let out = roundtrip(lookup, b"version\r\n").await;
        assert_eq!(out, b"VERSION 0.1.0 gen/0\r\n");
    }

    #[tokio::test]
    async fn quit_flushes_and_closes() {
        let lookup = MockLookup::new(&[(b"k", b"v")]);
        // quit closes the connection; we don't drop the write side.
        let (client, server) = tokio::io::duplex(64 * 1024);
        tokio::spawn(handle_connection(server, lookup, "0.1.0".into(), 0, noop_metrics(), "tcp"));

        let (mut rd, mut wr) = tokio::io::split(client);
        wr.write_all(b"mg k\r\nquit\r\n").await.unwrap();

        let mut out = Vec::new();
        rd.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"VA 1\r\nv\r\n");
    }

    #[tokio::test]
    async fn write_command_rejected() {
        let lookup = MockLookup::new(&[]);
        let out = roundtrip(lookup, b"md somekey\r\n").await;
        assert_eq!(out, b"SERVER_ERROR read-only\r\n");
    }

    #[tokio::test]
    async fn write_command_pipelined_does_not_stall() {
        // md/ma have no body (data_len == 0) so they take the Continue path.
        // A command following them in the same pipeline batch must still be
        // dispatched — a parser bug that leaves bytes unconsumed would cause
        // the version response to be silently dropped.
        let lookup = MockLookup::new(&[]);
        let out = roundtrip(lookup, b"md somekey\r\nversion\r\n").await;
        assert_eq!(out, b"SERVER_ERROR read-only\r\nVERSION 0.1.0 gen/0\r\n");
    }

    // --- pipelining ---

    #[tokio::test]
    async fn pipeline_batch_flushed_together() {
        let lookup = MockLookup::new(&[(b"a", b"1"), (b"b", b"2")]);
        let out = roundtrip(lookup, b"mg a\r\nmg b\r\nversion\r\n").await;
        assert_eq!(out, b"VA 1\r\n1\r\nVA 1\r\n2\r\nVERSION 0.1.0 gen/0\r\n");
    }

    // --- ms drain ---

    #[tokio::test]
    async fn ms_body_drained_pipeline_intact() {
        // ms command line + 5-byte body + \r\n, followed by version.
        let lookup = MockLookup::new(&[]);
        let out = roundtrip(lookup, b"ms k 5\r\nhello\r\nversion\r\n").await;
        assert_eq!(out, b"SERVER_ERROR read-only\r\nVERSION 0.1.0 gen/0\r\n");
    }

    // --- parse error ---

    #[tokio::test]
    async fn parse_error_sends_error_and_closes() {
        let lookup = MockLookup::new(&[]);
        let out = roundtrip(lookup, b"BADCMD\r\n").await;
        assert_eq!(out, b"ERROR\r\n");
    }
}
