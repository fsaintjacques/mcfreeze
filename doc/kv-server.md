# KV Server

## Overview

`mcf serve` is a stateless, read-only key-value server. It serves the memcache
meta protocol over a Unix domain socket (UDS) and TCP. It is meant to run as an
unprivileged container in a DaemonSet pod alongside the node-agent.

The server never writes or builds snapshot data. It only:
1. Opens `SnapshotReader` handles against mounted snapshot directories
2. Serves key lookups by probing the Robin Hood index and `pread`-ing from `data.bin`
3. In `catalog` mode: watches `catalog.json` for changes and atomically
   hot-swaps the active snapshot set

---

## Modes

### `snapshot` (development / testing)

```
mcf serve snapshot --dir /mnt/snapshots/v42 --uds /run/kv/kv.sock --tcp 0.0.0.0:7777
```

- Opens a single `SnapshotReader` at startup; never changes
- No file watching, no catalog, no dataset routing, no hot-swap
- Keys are passed to the reader verbatim — no prefix stripping
- Generation is always 0

### `catalog` (production)

```
mcf serve catalog --catalog /run/kv/catalog.json --uds /run/kv/kv.sock --tcp 0.0.0.0:7777
```

- Watches `catalog.json` via filesystem notifications (`notify` crate)
- On change: opens new `SnapshotReader`s, builds a new `ActiveCatalog`,
  atomically swaps the registry
- Keys carry a dataset prefix: `<key_prefix>:<actual-key>`
- If `catalog.json` does not exist at startup, the server starts with an empty
  catalog and loads the first version when the file appears

Both modes share the same protocol, listener, and metrics layers. They differ
only in how the `Lookup` implementation is constructed and whether the
catalog-watcher task is spawned.

---

## catalog.json

Written atomically by the node-agent via `rename(2)` into a shared EmptyDir.
Contains entries for **all** active datasets on the node.

```json
{
  "entries": [
    {
      "dataset": "users",
      "key_prefix": "users",
      "version_id": "v42",
      "mount_path": "/mnt/kv/users/v42"
    }
  ]
}
```

| Field | Description |
|---|---|
| `dataset` | Dataset name |
| `key_prefix` | Routing prefix; must be unique across all entries |
| `version_id` | Opaque version identifier |
| `mount_path` | Absolute path to the mounted snapshot directory |

Duplicate `key_prefix` values within a single catalog file are rejected at
parse time.

---

## Memcache Meta Protocol

The server implements the [memcache meta protocol][meta-proto] (introduced in
memcached 1.6), not the classic text or binary protocols. Retains full
debuggability with `nc` / `telnet`.

[meta-proto]: https://github.com/memcached/memcached/blob/master/doc/protocol.txt

### Commands

| Command | Wire | Notes |
|---|---|---|
| `mg` | `mg <key> [flags]\r\n` | Meta get — one key per command; batch via pipelining |
| `version` | `version\r\n` | Returns `VERSION <semver> gen/<generation>\r\n` |
| `quit` | `quit\r\n` | Clean connection teardown |

The server is read-only. Write commands (`ms`, `md`, `ma`) return
`SERVER_ERROR read-only\r\n`. For `ms`, the server drains the trailing data
block before resuming command parsing.

### `mg` response format

```
# Hit:
VA <size> [echo-flags]\r\n
<value bytes>\r\n

# Miss:
EN\r\n

# Error:
SERVER_ERROR <message>\r\n
```

### Supported `mg` flags

| Flag | Meaning | Server behaviour |
|---|---|---|
| `v` | Return value | Always honoured (value is always returned on hit) |
| `k` | Echo key in response | Echoed as `k<key>` token in the `VA` line |

Unrecognised flags are silently ignored, preserving forward compatibility.

### Batching

The meta protocol batches via pipelining — clients send multiple `mg` commands
back-to-back without waiting for individual responses. The connection handler
reads and dispatches commands in a loop, flushing the write buffer at the end of
each pipeline batch (when the read buffer is drained). This achieves multi-get
semantics without a dedicated multi-key command.

### Key routing (catalog mode only)

In `catalog` mode keys carry a dataset prefix: `<key_prefix>:<actual-key>`.
The lookup splits on the first `:`, validates the prefix as UTF-8, and
dispatches to the matching dataset's `SnapshotReader`.

In `snapshot` mode keys are passed to the reader verbatim.

---

## HTTP Endpoints

Served on the metrics address (`--metrics` flag, e.g. `:7777` in the Helm
chart).

### `GET /version`

Returns the currently loaded datasets and their versions.

```json
{
  "datasets": [
    {
      "dataset": "users",
      "version_id": "v42",
      "loaded_at": "2026-04-10T02:12:05Z"
    }
  ]
}
```

Datasets are sorted by name for deterministic output. The node-agent polls
this endpoint after writing `catalog.json` to confirm the hot-swap completed.

In snapshot mode, returns an empty `datasets` array.

### `GET /metrics`

Prometheus text exposition format. See [Observability](#observability).

---

## Concurrency Model

```
tokio multi-thread runtime
│
├── catalog-watcher task (catalog mode only)
│     notify watcher (spawn_blocking) → mpsc → async reload loop
│     → Registry::swap(new) → old Arc<ActiveCatalog> dropped (mmaps + fds released)
│
├── UDS accept-loop task
│     tokio::net::UnixListener::accept()
│     → factory.for_connection() → spawn connection-handler task
│
├── TCP accept-loop task
│     tokio::net::TcpListener::accept()
│     → factory.for_connection() → spawn connection-handler task
│
├── connection-handler tasks (×N, one per open connection)
│     owns Box<dyn Lookup> for the connection lifetime
│     per request:
│       lookup.get(key).await
│         → spawn_blocking(|| reader.get(key))
│         // Robin Hood probe (mmap, instant) + pread (blocking syscall)
│
└── HTTP server task
      axum handler on :9090 → /metrics + /version
```

### Lookup Trait

```rust
#[async_trait]
pub trait Lookup: Send + 'static {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>, ServeError>;
}
```

Takes `&mut self` so each connection can hold per-connection state (e.g. a
cached `Arc<ActiveCatalog>`) without synchronisation. Each connection gets its
own `Box<dyn Lookup>` from the `LookupFactory`.

### Hot-Swap (catalog mode)

The `Registry` wraps an `ArcSwap<ActiveCatalog>` for lock-free replacement:

- **Read path (per request):** `registry.load()` returns a seqlock-pinned guard.
  The per-connection lookup caches the `Arc<ActiveCatalog>` and only reloads
  when the generation changes — so the guard is held for nanoseconds, not
  across `pread` syscalls.
- **Swap path (catalog watcher):** `registry.swap(new)` atomically replaces the
  catalog and returns the old `Arc`. The watcher drops the old Arc after swap,
  releasing mmaps and file descriptors.

No `Mutex` or `RwLock` on the hot path.

### Blocking I/O

`pread(2)` is a blocking syscall. `SnapshotLookup` offloads it to tokio's
blocking thread pool via `spawn_blocking`. The `Lookup` trait is async;
`spawn_blocking` is the adapter inside the implementation. The
`mcfreeze-format` crate remains a zero-runtime synchronous library.

---

## Observability

All metrics are registered in a single `prometheus_client::Registry`. Served
at `:9090/metrics`.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `fm_request_duration_seconds` | Histogram | `result` | Per-key latency; `result` ∈ {hit, miss, error} |
| `fm_keys_requested_total` | Counter | — | Total key lookups |
| `fm_keys_hit_total` | Counter | — | Key hits |
| `fm_keys_miss_total` | Counter | — | Key misses |
| `fm_response_bytes_total` | Counter | — | Total value bytes sent |
| `fm_catalog_generation` | Gauge | — | Current generation; 0 in snapshot mode |
| `fm_catalog_swap_total` | Counter | `result` | Catalog swaps; `result` ∈ {ok, error} |
| `fm_active_datasets` | Gauge | — | Dataset count; always 1 in snapshot mode |
| `fm_connections_active` | Gauge | `transport` | Open connections; `transport` ∈ {uds, tcp} |
| `fm_connections_total` | Counter | `transport` | Total connections accepted |

Histogram buckets for `fm_request_duration_seconds`:
`[50µs, 100µs, 200µs, 500µs, 1ms, 2ms, 5ms, 10ms, 50ms, 100ms]`
