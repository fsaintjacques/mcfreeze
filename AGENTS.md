# Agent Instructions

## Project

Immutable key-value snapshots built from BigQuery, served at sub-millisecond
latency over the memcache meta protocol. Rust owns the data plane (writing
and reading snapshots). Go owns the control plane (orchestrating builds,
managing versions, driving rollout to nodes).

## Workspace layout

```
mcfreeze/
├── rust/crates/
│   ├── mcfreeze-format/    # Snapshot reader/writer (mmap, Robin Hood index, pread)
│   ├── mcfreeze-loader/    # Parallel scatter-gather pipeline (KvSource → SnapshotWriter)
│   ├── mcfreeze-bq/        # BigQuery Storage Read API source
│   ├── mcfreeze-server/    # KV server (memcache meta protocol, catalog.json hot-swap)
│   └── mcfreeze-cli/       # `mcf` binary: load, serve, inspect, verify
├── go/
│   ├── api/                # Shared wire types (VersionRecord, NodeAssignment, CatalogEntry, ...)
│   ├── internal/
│   │   ├── controlplane/   # In-memory store, HTTP server, orchestrator, builders
│   │   ├── nodeagent/      # Reconciliation loop: attach, mount, catalog write, state report
│   │   ├── volume/         # VolumeManager interface (K8s VolumeAttachment, FS simulation)
│   │   ├── mount/          # Mounter interface (Linux syscall, FS symlink simulation)
│   │   └── testutil/       # Helpers: BuildSnapshot, StartCatalogServer
│   └── cmd/mcfctl/         # Binary entry point (subcommands: node-agent, control-plane)
└── doc/
    ├── FORMAT.md           # On-disk snapshot format spec
    ├── HIGHLEVEL.md        # Product overview and architecture
    ├── ORCHESTRATION.md    # Control plane components and contracts
    └── plan/               # Implementation plans per feature/phase
```

Binaries: `mcf` (Rust, snapshot builder/server) and `mcfctl` (Go, control plane).

## Build and test

```bash
make all              # Build all binaries (Rust release + Go)
make test             # Run all tests (unit + integration)
make test-unit        # Unit tests only (no binary prereqs)
make test-integration # Integration tests (builds mcf first)
```

Running Go tests directly:

```bash
cd go && go test ./...                              # unit tests
cd go && MCF=/path/to/mcf go test -tags integration ./...  # integration tests
```

Running Rust tests directly:

```bash
cd rust && cargo test
```

Integration tests require the `mcf` binary on PATH or via the `MCF` env var.

## Architecture invariants

- **Rust and Go are independent.** They share no code. The only contract
  is the on-disk format (`meta.json`, `index.all`, `data.bin`) and the
  `catalog.json` file format. The Go side shells out to `mcf` for builds.
- **The control plane never touches disks or nodes directly.** It delegates
  build work to builders (K8s Jobs or forked processes) and node-level work
  to node-agents.
- **Node-agents are converging.** They report full state periodically. A
  missed report never causes permanent divergence. The control plane diffs
  reported state against desired assignments.
- **All interfaces are injected.** `AsyncBuilder`, `VolumeManager`,
  `Mounter`, `AssignmentSource`, `StateReporter` — each has a production
  impl and a test fake/simulation. Integration tests compose real binaries
  with filesystem-based fakes (FSVolumeManager, FSMounter).
- **One build at a time per dataset.** `Store.CreateVersion` rejects a
  second building version for the same dataset.

## Version state machine

```
[building] ──(build succeeds)──> [ready] ──(promote)──> [active] ──(new version promoted)──> [retired]
     │
     └──(build fails)──> [failed]
```

Retries create a fresh `VersionRecord`, not a state transition on the
failed one. Failed versions are immutable audit trail.

## Key conventions

- **Go packages:** `internal/` for all non-API code. `api/` for shared wire
  types imported by both control plane and node-agent.
- **Test fakes vs mocks:** use struct-based fakes (FakeVolumeManager,
  FakeMounter, FakeAsyncBuilder) with call recording, not mock frameworks.
- **Filesystem simulation:** FSVolumeManager (mkdir = attach), FSMounter
  (symlink = mount). Enables integration tests without K8s or root.
- **Conventional commits:** `feat:`, `fix:`, `test:`, `refactor:`, `docs:`
  with optional scope, e.g. `feat(controlplane):`, `fix(format):`.

## On-disk format (snapshot)

```
<snapshot>/
  meta.json             # Partition count, key stats, index offsets
  index.all             # Unified Robin Hood index, 2MB-aligned per partition
  data/
    part-00/data.bin    # Values: 12-byte header + payload, 64-byte aligned
    part-01/data.bin
    ...
```

- Keys are routed by `xxhash64(key) & (n_partitions - 1)`
- Index: 8-byte compact buckets (32-bit fingerprint + 32-bit offset)
- `meta.json` existence is the ground truth for a completed build

## Control plane HTTP API

```
GET  /api/v1/node/{node}/assignments   # Long-poll (generation-based)
POST /api/v1/node/{node}/state         # Full NodeState report
POST /admin/node/{node}/assignments    # Set assignments (admin/test)
GET  /admin/dataset/{name}/rollout     # Rollout convergence status
GET  /admin/dataset/{name}/retired     # Retirement-eligible versions
```

## Commit Conventions

- Use conventional commits: `feat:`, `fix:`, `test:`, `refactor:`, `style:`, `docs:`
- Update README.md when changing user-facing syntax
- Each commit should pass `make check` independently
