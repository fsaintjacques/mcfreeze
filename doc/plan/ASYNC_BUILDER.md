<!-- SPDX-License-Identifier: Apache-2.0 -->

# Async Builder: Restartable Snapshot Builds

## Goal

Replace the synchronous `VersionBuilder` interface with an async,
handle-based `AsyncBuilder` that survives control-plane restarts. Builds
are long-running (minutes for BigQuery export + snapshot write). If the
control-plane restarts mid-build, it must recover in-flight builds by
polling their persisted handles.

## Non-goals (deferred)

- **Automatic retry policy**: failed builds stay failed; the operator (or a
  future policy layer) triggers a new build manually. The interface supports
  retries — nothing prevents calling `StartBuild` with a new versionID —
  but automatic retry logic is out of scope.
- **K8s Job implementation**: the interface is designed to support it, but
  only the fork builder is implemented in these chunks.
- **Concurrent builds for the same dataset**: `Store.CreateVersion` already
  rejects a second building version for the same dataset. One build at a
  time per dataset, enforced by the store.
- **Build artifact caching / multi-node build distribution.**
- **Metrics / structured observability**: `ReconcileBuilds` logs state
  transitions via `slog.Info` / `slog.Error`. A metrics layer is added
  when we have a metrics stack.

## Interface

```go
type BuildHandle string

type BuildPhase string

const (
    BuildRunning  BuildPhase = "running"
    BuildComplete BuildPhase = "complete"
    BuildFailed   BuildPhase = "failed"
    BuildNotFound BuildPhase = "not_found"
)

type BuildResult struct {
    SnapshotPath string // on-disk location of the snapshot
}

type BuildStatus struct {
    Phase  BuildPhase
    Result BuildResult // set when Phase == BuildComplete
    Error  string      // set when Phase == BuildFailed
}

type AsyncBuilder interface {
    // Start kicks off a build. Idempotent: if a build for this
    // (dataset, versionID) is already running, returns the existing handle.
    //
    // Callers must serialize Start calls for the same (dataset, versionID).
    // The orchestrator guarantees this — the builder does not need internal
    // locking for idempotency checks.
    Start(ctx context.Context, spec api.DatasetSpec, versionID string) (BuildHandle, error)

    // Poll checks the current status of a build. Pure read, no side effects.
    Poll(ctx context.Context, handle BuildHandle) (BuildStatus, error)

    // Cancel stops a running build and cleans up resources (partial snapshot
    // dir, provisioned disk). Best-effort: the build may complete before
    // cancellation takes effect.
    //
    // Cancel sends SIGTERM (fork) or deletes the Job (K8s), waits for the
    // process to exit (grace period: 10s, then SIGKILL for fork), and only
    // then removes partial output. The process is guaranteed dead before
    // any filesystem cleanup.
    Cancel(ctx context.Context, handle BuildHandle) error
}
```

The builder produces `SnapshotPath` only. It never touches PVName. The
orchestrator owns the translation from snapshot path to volume handle — see
"PVName / symlink ownership" below.

## Handle conventions

The handle is an opaque string whose meaning depends on the implementation.
It is persisted in the store on `VersionEntry.BuildHandle` so it survives
restarts.

| Impl | Handle value | Ground truth for completion |
|------|-------------|---------------------------|
| Fork | output dir path (`<base>/<dataset>/<version>`) | `meta.json` exists in dir |
| K8s Job (future) | Job resource name | Job status conditions |
| Fake (tests) | synthetic key | in-memory map |

## Store changes

Add to `VersionEntry`:

```go
type VersionEntry struct {
    api.VersionRecord
    SnapshotPath string
    BuildHandle  BuildHandle // non-empty while State == building
}
```

New methods:

- `SetBuildHandle(dataset, versionID string, handle BuildHandle) error`
- `GetBuildingVersions() []VersionEntry` — returns all versions in building
  state across all datasets; used by the reconciliation loop

### Known limitation: in-memory store

The store is currently in-memory. A control-plane restart loses all state,
including build handles. This means a restart during a build orphans the
builder process (fork) or Job (K8s) — nobody polls it. The fork builder's
process keeps running and completes, but the result is never picked up.

This is a known Phase 4 gap. Once the store is backed by Kubernetes CRDs,
restart recovery works automatically: `ReconcileBuilds` finds the building
versions with their handles and resumes polling. The `BuildHandle` design
is forward-compatible with that transition.

## Orchestrator changes

Replace `Builder VersionBuilder` with `Builder AsyncBuilder`.

`NewOrchestrator` takes `AsyncBuilder` instead of `VersionBuilder`.
`Run(ctx)` is a blocking loop that the caller launches in a goroutine,
same as `Serve()`:

```go
go orch.Run(ctx)   // reconciliation loop
go srv.Serve()     // HTTP server (already exists)
```

They are independent: `Serve` handles HTTP requests, `Run` drives build
reconciliation. Both stop when ctx is cancelled or the server is closed.

### New methods

- `StartBuild(ctx, spec, versionID) error` — registers dataset, creates
  version in building state, calls `Builder.Start`, persists handle
- `ReconcileBuilds(ctx) error` — iterates `GetBuildingVersions`, polls
  each handle, transitions complete builds to ready (and promotes), marks
  failed builds
- `Run(ctx) error` — calls `ReconcileBuilds` on a ticker; blocks until
  ctx is cancelled

`BuildAndPromote` is kept as a synchronous convenience for tests: it calls
`StartBuild`, then loops `ReconcileBuilds` until the version leaves
building state, then promotes.

### Run loop: tick-with-skip

`Run` uses a `time.Ticker` (default 5s) but only schedules the next tick
after the current `ReconcileBuilds` call returns. If a reconciliation takes
longer than 5s, ticks are skipped — not piled up. No concurrent
reconciliation runs.

```
tick → ReconcileBuilds() → (wait for completion) → tick → ...
```

### PVName / symlink ownership

The **orchestrator** owns PVName generation and symlink creation, not the
builder. When `ReconcileBuilds` sees a `BuildComplete` result:

1. Derives `pvName = fmt.Sprintf("pv-%s-%s", dataset, versionID)`
2. Creates symlink: `os.Symlink(result.SnapshotPath, volumeBase/pvName)`
3. Calls `Store.MarkReady(dataset, versionID, snapshotPath, pvName)`

This is the same logic currently in `BuildAndPromote` (lines 73–85 of
`orchestrator.go`), moved into the reconciliation path. For K8s (future),
step 2 becomes "create PV from DiskURL" — same boundary, different backend.

### Build timeout

The orchestrator enforces a per-build deadline via `VersionRecord.CreatedAt`.
If a build has been in `building` state longer than `BuildTimeout` (global
default on the orchestrator, overridable per dataset in the future),
`ReconcileBuilds` calls `Cancel` and marks the version failed with
"build timeout exceeded".

### Orphan detection

If `Poll` returns `BuildNotFound` (process gone, Job deleted externally),
the orchestrator marks the version failed with "build handle not found;
orphaned".

### Restart recovery

No special recovery code. On startup, `Run` calls `ReconcileBuilds` which
polls every building version's handle. The builder impl checks ground truth
(pid + meta.json for fork, Job status for K8s). Same code path hot or cold.

### Graceful shutdown

When ctx is cancelled, `Run` returns. It does **not** cancel in-flight
builds — child processes (fork) or K8s Jobs keep running. On next startup,
`ReconcileBuilds` picks them up. The builds are the expensive part; don't
kill them just because the orchestrator is cycling.

## Error handling

### Principles

- The builder is dumb: one attempt, report outcome, clean up on failure.
- Failed versions are immutable audit trail. A retry creates a fresh
  `VersionRecord` in building state (per existing convention).
- Automatic retry is deferred (see Non-goals).

### Cleanup

The builder cleans up its own resources on failure:
- Fork: removes partial snapshot directory
- K8s: deletes provisioned disk if build didn't complete

The orchestrator cleans up on retirement (already exists via
`DeleteVersion`).

## Fork builder

New file: `fork_builder.go`.

```go
type ForkBuilder struct {
    MCFBinary     string        // path to mcf binary, default "mcf"
    OutputBase   string        // <OutputBase>/<dataset>/<versionID>/
    GracePeriod  time.Duration // SIGTERM → SIGKILL wait, default 10s
}
```

**Handle** = output dir path (deterministic from dataset + versionID, so
Start is naturally idempotent).

**Start:**
1. Compute `outDir = OutputBase/dataset/versionID`
2. If `outDir/meta.json` exists, return handle (already complete)
3. If `outDir/.build.pid` exists and process alive, return handle (already
   running)
4. Clean up stale state if needed (dead pid, no meta.json)
5. `cmd.Start()` the mcf process, write pid to `outDir/.build.pid`
6. Return `BuildHandle(outDir)`

**Poll:**
1. If `handle/meta.json` exists: `BuildComplete` with
   `SnapshotPath = string(handle)`
2. Read pid from `handle/.build.pid`; if missing: `BuildNotFound`
3. If process alive: `BuildRunning`
4. Process dead, no meta.json: `BuildFailed`

**Cancel:**
1. Read pid, send SIGTERM
2. Wait up to `GracePeriod` for process to exit; SIGKILL if still alive
3. Wait for process to exit (guaranteed dead)
4. Remove partial output directory and pid file

## Fake builder (tests)

Rewrite `fake_builder.go` to implement `AsyncBuilder`. Runs `mcf load csv`
synchronously in `Start` (fast for test data), stores `BuildComplete` in
an internal map. `Poll` returns stored result. Existing integration tests
work unchanged via the `BuildAndPromote` convenience wrapper.

## Files

| File | Action |
|------|--------|
| `go/internal/controlplane/builder.go` | Rewrite: `AsyncBuilder` interface, types |
| `go/internal/controlplane/store.go` | Add `BuildHandle` to `VersionEntry`, add `SetBuildHandle`, `GetBuildingVersions` |
| `go/internal/controlplane/orchestrator.go` | Replace `VersionBuilder` with `AsyncBuilder`; add `StartBuild`, `ReconcileBuilds`, `Run`; rewrite `BuildAndPromote` as sync wrapper; move symlink logic into reconciliation path |
| `go/internal/controlplane/fake_builder.go` | Rewrite: implement `AsyncBuilder`, sync completion |
| `go/internal/controlplane/fork_builder.go` | New: `ForkBuilder` with pid file convention |
| `go/internal/controlplane/store_test.go` | Tests for `SetBuildHandle`, `GetBuildingVersions` |
| `go/internal/controlplane/fork_builder_test.go` | New: start, poll, cancel, restart recovery |
| `go/internal/controlplane/integration_test.go` | Adjust if needed (`BuildAndPromote` preserved) |

## Implementation chunks

### Chunk 1: Interface + Store + Fake + Orchestrator

Atomic change: the new interface, store fields, fake builder, and
orchestrator wiring all land together so the repo compiles and tests pass
at every commit.

Files:
- `builder.go`: new types (`BuildHandle`, `BuildPhase`, `BuildStatus`,
  `BuildResult`) and `AsyncBuilder` interface; remove old `VersionBuilder`
- `store.go`: add `BuildHandle` field to `VersionEntry`; add
  `SetBuildHandle` and `GetBuildingVersions` methods
- `fake_builder.go`: rewrite to implement `AsyncBuilder` (synchronous
  completion in `Start`, result stored in map)
- `orchestrator.go`: replace `Builder VersionBuilder` with
  `Builder AsyncBuilder`; update `NewOrchestrator`; add `StartBuild`,
  `ReconcileBuilds`, `Run`; rewrite `BuildAndPromote` as sync wrapper
  that calls `StartBuild` then loops `ReconcileBuilds` + `Promote`;
  move symlink/PVName logic from `BuildAndPromote` into `ReconcileBuilds`
- `store_test.go`: tests for `SetBuildHandle` and `GetBuildingVersions`
- `integration_test.go`: adjust if needed

Exit: package compiles, all existing tests pass, new store tests pass.

### Chunk 2: Fork builder

New capability. Fully additive — nothing existing depends on it.

Files:
- `fork_builder.go`: `ForkBuilder` implementation with pid file convention
- `fork_builder_test.go`: start/poll/cancel, and simulated restart recovery

Restart recovery test procedure: start a real `mcf load csv` subprocess via
`ForkBuilder.Start`, drop the `ForkBuilder` instance (simulating a
control-plane restart), create a new `ForkBuilder` with the same
`OutputBase`, call `Poll` with the original handle — verify it detects the
running (or completed) build and returns the correct status.

Exit: fork builder tests pass, including restart recovery and cancel
cleanup.
