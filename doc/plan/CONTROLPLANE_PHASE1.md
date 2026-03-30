# Control-Plane Phase 1: HTTP API Shell + Full-Loop Integration Test

## Goal

Build the minimal control-plane that closes the loop: a test can trigger a
dataset build, the control-plane serves the assignment, the node-agent
reconciles it against a real KV server, and the test verifies end-to-end
data serving. No Kubernetes, no CRDs, no state machine — just the HTTP
wiring and an in-memory store.

## Components

### VersionBuilder interface

```go
// VersionBuilder builds a snapshot for a dataset version.
type VersionBuilder interface {
    Build(ctx context.Context, spec api.DatasetSpec, versionID string) (snapshotPath string, err error)
}
```

**FakeVersionBuilder:** Takes a map of `dataset → []testutil.KV` pairs
configured at construction. `Build` calls `fm load csv` via
`testutil.BuildSnapshot` (reusing existing infrastructure) and returns the
snapshot directory.

Production implementation (Phase 4) will create a Kubernetes Job running
`fm load bq`.

### In-memory store

Holds the full control-plane state in a mutex-protected struct:

```go
type Store struct {
    mu          sync.RWMutex
    datasets    map[string]api.DatasetSpec
    versions    map[string][]api.VersionRecord   // keyed by dataset name
    assignments map[string][]api.NodeAssignment   // keyed by node name
    nodeStates  map[string]api.NodeState          // keyed by node name
    generation  map[string]int64                  // keyed by node name
    notify      map[string]chan struct{}           // per-node wake channels for long-poll
}
```

Methods:
- `RegisterDataset(spec DatasetSpec)`
- `SetAssignment(nodeName string, assignments []NodeAssignment)` — updates
  assignments and bumps the generation; wakes the long-poll channel
- `GetAssignments(nodeName string, afterGeneration int64) (AssignmentsResponse, chan struct{})` —
  returns current assignments if generation > afterGeneration, otherwise
  returns the wake channel for blocking
- `ReportState(nodeName string, state NodeState)`
- `GetNodeState(nodeName string) (NodeState, bool)`
- `SetVersionState(dataset, versionID string, state VersionState)`

The store is intentionally simple — no state machine enforcement (Phase 2),
no rollout logic (Phase 3). Tests manipulate it directly.

### HTTP server

Two endpoints matching the ORCHESTRATION.md API contract:

**`GET /api/v1/node/{node}/assignments?generation=N`**

Long-poll: if the store's generation for this node is > N, return
immediately. Otherwise, block on the node's wake channel until the
generation advances or ctx is cancelled. Returns `AssignmentsResponse`.

**`POST /api/v1/node/{node}/state`**

Accept `NodeState` JSON body, store it. Return 200 OK.

The server also exposes an internal admin API (not part of the production
contract) for tests to push assignments:

**`POST /admin/node/{node}/assignments`**

Body: `[]NodeAssignment`. Sets the assignments for the node and wakes any
blocked long-poll.

### Orchestrator

Ties the pieces together for the integration test:

```go
type Orchestrator struct {
    store   *Store
    builder VersionBuilder
    server  *http.Server
}
```

Methods:
- `BuildAndPromote(ctx, datasetName, versionID, nodeName string)` —
  calls `builder.Build`, creates a `VersionRecord`, sets assignments for the
  node via the store. This is the test-facing entry point.
- `Addr() string` — returns the HTTP server address for node-agent config.

## Files

| File | Contents |
|------|----------|
| `go/internal/controlplane/store.go` | In-memory store |
| `go/internal/controlplane/server.go` | HTTP handlers (assignments, state, admin) |
| `go/internal/controlplane/orchestrator.go` | Orchestrator tying store + builder |
| `go/internal/controlplane/builder.go` | VersionBuilder interface |
| `go/internal/controlplane/fake_builder.go` | FakeVersionBuilder using fm load csv |
| `go/internal/controlplane/store_test.go` | Unit tests for store (long-poll, generation) |
| `go/internal/controlplane/server_test.go` | HTTP handler unit tests |
| `go/internal/controlplane/integration_test.go` | Full-loop integration test |

## Integration test sketch

```
1. Build FakeVersionBuilder with test KV data
2. Create Orchestrator (store + builder + HTTP server on free port)
3. Start KV server (empty catalog) via testutil.StartEmptyCatalogServer
4. Start node-agent:
     - HTTPAssignmentSource → control-plane addr
     - HTTPStateReporter → control-plane addr
     - HTTPVersionChecker → KV server addr
     - FSVolumeManager + FSMounter
5. Call orchestrator.BuildAndPromote("users", "v1", "test-node")
     - FakeVersionBuilder runs fm load csv → snapshot dir
     - Store gets assignment with PVName = snapshot dir (symlinked into volume base)
     - Long-poll wakes the node-agent
6. Wait for control-plane to receive NodeState with PhaseActive
7. Verify memcache mg returns correct data
8. Call orchestrator.BuildAndPromote("users", "v2", "test-node") with new data
9. Wait for convergence to v2
10. Verify memcache returns v2 data
11. Shutdown
```

## Exit criterion

The integration test in step 5–10 passes: build → assign → reconcile →
serve → upgrade → re-serve, with the only fakes being the build step
(CSV instead of BigQuery) and disk provisioning (filesystem instead of
Hyperdisk).
