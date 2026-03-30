# Control-Plane Phase 3: Rollout and Retirement

## Goal

Make the control-plane aware of fleet-wide convergence. Phase 2 promotes a
version and pushes assignments to all nodes simultaneously. Phase 3 adds
the ability to track which nodes have converged, drive progressive rollout,
and retire old versions once all nodes have moved on.

## Rollout

When a version is promoted, the control-plane doesn't assume all nodes pick
it up instantly. Instead it:

1. Pushes the assignment to all registered nodes (already done in Phase 2)
2. Monitors `NodeState` reports to track per-node convergence
3. Exposes rollout status: how many nodes are on the new version, how many
   are still on the old one, how many are in error

For Phase 3 the rollout strategy is **all-at-once** — all nodes get the
assignment immediately. Progressive rollout (canary, percentage-based) is
a future enhancement that builds on the same convergence tracking.

## Retirement

A retired version's disk and PV can be deleted once:

1. **All nodes** report `PhaseActive` for the new version (or the dataset
   is no longer assigned to that node)
2. A **retention window** has elapsed (configurable per dataset, default 0)

Phase 2 already transitions the old version to `retired` state when a new
one is promoted. Phase 3 adds the convergence check and cleanup trigger.

## Changes to Store

New methods:

- `RolloutStatus(dataset string) RolloutStatus` — returns per-version node
  counts by diffing assignments against reported NodeStates
- `CheckRetirement(dataset string) []VersionEntry` — returns retired
  versions eligible for cleanup (all nodes converged, retention elapsed)
- `DeleteVersion(dataset, versionID string) error` — removes a retired
  version from the store (the actual disk/PV cleanup is the caller's job)

New type:

```go
type RolloutStatus struct {
    Dataset        string
    ActiveVersion  string
    NodeCounts     map[string]int // version_id → count of nodes on that version
    PendingNodes   []string       // nodes not yet reporting the active version
    ConvergedNodes []string       // nodes reporting the active version
    ErrorNodes     []string       // nodes in error state
}
```

## Changes to Orchestrator

Add a convergence polling method for tests:

- `WaitForConvergence(ctx, dataset, versionID string) error` — polls
  `RolloutStatus` until all registered nodes report the version as active

## Changes to HTTP API

Admin endpoints:

- `GET /admin/dataset/{name}/rollout` — returns `RolloutStatus`
- `GET /admin/dataset/{name}/retired` — returns retired versions eligible
  for cleanup

## Integration test

Extend `TestFullLoop` or add a new test with **two simulated nodes**:

1. Register node-1 and node-2
2. Start two node-agent instances (each with its own FSVolumeManager,
   FSMounter, but sharing the same KV server — or separate KV servers)
3. BuildAndPromote v1 → both nodes receive the assignment
4. Wait for both nodes to report PhaseActive
5. Verify `RolloutStatus` shows 2/2 converged
6. BuildAndPromote v2 → both nodes converge
7. Verify v1 is eligible for retirement (all nodes on v2)
8. Call `DeleteVersion` for v1

The tricky part is running two node-agents against the same control-plane.
Each needs its own:
- FSVolumeManager (separate volume base dirs)
- FSMounter (separate mount base dirs)
- KV server (separate catalog dirs — each node has its own KV server pod)

But they share the same control-plane HTTP server.

## Files

| File | Action |
|------|--------|
| `go/internal/controlplane/store.go` | Add RolloutStatus, CheckRetirement, DeleteVersion |
| `go/internal/controlplane/orchestrator.go` | Add WaitForConvergence |
| `go/internal/controlplane/server.go` | Add rollout/retired admin endpoints |
| `go/internal/controlplane/store_test.go` | Unit tests for rollout status and retirement |
| `go/internal/controlplane/integration_test.go` | Multi-node convergence test |

## Exit criterion

Integration test with two nodes: build → promote → both converge →
RolloutStatus shows 2/2 → upgrade → both converge again → old version
eligible for retirement.
