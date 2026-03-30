# Control-Plane Phase 2: Version State Machine

## Goal

Add lifecycle enforcement to the control-plane so that `VersionRecord`
transitions follow the state machine defined in ORCHESTRATION.md. Phase 1
lets tests push arbitrary assignments; Phase 2 ensures versions move through
`building → ready → active → retired` (and `failed`) with valid transitions
only.

## State machine (from ORCHESTRATION.md)

```
[building] ──(job succeeds)──▶ [ready] ──(promote)──▶ [active]
    │                              │                       │
    └──(job fails)──▶ [failed]     └──(PV created)         │
                          │                     (all nodes converged
                          └──(retry)──▶ [building]  + retention)──▶ [retired]
```

- `building`: a build is in progress (builder has been called)
- `ready`: snapshot exists, PV name is set, not yet served by any node
- `active`: the version the control-plane wants nodes to serve
- `retired`: superseded, pending disk cleanup (Phase 3)
- `failed`: build error; a retry creates a fresh VersionRecord

Only one version per dataset can be `active` at a time. Promoting a new
version to `active` implicitly moves the previous `active` version to
`retired` (retirement cleanup is Phase 3).

## Changes to Store

Add version tracking to the store:

```go
type VersionRecord struct {
    api.VersionRecord
    SnapshotPath string // local path to the snapshot directory
}

// Per-dataset version list, ordered by creation time.
versions map[string][]VersionRecord // keyed by dataset name
specs    map[string]api.DatasetSpec // keyed by dataset name
```

New methods:
- `RegisterDataset(spec DatasetSpec)` — registers a dataset spec
- `GetDatasetSpec(name string) (DatasetSpec, bool)`
- `CreateVersion(dataset, versionID string) error` — creates a record in
  `building` state; rejects if a `building` version already exists for this
  dataset
- `MarkReady(dataset, versionID, snapshotPath, pvName string) error` —
  transitions `building → ready`; rejects if not in `building`
- `Promote(dataset, versionID string) error` — transitions `ready → active`;
  moves the current `active` version (if any) to `retired`; updates
  assignments for all registered nodes
- `MarkFailed(dataset, versionID string, reason string) error` —
  transitions `building → failed`
- `GetVersions(dataset string) []VersionRecord`
- `GetActiveVersion(dataset string) (VersionRecord, bool)`

All transitions validate the current state and return an error on invalid
transitions.

## Changes to Orchestrator

`BuildAndPromote` becomes a multi-step flow using the state machine:

```go
func (o *Orchestrator) BuildAndPromote(ctx, spec, versionID, nodeName) error {
    o.Store.RegisterDataset(spec)
    o.Store.CreateVersion(spec.Name, versionID)

    snapPath, err := o.Builder.Build(ctx, spec, versionID)
    if err != nil {
        o.Store.MarkFailed(spec.Name, versionID, err.Error())
        return err
    }

    pvName := symlink snapshot into VolumeBase
    o.Store.MarkReady(spec.Name, versionID, snapPath, pvName)
    o.Store.Promote(spec.Name, versionID)
    // Promote internally updates assignments for all registered nodes
    return nil
}
```

The `Promote` method on the store handles assignment updates atomically —
the orchestrator no longer manipulates assignments directly.

## Changes to HTTP API

Add version management endpoints (admin API for now):

- `POST /admin/dataset` — register a dataset spec
- `POST /admin/dataset/{name}/version/{id}/promote` — trigger promote
- `GET /admin/dataset/{name}/versions` — list versions with states

These are admin/test endpoints. The production trigger flow (Phase 4) will
use CRD watches instead.

## Testing

### Unit tests (store_test.go)

- Valid transition: building → ready → active
- Invalid transition: ready → building (rejected)
- Promote moves previous active to retired
- Only one building version per dataset at a time
- CreateVersion after failed version succeeds (new record)
- MarkFailed from building
- Promote updates assignments for registered nodes

### Integration test

Extend `TestFullLoop` to use the state machine flow:
- `CreateVersion` → `Build` → `MarkReady` → `Promote`
- Verify the assignment is served via long-poll only after `Promote`
- Build a v2 while v1 is active → promote v2 → verify v1 moves to retired

## Files

| File | Action |
|------|--------|
| `go/internal/controlplane/store.go` | Add version/dataset tracking, state transitions |
| `go/internal/controlplane/orchestrator.go` | Use state machine in BuildAndPromote |
| `go/internal/controlplane/server.go` | Add admin version endpoints |
| `go/internal/controlplane/store_test.go` | Add state machine unit tests |
| `go/internal/controlplane/integration_test.go` | Verify full flow with state machine |

## Exit criterion

All state transitions are enforced with unit tests. The Phase 1 full-loop
integration test still passes, now going through the state machine.
`Promote` atomically updates assignments — no direct assignment
manipulation outside the store.
