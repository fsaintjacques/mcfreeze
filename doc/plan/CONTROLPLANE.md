# Control-Plane Implementation Plan

## Goal

Implement the control-plane component described in ORCHESTRATION.md: the
single authority that manages dataset versions, assigns them to nodes, and
drives convergence. The implementation is phased so that each phase produces
a testable, integrated system — no phase requires Kubernetes.

## Existing foundation

The node-agent and KV server are fully implemented and integration-tested.
The control-plane slots in by implementing the two HTTP endpoints the
node-agent already calls (`GET /assignments`, `POST /state`) and adding the
version lifecycle on top.

## Phases

### Phase 1 — Interfaces, HTTP API shell, full-loop integration test

Extract the `VersionBuilder` interface (build a snapshot from a dataset
spec). Implement a `FakeVersionBuilder` that shells out to `mcf load csv`.
Build the control-plane HTTP server with in-memory state: assignments
endpoint (long-poll), state reporting endpoint, and a programmatic API to
trigger builds and promotions from tests.

**Exit criterion:** A single integration test exercises the entire pipeline:
test triggers a build → `FakeVersionBuilder` produces a snapshot →
control-plane promotes the version → node-agent picks up the assignment via
long-poll → attaches, mounts, writes catalog → KV server loads the snapshot
→ node-agent confirms via `/version` → reports `PhaseActive` → control-plane
receives the report → test verifies memcache lookups return the expected
data.

### Phase 2 — Version state machine

Implement the `VersionRecord` lifecycle: `building → ready → active →
retired` (and `failed`). The control-plane tracks all versions per dataset,
enforces valid transitions, creates PV references on `ready`, and promotes
exactly one version to `active` at a time. Retry semantics: a retry creates
a fresh `VersionRecord` rather than reusing the failed one.

**Exit criterion:** Unit tests cover every state transition, invalid
transitions are rejected, and the Phase 1 integration test still passes with
the state machine enforcing the lifecycle.

### Phase 3 — Rollout and retirement

Diff reported `NodeState` against desired assignments. Drive progressive
rollout: the control-plane only promotes a new version to `active` once a
configurable fraction of nodes have converged on the previous version. Retire
old versions once all nodes report the new version as active and the
retention window has elapsed.

**Exit criterion:** Integration test with multiple simulated nodes (multiple
node-agent instances or a single agent reporting for multiple node names).
The control-plane advances the rollout as nodes converge and retires the old
version.

### Phase 4 — Kubernetes integration (CRDs + Job runner)

Replace in-memory state with Kubernetes CRDs (`DatasetSpec`, `VersionRecord`
as custom resources). Implement the real `VersionBuilder` that creates a
Kubernetes Job running `mcf load bq`. The in-memory backend remains as the
test double — integration tests never require a Kubernetes cluster.

**Exit criterion:** The control-plane can be deployed to a Kubernetes cluster
and drive the full lifecycle using real CRDs and Jobs. Integration tests
continue to pass using the in-memory backend.
