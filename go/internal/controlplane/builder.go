package controlplane

import (
	"context"

	"frostmap.io/fmtctl/api"
)

// BuildHandle is an opaque identifier for an in-flight build. Its meaning
// depends on the AsyncBuilder implementation (output dir path for fork,
// Job name for K8s, synthetic key for tests). Persisted in VersionEntry
// so it survives control-plane restarts.
type BuildHandle string

// BuildPhase represents the current phase of a build.
type BuildPhase string

const (
	BuildRunning  BuildPhase = "running"
	BuildComplete BuildPhase = "complete"
	BuildFailed   BuildPhase = "failed"
	BuildNotFound BuildPhase = "not_found"
)

// BuildResult holds the output of a completed build.
type BuildResult struct {
	SnapshotPath string // on-disk location of the snapshot (ForkBuilder)
	PVName       string // Kubernetes PersistentVolume name (JobBuilder)
	Descriptor   string // base64-encoded FileDescriptorSet (empty for raw encoding)
	MessageName  string // fully-qualified protobuf message name (empty for raw encoding)
}

// BuildStatus is the current status of a build as returned by Poll.
type BuildStatus struct {
	Phase  BuildPhase
	Result BuildResult // set when Phase == BuildComplete
	Error  string      // set when Phase == BuildFailed
}

// AsyncBuilder builds snapshots asynchronously. Implementations must be
// safe for concurrent use.
type AsyncBuilder interface {
	// Start kicks off a build. Idempotent: if a build for this
	// (dataset, versionID) is already running, returns the existing handle.
	//
	// Callers must serialize Start calls for the same (dataset, versionID).
	// The orchestrator guarantees this — the builder does not need internal
	// locking for idempotency checks.
	Start(ctx context.Context, spec api.DatasetSpec, versionID string) (BuildHandle, error)

	// Poll checks the current status of a build. Implementations may perform
	// one-time side effects on first completion detection (e.g., finalizing
	// storage). Callers must tolerate retries — Poll must be idempotent.
	Poll(ctx context.Context, handle BuildHandle) (BuildStatus, error)

	// Cancel stops a running build and cleans up resources. Best-effort:
	// the build may complete before cancellation takes effect.
	Cancel(ctx context.Context, handle BuildHandle) error
}
