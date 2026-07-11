// SPDX-License-Identifier: Apache-2.0

// Package builder provides the AsyncBuilder interface and implementations
// for running snapshot builds (fork, Kubernetes Job, fake).
package builder

import (
	"context"

	"github.com/fsaintjacques/mcfreeze/go/api"
)

// Handle is an opaque identifier for an in-flight build. Its meaning
// depends on the Async implementation (output dir path for fork,
// Job name for K8s, synthetic key for tests). Persisted in
// DatasetVersion.status.buildJob so it survives control-plane restarts.
type Handle string

// Phase represents the current phase of a build.
type Phase string

const (
	Running  Phase = "running"
	Complete Phase = "complete"
	Failed   Phase = "failed"
	NotFound Phase = "not_found"
)

// Result holds the output of a completed build.
type Result struct {
	SnapshotPath string // on-disk location of the snapshot (Fork)
	PVName       string // Kubernetes PersistentVolume name (Job)
	Descriptor   string // base64-encoded FileDescriptorSet (empty for raw encoding)
	MessageName  string // fully-qualified protobuf message name (empty for raw encoding)
}

// Status is the current status of a build as returned by Poll.
type Status struct {
	Phase  Phase
	Result Result // set when Phase == Complete
	Error  string // set when Phase == Failed
}

// Async builds snapshots asynchronously. Implementations must be
// safe for concurrent use.
type Async interface {
	// Start kicks off a build. Idempotent: if a build for this
	// (dataset, versionID) is already running, returns the existing handle.
	//
	// Callers must serialize Start calls for the same (dataset, versionID).
	// The orchestrator guarantees this — the builder does not need internal
	// locking for idempotency checks.
	Start(ctx context.Context, spec api.DatasetSpec, versionID string) (Handle, error)

	// Poll checks the current status of a build. Implementations may perform
	// one-time side effects on first completion detection (e.g., finalizing
	// storage). Callers must tolerate retries — Poll must be idempotent.
	Poll(ctx context.Context, handle Handle) (Status, error)

	// Cancel stops a running build and cleans up resources. Best-effort:
	// the build may complete before cancellation takes effect.
	Cancel(ctx context.Context, handle Handle) error
}
