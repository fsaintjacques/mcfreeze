package nodeagent

import (
	"context"

	"frostmap.io/fmtctl/api"
)

// AssignmentSource provides dataset assignments for this node.
type AssignmentSource interface {
	// FetchAssignments blocks until the assignment generation changes
	// (long-poll) or ctx is cancelled.
	FetchAssignments(ctx context.Context, generation int64) (*api.AssignmentsResponse, error)
}

// StateReporter sends the node's current state to the control-plane.
type StateReporter interface {
	ReportState(ctx context.Context, state api.NodeState) error
}

// VersionChecker confirms a dataset version is active on the KV server.
type VersionChecker interface {
	// WaitForVersion polls the KV server until it reports the given version
	// for the dataset, or ctx is cancelled.
	WaitForVersion(ctx context.Context, dataset, versionID string) error
}
