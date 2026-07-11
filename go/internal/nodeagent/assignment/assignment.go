// SPDX-License-Identifier: Apache-2.0

// Package assignment provides the control-plane assignment and state
// reporting interfaces used by the node-agent.
package assignment

import (
	"context"

	"github.com/fsaintjacques/mcfreeze/go/api"
)

// Source provides dataset assignments for a node.
type Source interface {
	// FetchAssignments blocks until the assignment generation changes
	// (long-poll) or ctx is cancelled.
	FetchAssignments(ctx context.Context, generation int64) (*api.AssignmentsResponse, error)
}

// StateReporter sends the node's current state to the control-plane.
type StateReporter interface {
	ReportState(ctx context.Context, state api.NodeState) error
}
