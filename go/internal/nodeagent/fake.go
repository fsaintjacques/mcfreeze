package nodeagent

import (
	"context"
	"sync"

	"frostmap.io/fmtctl/api"
)

// FakeAssignmentSource returns pre-configured responses via a channel.
// Send an AssignmentsResponse to Responses to unblock FetchAssignments.
type FakeAssignmentSource struct {
	Responses chan *api.AssignmentsResponse

	mu    sync.Mutex
	calls int
}

func NewFakeAssignmentSource() *FakeAssignmentSource {
	return &FakeAssignmentSource{
		Responses: make(chan *api.AssignmentsResponse, 8),
	}
}

func (f *FakeAssignmentSource) FetchAssignments(ctx context.Context, generation int64) (*api.AssignmentsResponse, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()

	select {
	case resp := <-f.Responses:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *FakeAssignmentSource) CallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// FakeStateReporter records every reported NodeState.
type FakeStateReporter struct {
	mu     sync.Mutex
	States []api.NodeState
	err    error
}

func (f *FakeStateReporter) ReportState(_ context.Context, state api.NodeState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.States = append(f.States, state)
	return f.err
}

func (f *FakeStateReporter) InjectError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *FakeStateReporter) LastState() (api.NodeState, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.States) == 0 {
		return api.NodeState{}, false
	}
	return f.States[len(f.States)-1], true
}

// FakeVersionChecker records calls and returns immediately (or an injected error).
type FakeVersionChecker struct {
	mu    sync.Mutex
	Calls []VersionCheckCall
	err   error
}

type VersionCheckCall struct {
	Dataset   string
	VersionID string
}

func (f *FakeVersionChecker) WaitForVersion(_ context.Context, dataset, versionID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, VersionCheckCall{Dataset: dataset, VersionID: versionID})
	return f.err
}

func (f *FakeVersionChecker) InjectError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}
