package assignment

import (
	"context"
	"sync"

	"github.com/fsaintjacques/mcfreeze/go/api"
)

// FakeSource returns pre-configured responses via a channel.
// Send an AssignmentsResponse to Responses to unblock FetchAssignments.
type FakeSource struct {
	Responses chan *api.AssignmentsResponse

	mu    sync.Mutex
	calls int
}

func NewFakeSource() *FakeSource {
	return &FakeSource{
		Responses: make(chan *api.AssignmentsResponse, 8),
	}
}

func (f *FakeSource) FetchAssignments(ctx context.Context, generation int64) (*api.AssignmentsResponse, error) {
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

func (f *FakeSource) CallCount() int {
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
