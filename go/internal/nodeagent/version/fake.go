package version

import (
	"context"
	"sync"
)

// FakeChecker records calls and returns immediately (or an injected error).
type FakeChecker struct {
	mu    sync.Mutex
	Calls []CheckCall
	err   error
}

type CheckCall struct {
	Dataset   string
	VersionID string
}

func (f *FakeChecker) WaitForVersion(_ context.Context, dataset, versionID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, CheckCall{Dataset: dataset, VersionID: versionID})
	return f.err
}

func (f *FakeChecker) InjectError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}
