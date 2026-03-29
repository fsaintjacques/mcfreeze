package mount

import (
	"context"
	"fmt"
	"sync"
)

// FakeMounter is an in-memory Mounter for use in tests.
type FakeMounter struct {
	mu sync.Mutex

	// mounted maps target → device for currently-mounted paths.
	mounted map[string]string
	// Calls records every method invocation in order.
	Calls []MountCall

	// errors maps call index → error to inject.
	errors map[int]error
}

// MountCall records a single Mounter method invocation.
type MountCall struct {
	Op     string // "mount", "unmount"
	Device string
	Target string
}

// NewFakeMounter returns a ready-to-use FakeMounter.
func NewFakeMounter() *FakeMounter {
	return &FakeMounter{
		mounted: make(map[string]string),
		errors:  make(map[int]error),
	}
}

// InjectError causes the n-th call (0-indexed across all methods) to return err.
func (f *FakeMounter) InjectError(n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errors[n] = err
}

// IsMounted reports whether target is currently mounted.
func (f *FakeMounter) IsMounted(target string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.mounted[target]
	return ok
}

func (f *FakeMounter) Mount(_ context.Context, device, target string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	call := MountCall{Op: "mount", Device: device, Target: target}
	if err := f.nextError(); err != nil {
		f.Calls = append(f.Calls, call)
		return err
	}
	if _, ok := f.mounted[target]; ok {
		f.Calls = append(f.Calls, call)
		return fmt.Errorf("fake: %s already mounted", target)
	}
	f.mounted[target] = device
	f.Calls = append(f.Calls, call)
	return nil
}

func (f *FakeMounter) Unmount(_ context.Context, target string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	call := MountCall{Op: "unmount", Target: target}
	if err := f.nextError(); err != nil {
		f.Calls = append(f.Calls, call)
		return err
	}
	delete(f.mounted, target)
	f.Calls = append(f.Calls, call)
	return nil
}

func (f *FakeMounter) nextError() error {
	idx := len(f.Calls)
	if err, ok := f.errors[idx]; ok {
		return err
	}
	return nil
}
