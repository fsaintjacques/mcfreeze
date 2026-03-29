package volume

import (
	"context"
	"fmt"
	"sync"
)

// FakeVolumeManager is an in-memory VolumeManager for use in tests.
// All methods record their calls so tests can assert on them.
type FakeVolumeManager struct {
	mu sync.Mutex

	// attached maps pvName → device path for disks that have been attached.
	attached map[string]string
	// Calls records every method invocation in order.
	Calls []VolumeCall

	// DevicePath, if set, is returned by WaitForDevice for any disk.
	// Defaults to "/dev/fake-disk".
	DevicePath string
	// errors maps call index → error to inject.
	errors map[int]error
}

// VolumeCall records a single VolumeManager method invocation.
type VolumeCall struct {
	Op     string // "attach", "wait", "detach"
	Node   string
	PVName string
}

// NewFakeVolumeManager returns a ready-to-use FakeVolumeManager.
func NewFakeVolumeManager() *FakeVolumeManager {
	return &FakeVolumeManager{
		attached:   make(map[string]string),
		DevicePath: "/dev/fake-disk",
		errors:     make(map[int]error),
	}
}

// InjectError causes the n-th call (0-indexed across all methods) to return err.
func (f *FakeVolumeManager) InjectError(n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errors[n] = err
}

func (f *FakeVolumeManager) AttachDisk(ctx context.Context, nodeName, pvName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	call := VolumeCall{Op: "attach", Node: nodeName, PVName: pvName}
	if err := f.nextError(); err != nil {
		f.Calls = append(f.Calls, call)
		return err
	}
	f.attached[pvName] = f.DevicePath
	f.Calls = append(f.Calls, call)
	return nil
}

func (f *FakeVolumeManager) WaitForDevice(ctx context.Context, pvName string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	call := VolumeCall{Op: "wait", PVName: pvName}
	if err := f.nextError(); err != nil {
		f.Calls = append(f.Calls, call)
		return "", err
	}
	dev, ok := f.attached[pvName]
	if !ok {
		f.Calls = append(f.Calls, call)
		return "", fmt.Errorf("fake: disk %q not attached", pvName)
	}
	f.Calls = append(f.Calls, call)
	return dev, nil
}

func (f *FakeVolumeManager) DetachDisk(ctx context.Context, nodeName, pvName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	call := VolumeCall{Op: "detach", Node: nodeName, PVName: pvName}
	if err := f.nextError(); err != nil {
		f.Calls = append(f.Calls, call)
		return err
	}
	delete(f.attached, pvName)
	f.Calls = append(f.Calls, call)
	return nil
}

func (f *FakeVolumeManager) nextError() error {
	idx := len(f.Calls)
	if err, ok := f.errors[idx]; ok {
		return err
	}
	return nil
}
