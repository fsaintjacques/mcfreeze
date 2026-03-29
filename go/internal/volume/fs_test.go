package volume_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"frostmap.io/fmtctl/internal/volume"
)

func newFS(t *testing.T) *volume.FSVolumeManager {
	t.Helper()
	m := volume.NewFSVolumeManager(t.TempDir())
	m.PollInterval = 5 * time.Millisecond
	m.PollTimeout = 2 * time.Second
	return m
}

func TestFSVolumeManager_AttachCreatesDir(t *testing.T) {
	m := newFS(t)
	if err := m.AttachDisk(context.Background(), "node-1", "pv-foo"); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(m.BaseDir, "pv-foo")
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("expected volume dir %s to exist after attach: %v", dir, err)
	}
}

func TestFSVolumeManager_AttachIdempotent(t *testing.T) {
	m := newFS(t)
	for range 3 {
		if err := m.AttachDisk(context.Background(), "node-1", "pv-foo"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFSVolumeManager_WaitForDevice_AlreadyAttached(t *testing.T) {
	m := newFS(t)
	if err := m.AttachDisk(context.Background(), "node-1", "pv-foo"); err != nil {
		t.Fatal(err)
	}
	dev, err := m.WaitForDevice(context.Background(), "pv-foo")
	if err != nil {
		t.Fatal(err)
	}
	if dev == "" {
		t.Fatal("expected non-empty device path")
	}
}

func TestFSVolumeManager_WaitForDevice_AppearsLate(t *testing.T) {
	m := newFS(t)
	ctx := context.Background()

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = m.AttachDisk(ctx, "node-1", "pv-late")
	}()

	dev, err := m.WaitForDevice(ctx, "pv-late")
	if err != nil {
		t.Fatal(err)
	}
	if dev == "" {
		t.Fatal("expected non-empty device path")
	}
}

func TestFSVolumeManager_WaitForDevice_Timeout(t *testing.T) {
	m := newFS(t)
	m.PollTimeout = 50 * time.Millisecond

	_, err := m.WaitForDevice(context.Background(), "pv-never")
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestFSVolumeManager_WaitForDevice_ContextCancelled(t *testing.T) {
	m := newFS(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := m.WaitForDevice(ctx, "pv-never")
	if err == nil {
		t.Fatal("expected context error, got nil")
	}
}

func TestFSVolumeManager_DetachRemovesDir(t *testing.T) {
	m := newFS(t)
	ctx := context.Background()

	if err := m.AttachDisk(ctx, "node-1", "pv-foo"); err != nil {
		t.Fatal(err)
	}
	if err := m.DetachDisk(ctx, "node-1", "pv-foo"); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(m.BaseDir, "pv-foo")
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("expected volume dir %s to be gone after detach", dir)
	}
}

func TestFSVolumeManager_DetachIdempotent(t *testing.T) {
	m := newFS(t)
	ctx := context.Background()

	if err := m.AttachDisk(ctx, "node-1", "pv-foo"); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := m.DetachDisk(ctx, "node-1", "pv-foo"); err != nil {
			t.Fatal(err)
		}
	}
}
