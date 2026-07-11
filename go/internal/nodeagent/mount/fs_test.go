// SPDX-License-Identifier: Apache-2.0

package mount_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/fsaintjacques/mcfreeze/go/internal/nodeagent/mount"
)

func newFS(t *testing.T) (*mount.FSMounter, string) {
	t.Helper()
	return mount.NewFSMounter(), t.TempDir()
}

func TestFSMounter_MountCreatesSymlink(t *testing.T) {
	m, base := newFS(t)
	device := filepath.Join(base, "device")
	target := filepath.Join(base, "mnt", "dataset", "v1")

	if err := os.MkdirAll(device, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := m.Mount(context.Background(), device, target); err != nil {
		t.Fatal(err)
	}

	got, err := os.Readlink(target)
	if err != nil {
		t.Fatalf("expected symlink at %s: %v", target, err)
	}
	if got != device {
		t.Fatalf("symlink points to %q, want %q", got, device)
	}
}

func TestFSMounter_MountCreatesParentDirs(t *testing.T) {
	m, base := newFS(t)
	target := filepath.Join(base, "a", "b", "c", "target")

	if err := m.Mount(context.Background(), "/dev/fake", target); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(target); err != nil {
		t.Fatalf("expected target %s to exist: %v", target, err)
	}
}

func TestFSMounter_MountIdempotent(t *testing.T) {
	m, base := newFS(t)
	target := filepath.Join(base, "mnt", "v1")

	for range 3 {
		if err := m.Mount(context.Background(), "/dev/fake", target); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFSMounter_UnmountRemovesSymlink(t *testing.T) {
	m, base := newFS(t)
	target := filepath.Join(base, "mnt", "v1")

	if err := m.Mount(context.Background(), "/dev/fake", target); err != nil {
		t.Fatal(err)
	}
	if err := m.Unmount(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(target); !os.IsNotExist(err) {
		t.Fatalf("expected symlink %s to be gone after unmount", target)
	}
}

func TestFSMounter_UnmountIdempotent(t *testing.T) {
	m, base := newFS(t)
	target := filepath.Join(base, "mnt", "v1")

	if err := m.Mount(context.Background(), "/dev/fake", target); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := m.Unmount(context.Background(), target); err != nil {
			t.Fatal(err)
		}
	}
}
