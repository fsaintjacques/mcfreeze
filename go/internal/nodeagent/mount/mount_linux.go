//go:build linux

package mount

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
)

// LinuxMounter implements Mounter using Linux syscall.Mount.
type LinuxMounter struct{}

// NewLinuxMounter returns a LinuxMounter.
func NewLinuxMounter() *LinuxMounter { return &LinuxMounter{} }

// Mount mounts device at target read-only (MS_RDONLY | MS_NODEV | MS_NOSUID).
// Idempotent: any stale mount at target is unmounted first, and the target
// directory is created if it does not exist.
func (m *LinuxMounter) Mount(_ context.Context, device, target string) error {
	// Clean up any stale mount at target (e.g. from a previous pod run).
	// EINVAL means not mounted — that's the common case on a fresh start.
	if err := syscall.Unmount(target, syscall.MNT_DETACH); err != nil && !errors.Is(err, syscall.EINVAL) {
		// ENOENT is fine — target directory doesn't exist yet.
		if !errors.Is(err, syscall.ENOENT) {
			return fmt.Errorf("mount: pre-unmount %s: %w", target, err)
		}
	}

	if err := os.MkdirAll(target, 0o755); err != nil {
		return fmt.Errorf("mount: create target %s: %w", target, err)
	}
	flags := uintptr(syscall.MS_RDONLY | syscall.MS_NODEV | syscall.MS_NOSUID)
	if err := syscall.Mount(device, target, "ext4", flags, ""); err != nil {
		// Try xfs as a fallback — Hyperdisk ML volumes may be formatted either way.
		if err2 := syscall.Mount(device, target, "xfs", flags, ""); err2 != nil {
			return fmt.Errorf("mount: %s → %s: ext4: %w; xfs: %v", device, target, err, err2)
		}
	}
	return nil
}

// Unmount lazily unmounts target and removes the mount-point directory.
// MNT_DETACH detaches the mount from the namespace immediately but keeps the
// filesystem alive until all references (open fds, mmaps) are dropped. This
// avoids invalidating mmap handles held by the KV server during hot-swap.
func (m *LinuxMounter) Unmount(_ context.Context, target string) error {
	if err := syscall.Unmount(target, syscall.MNT_DETACH); err != nil {
		// EINVAL means not mounted; treat as idempotent.
		if !errors.Is(err, syscall.EINVAL) {
			return fmt.Errorf("unmount %s: %w", target, err)
		}
	}
	if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("unmount: remove %s: %w", target, err)
	}
	return nil
}
