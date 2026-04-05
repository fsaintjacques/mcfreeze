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
// target is created with mode 0o755 if it does not already exist.
func (m *LinuxMounter) Mount(_ context.Context, device, target string) error {
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

// Unmount unmounts target and removes the mount-point directory.
func (m *LinuxMounter) Unmount(_ context.Context, target string) error {
	if err := syscall.Unmount(target, 0); err != nil {
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
