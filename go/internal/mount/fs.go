package mount

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// FSMounter implements Mounter using symlinks.  This is useful for local
// development and integration tests that cannot perform real mount syscalls
// (which require root privileges).
//
//   - Mount   → mkdir -p <filepath.Dir(target)> && ln -s <device> <target>
//   - Unmount → rm <target>
type FSMounter struct{}

// NewFSMounter returns an FSMounter.
func NewFSMounter() *FSMounter { return &FSMounter{} }

// Mount creates a symlink at target pointing to device.
func (m *FSMounter) Mount(_ context.Context, device, target string) error {
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return fmt.Errorf("fs mount: create parent %s: %w", filepath.Dir(target), err)
	}
	if err := os.Symlink(device, target); err != nil {
		if os.IsExist(err) {
			return nil // idempotent
		}
		return fmt.Errorf("fs mount: symlink %s → %s: %w", target, device, err)
	}
	return nil
}

// Unmount removes the symlink at target.
func (m *FSMounter) Unmount(_ context.Context, target string) error {
	if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("fs unmount: remove %s: %w", target, err)
	}
	return nil
}
