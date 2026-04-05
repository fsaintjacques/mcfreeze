package volume

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// FSManager implements Manager using the local filesystem.
// Each volume is represented as a directory under BaseDir.  This is useful for
// local development and integration tests that cannot call the GCP Compute
// Engine API or require real block devices.
//
//   - AttachDisk  → mkdir <BaseDir>/<pvName>
//   - WaitForDevice → poll until the directory exists, return its path
//   - DetachDisk  → rmdir <BaseDir>/<pvName>
type FSManager struct {
	// BaseDir is the root under which volume directories are created.
	BaseDir string
	// PollInterval controls how often WaitForDevice checks for the directory.
	PollInterval time.Duration
	// PollTimeout is the maximum time WaitForDevice will wait.
	PollTimeout time.Duration
}

// NewFSManager returns an FSManager with sensible defaults.
func NewFSManager(baseDir string) *FSManager {
	return &FSManager{
		BaseDir:      baseDir,
		PollInterval: 100 * time.Millisecond,
		PollTimeout:  10 * time.Second,
	}
}

// AttachDisk creates the volume directory, simulating a disk attachment.
func (m *FSManager) AttachDisk(_ context.Context, _, pvName string) error {
	dir := m.volumeDir(pvName)
	if err := os.Mkdir(dir, 0o755); err != nil && !os.IsExist(err) {
		return fmt.Errorf("fs attach %s: %w", pvName, err)
	}
	return nil
}

// WaitForDevice polls until the volume directory exists and returns its path.
func (m *FSManager) WaitForDevice(ctx context.Context, pvName string) (string, error) {
	dir := m.volumeDir(pvName)
	deadline := time.Now().Add(m.PollTimeout)
	for {
		if _, err := os.Stat(dir); err == nil {
			return dir, nil
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("fs wait-for-device %s: timeout after %s", pvName, m.PollTimeout)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(m.PollInterval):
		}
	}
}

// DetachDisk removes the volume directory, simulating a disk detachment.
func (m *FSManager) DetachDisk(_ context.Context, _, pvName string) error {
	dir := m.volumeDir(pvName)
	if err := os.Remove(dir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("fs detach %s: %w", pvName, err)
	}
	return nil
}

func (m *FSManager) volumeDir(pvName string) string {
	return filepath.Join(m.BaseDir, pvName)
}
