//go:build !linux

package mount

import (
	"context"
	"errors"
)

// LinuxMounter is a no-op stub on non-Linux platforms so the package compiles
// on developer machines.  All methods return errors.
type LinuxMounter struct{}

func NewLinuxMounter() *LinuxMounter { return &LinuxMounter{} }

func (m *LinuxMounter) Mount(_ context.Context, device, target string) error {
	return errors.New("mount: only supported on Linux")
}

func (m *LinuxMounter) Unmount(_ context.Context, target string) error {
	return errors.New("mount: only supported on Linux")
}
