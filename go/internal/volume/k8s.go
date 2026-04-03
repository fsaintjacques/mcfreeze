package volume

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ComputeDiskManager implements VolumeManager by calling the GCP Compute Engine
// API directly.  It requires a Kubernetes client to resolve pvName →
// spec.csi.volumeHandle (the full disk resource URL) before issuing Compute
// Engine calls.
//
// TODO: add the following dependencies:
//   - k8s.io/client-go for PV lookup
//   - cloud.google.com/go/compute/apiv1 for Compute Engine calls
type ComputeDiskManager struct {
	// Project is the GCP project that owns the node instance.
	Project string
	// Zone is the Compute Engine zone of this node.
	Zone string
	// DevicePollInterval controls how often WaitForDevice polls for the block
	// device to appear under /dev/disk/by-id/.
	DevicePollInterval time.Duration
	// DevicePollTimeout is the maximum time WaitForDevice will wait.
	DevicePollTimeout time.Duration
}

// NewComputeDiskManager creates a ComputeDiskManager with sensible defaults.
func NewComputeDiskManager(project, zone string) *ComputeDiskManager {
	return &ComputeDiskManager{
		Project:            project,
		Zone:               zone,
		DevicePollInterval: 2 * time.Second,
		DevicePollTimeout:  2 * time.Minute,
	}
}

// AttachDisk resolves pvName to a disk URL via the Kubernetes PV object, then
// calls Compute Engine instances.attachDisk in READ_ONLY mode.
func (m *ComputeDiskManager) AttachDisk(ctx context.Context, nodeName, pvName string) error {
	diskURL, err := m.diskURLFromPV(ctx, pvName)
	if err != nil {
		return err
	}
	// TODO: call Compute Engine instances.attachDisk(nodeName, diskURL, READ_ONLY).
	_ = diskURL
	return errors.New("ComputeDiskManager.AttachDisk: not implemented")
}

// WaitForDevice resolves pvName to a disk name, then polls
// /dev/disk/by-id/google-<disk-name> until it appears.
func (m *ComputeDiskManager) WaitForDevice(ctx context.Context, pvName string) (string, error) {
	diskURL, err := m.diskURLFromPV(ctx, pvName)
	if err != nil {
		return "", err
	}
	diskName := diskNameFromURL(diskURL)
	deviceLink := fmt.Sprintf("/dev/disk/by-id/google-%s", diskName)

	deadline := time.Now().Add(m.DevicePollTimeout)
	// TODO: implement device polling loop once wired up.
	_ = deviceLink
	_ = deadline
	return "", errors.New("ComputeDiskManager.WaitForDevice: not implemented")
}

// DetachDisk resolves pvName to a disk URL, then calls Compute Engine
// instances.detachDisk.
func (m *ComputeDiskManager) DetachDisk(ctx context.Context, nodeName, pvName string) error {
	diskURL, err := m.diskURLFromPV(ctx, pvName)
	if err != nil {
		return err
	}
	// TODO: call Compute Engine instances.detachDisk(nodeName, diskURL).
	_ = diskURL
	return errors.New("ComputeDiskManager.DetachDisk: not implemented")
}

// diskURLFromPV fetches the PersistentVolume by name and extracts
// spec.csi.volumeHandle, which for the GCP Compute Engine CSI driver contains
// the full disk resource URL.
//
// TODO: implement using a k8s.io/client-go corev1 client.
func (m *ComputeDiskManager) diskURLFromPV(ctx context.Context, pvName string) (string, error) {
	_ = ctx
	return "", fmt.Errorf("diskURLFromPV(%q): not implemented", pvName)
}

// diskNameFromURL extracts the disk name from a Compute Engine resource URL,
// e.g. "projects/p/zones/z/disks/my-disk" → "my-disk".
func diskNameFromURL(diskURL string) string {
	for i := len(diskURL) - 1; i >= 0; i-- {
		if diskURL[i] == '/' {
			return diskURL[i+1:]
		}
	}
	return diskURL
}
