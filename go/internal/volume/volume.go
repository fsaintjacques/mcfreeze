// Package volume abstracts disk attachment operations needed by node-agent.
// All implementations use the Kubernetes PersistentVolume name as their
// primary identifier; the underlying cloud operation is an implementation
// detail.
package volume

import "context"

// VolumeManager handles disk attachment lifecycle on behalf of node-agent.
// In both implementations pvName is the Kubernetes PersistentVolume name
// created by the control-plane when the version transitions to ready.
type VolumeManager interface {
	// AttachDisk attaches the disk backing pvName to nodeName in read-only
	// mode.  The call is idempotent — attaching an already-attached disk is
	// not an error.
	AttachDisk(ctx context.Context, nodeName, pvName string) error

	// WaitForDevice blocks until the block device for pvName appears on the
	// local node and returns its path (e.g. /dev/disk/by-id/google-foo).
	// The disk must already be attached before calling this.
	WaitForDevice(ctx context.Context, pvName string) (string, error)

	// DetachDisk detaches the disk backing pvName from nodeName.  The call is
	// idempotent — detaching an already-detached disk is not an error.
	DetachDisk(ctx context.Context, nodeName, pvName string) error
}
