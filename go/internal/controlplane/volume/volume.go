package volume

import "context"

// Manager manages PVC and PV lifecycle for build output. Implementations
// handle the storage-class-specific details of creating build storage,
// finalizing it into a read-only PV, and cleaning up.
type Manager interface {
	// CreateBuildPVC creates a RWO PersistentVolumeClaim for a build to write
	// its output to. Idempotent: if the PVC already exists, returns nil.
	CreateBuildPVC(ctx context.Context, name, storageClass string, sizeGB int64) error

	// FinalizeBuild transitions a build PVC into a read-only PV suitable for
	// multi-attach by node-agents. It waits for the PVC to be bound, patches
	// the backing PV's reclaimPolicy to Retain, deletes the PVC, clears the
	// PV's claimRef, and sets accessModes to ReadOnlyMany. Returns the PV name.
	FinalizeBuild(ctx context.Context, pvcName string) (pvName string, err error)

	// DeletePV deletes a PersistentVolume. Idempotent: if the PV does not
	// exist, returns nil.
	DeletePV(ctx context.Context, pvName string) error
}
