package volume

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// LocalPathManager implements Manager for KIND's local-path
// StorageClass. It manages PVC/PV lifecycle using the Kubernetes API.
//
// On a single-node KIND cluster, ReadOnlyMany access mode doesn't enable
// real multi-attach, but it validates the full finalization flow that
// production implementations (Hyperdisk ML) rely on.
type LocalPathManager struct {
	Client    kubernetes.Interface
	Namespace string
}

// pvcPollInterval is how often FinalizeBuild polls for PVC binding.
const pvcPollInterval = 500 * time.Millisecond

// pvcPollTimeout is the maximum time FinalizeBuild waits for PVC binding.
const pvcPollTimeout = 2 * time.Minute

// pvcDeleteTimeout is the maximum time FinalizeBuild waits for PVC deletion.
const pvcDeleteTimeout = 30 * time.Second

func (m *LocalPathManager) CreateBuildPVC(ctx context.Context, name, storageClass string, sizeGB int64) error {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: m.Namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &storageClass,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(fmt.Sprintf("%dGi", sizeGB)),
				},
			},
		},
	}

	_, err := m.Client.CoreV1().PersistentVolumeClaims(m.Namespace).Create(ctx, pvc, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (m *LocalPathManager) FinalizeBuild(ctx context.Context, pvcName string) (string, error) {
	// Wait for PVC to be bound.
	pvName, err := m.waitForBound(ctx, pvcName)
	if err != nil {
		return "", fmt.Errorf("wait for PVC %q bound: %w", pvcName, err)
	}

	// Patch PV reclaimPolicy to Retain so it survives PVC deletion.
	if err := m.setRetainPolicy(ctx, pvName); err != nil {
		return "", fmt.Errorf("set retain policy on PV %q: %w", pvName, err)
	}

	// Delete the PVC. The PV survives because reclaimPolicy is now Retain.
	if err := m.Client.CoreV1().PersistentVolumeClaims(m.Namespace).Delete(ctx, pvcName, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
		return "", fmt.Errorf("delete PVC %q: %w", pvcName, err)
	}

	// Wait for PVC to be fully deleted so the PV's claimRef becomes clearable.
	if err := m.waitForPVCDeleted(ctx, pvcName); err != nil {
		return "", fmt.Errorf("wait for PVC %q deletion: %w", pvcName, err)
	}

	// Clear claimRef and set ReadOnlyMany on the PV.
	if err := m.finalizePV(ctx, pvName); err != nil {
		return "", fmt.Errorf("finalize PV %q: %w", pvName, err)
	}

	return pvName, nil
}

func (m *LocalPathManager) DeletePV(ctx context.Context, pvName string) error {
	err := m.Client.CoreV1().PersistentVolumes().Delete(ctx, pvName, metav1.DeleteOptions{})
	if errors.IsNotFound(err) {
		return nil
	}
	return err
}

// waitForBound polls until the PVC is bound and returns the backing PV name.
func (m *LocalPathManager) waitForBound(ctx context.Context, pvcName string) (string, error) {
	deadline := time.After(pvcPollTimeout)
	ticker := time.NewTicker(pvcPollInterval)
	defer ticker.Stop()

	for {
		pvc, err := m.Client.CoreV1().PersistentVolumeClaims(m.Namespace).Get(ctx, pvcName, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		if pvc.Status.Phase == corev1.ClaimBound && pvc.Spec.VolumeName != "" {
			return pvc.Spec.VolumeName, nil
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline:
			return "", fmt.Errorf("timed out waiting for PVC %q to bind (phase: %s)", pvcName, pvc.Status.Phase)
		case <-ticker.C:
		}
	}
}

// setRetainPolicy patches the PV's reclaimPolicy to Retain.
func (m *LocalPathManager) setRetainPolicy(ctx context.Context, pvName string) error {
	pv, err := m.Client.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if pv.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimRetain {
		return nil
	}
	pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
	_, err = m.Client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
	return err
}

// waitForPVCDeleted polls until the PVC no longer exists.
func (m *LocalPathManager) waitForPVCDeleted(ctx context.Context, pvcName string) error {
	deadline := time.After(pvcDeleteTimeout)
	ticker := time.NewTicker(pvcPollInterval)
	defer ticker.Stop()

	for {
		_, err := m.Client.CoreV1().PersistentVolumeClaims(m.Namespace).Get(ctx, pvcName, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("timed out waiting for PVC %q to be deleted", pvcName)
		case <-ticker.C:
		}
	}
}

// finalizePV clears the claimRef and sets accessModes to ReadOnlyMany.
func (m *LocalPathManager) finalizePV(ctx context.Context, pvName string) error {
	pv, err := m.Client.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	pv.Spec.ClaimRef = nil
	pv.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany}
	_, err = m.Client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
	return err
}
