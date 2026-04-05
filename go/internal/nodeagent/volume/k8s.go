package volume

import (
	"context"
	"fmt"
	"strings"
	"time"

	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// K8sManager implements Manager using the Kubernetes VolumeAttachment API.
// It works with any CSI driver (csi-driver-host-path in KIND,
// pd.csi.storage.gke.io on GKE).
type K8sManager struct {
	Client    kubernetes.Interface
	CSIDriver string // e.g. "hostpath.csi.k8s.io" or "pd.csi.storage.gke.io"

	// PollInterval controls how often WaitForDevice polls VolumeAttachment
	// status.  Defaults to 2s.
	PollInterval time.Duration
	// PollTimeout is the maximum time WaitForDevice waits for the attachment
	// to become ready.  Defaults to 2m.
	PollTimeout time.Duration
}

func (m *K8sManager) pollInterval() time.Duration {
	if m.PollInterval > 0 {
		return m.PollInterval
	}
	return 2 * time.Second
}

func (m *K8sManager) pollTimeout() time.Duration {
	if m.PollTimeout > 0 {
		return m.PollTimeout
	}
	return 2 * time.Minute
}

// vaName returns a deterministic, DNS-safe VolumeAttachment name for a PV.
func vaName(pvName string) string {
	name := "fm-va-" + strings.ToLower(pvName)

	var b strings.Builder
	prevHyphen := false
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevHyphen = false
		} else {
			if !prevHyphen {
				b.WriteByte('-')
			}
			prevHyphen = true
		}
	}
	name = b.String()

	if len(name) > 253 {
		name = name[:253]
	}
	return strings.TrimRight(name, "-")
}

// AttachDisk creates a VolumeAttachment for the given PV on the given node.
// Idempotent: returns nil if the VolumeAttachment already exists.
func (m *K8sManager) AttachDisk(ctx context.Context, nodeName, pvName string) error {
	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: vaName(pvName),
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: m.CSIDriver,
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvName,
			},
			NodeName: nodeName,
		},
	}

	_, err := m.Client.StorageV1().VolumeAttachments().Create(ctx, va, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("k8s attach %s on %s: %w", pvName, nodeName, err)
	}
	return nil
}

// WaitForDevice polls the VolumeAttachment until .status.attached is true and
// returns the device path from .status.attachmentMetadata["devicePath"].
// If the CSI driver does not populate devicePath, the PV name is returned as
// a fallback identifier.
func (m *K8sManager) WaitForDevice(ctx context.Context, pvName string) (string, error) {
	name := vaName(pvName)
	deadline := time.After(m.pollTimeout())
	ticker := time.NewTicker(m.pollInterval())
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}

		va, err := m.Client.StorageV1().VolumeAttachments().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", fmt.Errorf("k8s wait-for-device %s: %w", pvName, err)
		}

		if va.Status.Attached {
			if dp, ok := va.Status.AttachmentMetadata["devicePath"]; ok && dp != "" {
				return dp, nil
			}
			return pvName, nil
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline:
			return "", fmt.Errorf("k8s wait-for-device %s: timed out after %s", pvName, m.pollTimeout())
		case <-ticker.C:
		}
	}
}

// DetachDisk deletes the VolumeAttachment for the given PV.
// Idempotent: returns nil if the VolumeAttachment does not exist.
func (m *K8sManager) DetachDisk(ctx context.Context, nodeName, pvName string) error {
	err := m.Client.StorageV1().VolumeAttachments().Delete(ctx, vaName(pvName), metav1.DeleteOptions{})
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("k8s detach %s from %s: %w", pvName, nodeName, err)
	}
	return nil
}
