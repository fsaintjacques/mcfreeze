package volume

import (
	"context"
	"fmt"
	"log/slog"
	"os"
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
// If the CSI driver does not populate devicePath, the PV's CSI volumeHandle
// is resolved to a host path (e.g. /var/lib/csi-hostpath-data/<handle> for
// csi-driver-host-path). If the PV has no CSI spec, the PV name is returned
// as a fallback identifier.
func (m *K8sManager) WaitForDevice(ctx context.Context, pvName string) (string, error) {
	name := vaName(pvName)
	deadline := time.After(m.pollTimeout())
	ticker := time.NewTicker(m.pollInterval())
	defer ticker.Stop()

	seen := false
	for {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}

		va, err := m.Client.StorageV1().VolumeAttachments().Get(ctx, name, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			if seen {
				// VA existed before but was deleted externally.
				return "", fmt.Errorf("k8s wait-for-device %s: %w", pvName, err)
			}
			// VA may not have propagated yet; keep polling.
		} else if err != nil {
			return "", fmt.Errorf("k8s wait-for-device %s: %w", pvName, err)
		} else {
			seen = true
			if va.Status.Attached {
				if dp, ok := va.Status.AttachmentMetadata["devicePath"]; ok && dp != "" {
					return dp, nil
				}
				return m.resolveDevicePath(ctx, pvName)
			}
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

// resolveDevicePath looks up the PV to derive a usable device path when the
// VolumeAttachment metadata does not include one.
//
// It first scans /dev/disk/by-id/ for a symlink containing the PV name (works
// for any block-device CSI driver: GCE PD, EBS, etc.). If no match is found,
// it falls back to CSI hostpath conventions (KIND) or hostPath PVs.
func (m *K8sManager) resolveDevicePath(ctx context.Context, pvName string) (string, error) {
	// Try /dev/disk/by-id/ first — works for any real block-device CSI driver.
	if path, err := findDiskByID(pvName); err == nil {
		slog.Info("resolveDevicePath: found block device", "pv", pvName, "path", path)
		return path, nil
	}

	// Fallback: look up PV spec for hostpath-style CSI drivers (KIND).
	pv, err := m.Client.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		slog.Warn("resolveDevicePath: PV lookup failed, using PV name as fallback", "pv", pvName, "err", err)
		return pvName, nil
	}
	if pv.Spec.CSI != nil && pv.Spec.CSI.VolumeHandle != "" {
		path := "/var/lib/csi-hostpath-data/" + pv.Spec.CSI.VolumeHandle
		slog.Info("resolveDevicePath: resolved CSI hostpath volume", "pv", pvName, "path", path)
		return path, nil
	}
	if pv.Spec.HostPath != nil {
		return pv.Spec.HostPath.Path, nil
	}
	slog.Warn("resolveDevicePath: no CSI or hostPath spec, using PV name as fallback", "pv", pvName)
	return pvName, nil
}

// findDiskByID scans /dev/disk/by-id/ for a symlink whose name ends with the
// PV name (e.g. "google-pvc-abc123" for PV "pvc-abc123").
func findDiskByID(pvName string) (string, error) {
	entries, err := os.ReadDir("/dev/disk/by-id")
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), pvName) {
			return "/dev/disk/by-id/" + e.Name(), nil
		}
	}
	return "", fmt.Errorf("no /dev/disk/by-id entry for %s", pvName)
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
