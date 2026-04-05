//go:build kind

package volume

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// kindClientset creates a Kubernetes clientset from the default kubeconfig.
func kindClientset(t *testing.T) kubernetes.Interface {
	t.Helper()

	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		home, _ := os.UserHomeDir()
		kubeconfig = home + "/.kube/config"
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatalf("build kubeconfig: %v", err)
	}

	cs, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("create clientset: %v", err)
	}
	return cs
}

// kindNodeName returns the name of a schedulable node in the cluster.
func kindNodeName(t *testing.T, cs kubernetes.Interface) string {
	t.Helper()
	ctx := context.Background()

	nodes, err := cs.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	if len(nodes.Items) == 0 {
		t.Fatal("no nodes in cluster")
	}
	return nodes.Items[0].Name
}

// createCSIHostPathPV creates a PersistentVolume backed by the
// csi-driver-host-path CSI driver and registers cleanup.
func createCSIHostPathPV(t *testing.T, cs kubernetes.Interface, name string) {
	t.Helper()
	ctx := context.Background()

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany},
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       "hostpath.csi.k8s.io",
					VolumeHandle: fmt.Sprintf("test-vol-%s-%d", name, time.Now().UnixNano()%100000),
					VolumeAttributes: map[string]string{
						"storage.kubernetes.io/csiProvisionerIdentity": "test",
					},
				},
			},
		},
	}

	if _, err := cs.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create PV: %v", err)
	}
	t.Cleanup(func() {
		cs.CoreV1().PersistentVolumes().Delete(ctx, name, metav1.DeleteOptions{})
	})
}

// TestKindK8sManager_FullLifecycle tests attach → wait → detach against a real
// KIND cluster with csi-driver-host-path.
//
// Prerequisites:
//   - KIND cluster running (make kind-up)
//   - csi-driver-host-path deployed (Phase 0)
func TestKindK8sManager_FullLifecycle(t *testing.T) {
	cs := kindClientset(t)
	nodeName := kindNodeName(t, cs)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pvName := fmt.Sprintf("test-va-pv-%d", time.Now().UnixNano()%100000)
	createCSIHostPathPV(t, cs, pvName)

	m := &K8sManager{
		Client:       cs,
		CSIDriver:    "hostpath.csi.k8s.io",
		PollInterval: 1 * time.Second,
		PollTimeout:  2 * time.Minute,
	}

	// Attach.
	t.Log("attaching disk")
	if err := m.AttachDisk(ctx, nodeName, pvName); err != nil {
		t.Fatalf("AttachDisk: %v", err)
	}

	// Verify VolumeAttachment exists.
	va, err := cs.StorageV1().VolumeAttachments().Get(ctx, vaName(pvName), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get VolumeAttachment: %v", err)
	}
	if va.Spec.Attacher != "hostpath.csi.k8s.io" {
		t.Errorf("attacher = %q, want %q", va.Spec.Attacher, "hostpath.csi.k8s.io")
	}
	if va.Spec.NodeName != nodeName {
		t.Errorf("nodeName = %q, want %q", va.Spec.NodeName, nodeName)
	}

	// Wait for device.
	t.Log("waiting for device")
	device, err := m.WaitForDevice(ctx, pvName)
	if err != nil {
		t.Fatalf("WaitForDevice: %v", err)
	}
	t.Logf("device path: %s", device)

	// Detach.
	t.Log("detaching disk")
	if err := m.DetachDisk(ctx, nodeName, pvName); err != nil {
		t.Fatalf("DetachDisk: %v", err)
	}

	// Verify VolumeAttachment deleted.
	_, err = cs.StorageV1().VolumeAttachments().Get(ctx, vaName(pvName), metav1.GetOptions{})
	if err == nil {
		t.Error("VolumeAttachment should be deleted after DetachDisk")
	}

	t.Log("full lifecycle completed")
}

// TestKindK8sManager_Idempotent verifies double-attach and double-detach
// are safe in a real cluster.
func TestKindK8sManager_Idempotent(t *testing.T) {
	cs := kindClientset(t)
	nodeName := kindNodeName(t, cs)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pvName := fmt.Sprintf("test-va-idem-%d", time.Now().UnixNano()%100000)
	createCSIHostPathPV(t, cs, pvName)

	m := &K8sManager{
		Client:       cs,
		CSIDriver:    "hostpath.csi.k8s.io",
		PollInterval: 1 * time.Second,
		PollTimeout:  2 * time.Minute,
	}

	// Double attach.
	if err := m.AttachDisk(ctx, nodeName, pvName); err != nil {
		t.Fatalf("first AttachDisk: %v", err)
	}
	if err := m.AttachDisk(ctx, nodeName, pvName); err != nil {
		t.Fatalf("second AttachDisk: %v", err)
	}

	// Clean up.
	if err := m.DetachDisk(ctx, nodeName, pvName); err != nil {
		t.Fatalf("first DetachDisk: %v", err)
	}

	// Double detach.
	if err := m.DetachDisk(ctx, nodeName, pvName); err != nil {
		t.Fatalf("second DetachDisk: %v", err)
	}

	t.Log("idempotency verified")
}
