package volume

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	testCSIDriver = "hostpath.csi.k8s.io"
	testNode      = "kind-control-plane"
	testPV        = "pv-users-v1"
)

func newTestK8sManager() (*K8sManager, *fake.Clientset) {
	cs := fake.NewSimpleClientset()
	return &K8sManager{
		Client:       cs,
		CSIDriver:    testCSIDriver,
		PollInterval: 10 * time.Millisecond,
		PollTimeout:  500 * time.Millisecond,
	}, cs
}

func TestK8sManager_AttachDisk(t *testing.T) {
	m, cs := newTestK8sManager()
	ctx := context.Background()

	if err := m.AttachDisk(ctx, testNode, testPV); err != nil {
		t.Fatalf("AttachDisk: %v", err)
	}

	va, err := cs.StorageV1().VolumeAttachments().Get(ctx, vaName(testPV), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get VolumeAttachment: %v", err)
	}

	if va.Spec.Attacher != testCSIDriver {
		t.Errorf("attacher = %q, want %q", va.Spec.Attacher, testCSIDriver)
	}
	if got := va.Spec.Source.PersistentVolumeName; got == nil || *got != testPV {
		t.Errorf("source PV = %v, want %q", got, testPV)
	}
	if va.Spec.NodeName != testNode {
		t.Errorf("nodeName = %q, want %q", va.Spec.NodeName, testNode)
	}
}

func TestK8sManager_AttachDisk_Idempotent(t *testing.T) {
	m, _ := newTestK8sManager()
	ctx := context.Background()

	if err := m.AttachDisk(ctx, testNode, testPV); err != nil {
		t.Fatalf("first AttachDisk: %v", err)
	}
	if err := m.AttachDisk(ctx, testNode, testPV); err != nil {
		t.Fatalf("second AttachDisk: %v", err)
	}
}

func TestK8sManager_WaitForDevice(t *testing.T) {
	m, cs := newTestK8sManager()
	ctx := context.Background()

	if err := m.AttachDisk(ctx, testNode, testPV); err != nil {
		t.Fatalf("AttachDisk: %v", err)
	}

	// Simulate the CSI driver marking the attachment as ready.
	va, _ := cs.StorageV1().VolumeAttachments().Get(ctx, vaName(testPV), metav1.GetOptions{})
	va.Status.Attached = true
	va.Status.AttachmentMetadata = map[string]string{"devicePath": "/dev/sdb"}
	cs.StorageV1().VolumeAttachments().Update(ctx, va, metav1.UpdateOptions{})

	device, err := m.WaitForDevice(ctx, testPV)
	if err != nil {
		t.Fatalf("WaitForDevice: %v", err)
	}
	if device != "/dev/sdb" {
		t.Errorf("device = %q, want %q", device, "/dev/sdb")
	}
}

func TestK8sManager_WaitForDevice_FallbackToPVName(t *testing.T) {
	m, cs := newTestK8sManager()
	ctx := context.Background()

	if err := m.AttachDisk(ctx, testNode, testPV); err != nil {
		t.Fatalf("AttachDisk: %v", err)
	}

	// Simulate attachment without devicePath metadata.
	va, _ := cs.StorageV1().VolumeAttachments().Get(ctx, vaName(testPV), metav1.GetOptions{})
	va.Status.Attached = true
	cs.StorageV1().VolumeAttachments().Update(ctx, va, metav1.UpdateOptions{})

	device, err := m.WaitForDevice(ctx, testPV)
	if err != nil {
		t.Fatalf("WaitForDevice: %v", err)
	}
	if device != testPV {
		t.Errorf("device = %q, want fallback %q", device, testPV)
	}
}

func TestK8sManager_WaitForDevice_Timeout(t *testing.T) {
	m, _ := newTestK8sManager()
	m.PollTimeout = 50 * time.Millisecond
	ctx := context.Background()

	if err := m.AttachDisk(ctx, testNode, testPV); err != nil {
		t.Fatalf("AttachDisk: %v", err)
	}

	// Never mark as attached — should timeout.
	_, err := m.WaitForDevice(ctx, testPV)
	if err == nil {
		t.Fatal("WaitForDevice should have timed out")
	}
}

func TestK8sManager_WaitForDevice_TransientNotFound(t *testing.T) {
	m, cs := newTestK8sManager()
	ctx := context.Background()

	// Start WaitForDevice before the VA exists (simulates propagation delay).
	// Create the VA after a short delay in a goroutine.
	go func() {
		time.Sleep(30 * time.Millisecond)
		m.AttachDisk(ctx, testNode, testPV)
		va, _ := cs.StorageV1().VolumeAttachments().Get(ctx, vaName(testPV), metav1.GetOptions{})
		va.Status.Attached = true
		va.Status.AttachmentMetadata = map[string]string{"devicePath": "/dev/sdc"}
		cs.StorageV1().VolumeAttachments().Update(ctx, va, metav1.UpdateOptions{})
	}()

	device, err := m.WaitForDevice(ctx, testPV)
	if err != nil {
		t.Fatalf("WaitForDevice: %v", err)
	}
	if device != "/dev/sdc" {
		t.Errorf("device = %q, want %q", device, "/dev/sdc")
	}
}

func TestK8sManager_WaitForDevice_PersistentNotFound(t *testing.T) {
	m, _ := newTestK8sManager()
	m.PollTimeout = 50 * time.Millisecond

	// Never create the VA — should timeout (not error immediately).
	_, err := m.WaitForDevice(context.Background(), "nonexistent-pv")
	if err == nil {
		t.Fatal("WaitForDevice should have timed out")
	}
}

func TestK8sManager_WaitForDevice_DeletedAfterSeen(t *testing.T) {
	m, cs := newTestK8sManager()
	ctx := context.Background()

	if err := m.AttachDisk(ctx, testNode, testPV); err != nil {
		t.Fatalf("AttachDisk: %v", err)
	}

	// Delete the VA after a short delay (simulates external deletion).
	go func() {
		time.Sleep(30 * time.Millisecond)
		cs.StorageV1().VolumeAttachments().Delete(ctx, vaName(testPV), metav1.DeleteOptions{})
	}()

	// VA exists but is not attached; then gets deleted — should fail immediately.
	_, err := m.WaitForDevice(ctx, testPV)
	if err == nil {
		t.Fatal("WaitForDevice should fail when VA is deleted after being seen")
	}
}

func TestK8sManager_DetachDisk(t *testing.T) {
	m, cs := newTestK8sManager()
	ctx := context.Background()

	if err := m.AttachDisk(ctx, testNode, testPV); err != nil {
		t.Fatalf("AttachDisk: %v", err)
	}

	if err := m.DetachDisk(ctx, testNode, testPV); err != nil {
		t.Fatalf("DetachDisk: %v", err)
	}

	_, err := cs.StorageV1().VolumeAttachments().Get(ctx, vaName(testPV), metav1.GetOptions{})
	if err == nil {
		t.Error("VolumeAttachment should be deleted after DetachDisk")
	}
}

func TestK8sManager_DetachDisk_Idempotent(t *testing.T) {
	m, _ := newTestK8sManager()
	ctx := context.Background()

	// Detach without prior attach — should not error.
	if err := m.DetachDisk(ctx, testNode, testPV); err != nil {
		t.Fatalf("DetachDisk on nonexistent: %v", err)
	}
}

func TestK8sManager_WaitForDevice_ContextCancelled(t *testing.T) {
	m, _ := newTestK8sManager()
	m.PollTimeout = 5 * time.Second

	if err := m.AttachDisk(context.Background(), testNode, testPV); err != nil {
		t.Fatalf("AttachDisk: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := m.WaitForDevice(ctx, testPV)
	if err == nil {
		t.Fatal("WaitForDevice should fail on cancelled context")
	}
}

func TestVAName(t *testing.T) {
	if got := vaName("pv-users-v1"); got != "fm-va-pv-users-v1" {
		t.Errorf("vaName = %q, want %q", got, "fm-va-pv-users-v1")
	}
}

// Verify K8sManager satisfies the Manager interface at compile time.
var _ Manager = (*K8sManager)(nil)
