package volume

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

const testNamespace = "default"

func newTestManager() (*LocalPathManager, *fake.Clientset) {
	cs := fake.NewSimpleClientset()
	return &LocalPathManager{Client: cs, Namespace: testNamespace}, cs
}

func TestCreateBuildPVC(t *testing.T) {
	dm, cs := newTestManager()
	ctx := context.Background()

	if err := dm.CreateBuildPVC(ctx, "test-pvc", "local-path", 10); err != nil {
		t.Fatalf("CreateBuildPVC: %v", err)
	}

	pvc, err := cs.CoreV1().PersistentVolumeClaims(testNamespace).Get(ctx, "test-pvc", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get PVC: %v", err)
	}

	if pvc.Spec.AccessModes[0] != corev1.ReadWriteOnce {
		t.Errorf("access mode = %v, want ReadWriteOnce", pvc.Spec.AccessModes[0])
	}
	if *pvc.Spec.StorageClassName != "local-path" {
		t.Errorf("storageClass = %q, want %q", *pvc.Spec.StorageClassName, "local-path")
	}
	want := resource.MustParse("10Gi")
	got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if got.Cmp(want) != 0 {
		t.Errorf("storage = %v, want %v", got.String(), want.String())
	}
}

func TestCreateBuildPVC_Idempotent(t *testing.T) {
	dm, _ := newTestManager()
	ctx := context.Background()

	if err := dm.CreateBuildPVC(ctx, "test-pvc", "local-path", 10); err != nil {
		t.Fatalf("first CreateBuildPVC: %v", err)
	}
	if err := dm.CreateBuildPVC(ctx, "test-pvc", "local-path", 10); err != nil {
		t.Fatalf("second CreateBuildPVC should be idempotent: %v", err)
	}
}

func TestDeletePV(t *testing.T) {
	dm, cs := newTestManager()
	ctx := context.Background()

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pv"},
	}
	if _, err := cs.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create PV: %v", err)
	}

	if err := dm.DeletePV(ctx, "test-pv"); err != nil {
		t.Fatalf("DeletePV: %v", err)
	}

	// Verify deleted.
	_, err := cs.CoreV1().PersistentVolumes().Get(ctx, "test-pv", metav1.GetOptions{})
	if err == nil {
		t.Fatal("PV should be deleted")
	}
}

func TestDeletePV_Idempotent(t *testing.T) {
	dm, _ := newTestManager()
	ctx := context.Background()

	if err := dm.DeletePV(ctx, "nonexistent-pv"); err != nil {
		t.Fatalf("DeletePV on nonexistent PV should be idempotent: %v", err)
	}
}

// TestFinalizeBuild tests the full finalization flow with a pre-bound PVC.
// The fake clientset doesn't run a provisioner, so we manually create the
// PV and set the PVC to Bound state.
func TestFinalizeBuild(t *testing.T) {
	dm, cs := newTestManager()
	ctx := context.Background()

	pvName := "pv-test"
	pvcName := "pvc-test"

	// Create PV with Delete reclaim policy (simulating local-path default).
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("10Gi"),
			},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			ClaimRef: &corev1.ObjectReference{
				Name:      pvcName,
				Namespace: testNamespace,
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: "/tmp/test"},
			},
		},
	}
	if _, err := cs.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create PV: %v", err)
	}

	// Create PVC in Bound state pointing to the PV.
	sc := "local-path"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: testNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &sc,
			VolumeName:       pvName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("10Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: corev1.ClaimBound,
		},
	}
	if _, err := cs.CoreV1().PersistentVolumeClaims(testNamespace).Create(ctx, pvc, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create PVC: %v", err)
	}

	gotPV, err := dm.FinalizeBuild(ctx, pvcName)
	if err != nil {
		t.Fatalf("FinalizeBuild: %v", err)
	}
	if gotPV != pvName {
		t.Errorf("FinalizeBuild returned PV %q, want %q", gotPV, pvName)
	}

	// Verify PV state.
	finalPV, err := cs.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get PV: %v", err)
	}
	if finalPV.Spec.ClaimRef != nil {
		t.Errorf("PV claimRef should be nil, got %+v", finalPV.Spec.ClaimRef)
	}
	if finalPV.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		t.Errorf("reclaimPolicy = %v, want Retain", finalPV.Spec.PersistentVolumeReclaimPolicy)
	}
	if len(finalPV.Spec.AccessModes) != 1 || finalPV.Spec.AccessModes[0] != corev1.ReadOnlyMany {
		t.Errorf("accessModes = %v, want [ReadOnlyMany]", finalPV.Spec.AccessModes)
	}

	// Verify PVC deleted.
	_, err = cs.CoreV1().PersistentVolumeClaims(testNamespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err == nil {
		t.Error("PVC should be deleted after FinalizeBuild")
	}
}
