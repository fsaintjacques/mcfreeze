//go:build kind

package builder

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/fsaintjacques/frostmap/go/api"
	"github.com/fsaintjacques/frostmap/go/internal/controlplane/volume"
)

// kindImage returns the frostmap image ref used by kind tests, honoring the
// FROSTMAP_IMAGE env var so podman-based local dev (which tags images as
// localhost/frostmap:dev) can override the docker-provider default.
func kindImage() string {
	if v := os.Getenv("FROSTMAP_IMAGE"); v != "" {
		return v
	}
	return "frostmap:dev"
}

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

// kindNamespace creates a temporary namespace for the test and deletes it
// on cleanup.
func kindNamespace(t *testing.T, cs kubernetes.Interface) string {
	t.Helper()
	ctx := context.Background()

	name := fmt.Sprintf("test-phase1-%d", time.Now().UnixNano()%100000)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if _, err := cs.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	t.Cleanup(func() {
		// Delete orphaned PVs created by this test's namespace. PVs are
		// cluster-scoped and don't cascade with namespace deletion.
		pvs, _ := cs.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
		if pvs != nil {
			for _, pv := range pvs.Items {
				if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Namespace == name {
					cs.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{})
				}
			}
		}
		cs.CoreV1().Namespaces().Delete(ctx, name, metav1.DeleteOptions{})
	})
	return name
}

// TestKindJob_FullLifecycle runs a build Job in a KIND cluster and
// validates the full PVC → finalize → PV → delete lifecycle.
//
// Prerequisites:
//   - KIND cluster running with fm image loaded (make kind-up kind-load)
//   - local-path StorageClass available (default in KIND)
func TestKindJob_FullLifecycle(t *testing.T) {
	cs := kindClientset(t)
	ns := kindNamespace(t, cs)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dm := &volume.LocalPathManager{Client: cs, Namespace: ns}
	jb := &Job{
		Client:          cs,
		Volumes:         dm,
		Namespace:       ns,
		Image:           kindImage(),
		ImagePullPolicy: corev1.PullNever,
		StorageClass:    "standard",
		DiskSizeGB:      1,
	}

	spec := api.DatasetSpec{
		Name:       "kindtest",
		KeyPrefix:  "kindtest",
		ShardCount: 2,
		Retention:  1,
		Source: api.SourceSpec{
			KeyColumn:   "key",
			ValueColumn: "value",
			CSV:         &api.CsvSource{Data: "key,value\nk1,v1\nk2,v2"},
		},
	}

	// Start build.
	handle, err := jb.Start(ctx, spec, "v1")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Logf("started build, handle=%s", handle)

	// Poll until complete or failed.
	var status Status
	deadline := time.After(4 * time.Minute)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		status, err = jb.Poll(ctx, handle)
		if err != nil {
			t.Fatalf("Poll: %v", err)
		}
		if status.Phase == Complete || status.Phase == Failed {
			break
		}
		t.Logf("build phase: %s", status.Phase)

		select {
		case <-deadline:
			t.Fatalf("timed out waiting for build (last phase: %s)", status.Phase)
		case <-ticker.C:
		}
	}

	if status.Phase != Complete {
		t.Fatalf("build failed: %s", status.Error)
	}

	pvName := status.Result.PVName
	t.Logf("build complete, PV=%s", pvName)

	// Verify PV state.
	pv, err := cs.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get PV: %v", err)
	}
	if pv.Spec.ClaimRef != nil {
		t.Errorf("PV claimRef should be nil, got %+v", pv.Spec.ClaimRef)
	}
	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		t.Errorf("reclaimPolicy = %v, want Retain", pv.Spec.PersistentVolumeReclaimPolicy)
	}
	if len(pv.Spec.AccessModes) != 1 || pv.Spec.AccessModes[0] != corev1.ReadOnlyMany {
		t.Errorf("accessModes = %v, want [ReadOnlyMany]", pv.Spec.AccessModes)
	}

	// A full reader-pod test mounting the PV and verifying meta.json requires
	// CSI hostpath driver support for ReadOnlyMany, which is validated in the
	// Phase 3 end-to-end test. PV properties are verified above.
	t.Log("PV properties verified; reader pod validated in Phase 3 e2e")

	// Delete PV.
	if err := dm.DeletePV(ctx, pvName); err != nil {
		t.Fatalf("DeletePV: %v", err)
	}

	// Verify PV gone or terminating.
	pvAfter, pvErr := cs.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if pvErr == nil && pvAfter.DeletionTimestamp == nil {
		t.Error("PV should be deleted or terminating")
	}
	t.Log("PV deleted successfully")
}

// TestKindJob_Cancel verifies Cancel cleans up resources in KIND.
func TestKindJob_Cancel(t *testing.T) {
	cs := kindClientset(t)
	ns := kindNamespace(t, cs)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	dm := &volume.LocalPathManager{Client: cs, Namespace: ns}
	jb := &Job{
		Client:          cs,
		Volumes:         dm,
		Namespace:       ns,
		Image:           kindImage(),
		ImagePullPolicy: corev1.PullNever,
		StorageClass:    "standard",
		DiskSizeGB:      1,
	}

	spec := api.DatasetSpec{
		Name:       "canceltest",
		KeyPrefix:  "canceltest",
		ShardCount: 2,
		Retention:  1,
		Source: api.SourceSpec{
			KeyColumn:   "key",
			ValueColumn: "value",
			CSV:         &api.CsvSource{Data: "key,value\nk1,v1\nk2,v2"},
		},
	}

	handle, err := jb.Start(ctx, spec, "v1")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := jb.Cancel(ctx, handle); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	// Verify resources cleaned up.
	_, err = cs.BatchV1().Jobs(ns).Get(ctx, string(handle), metav1.GetOptions{})
	if err == nil {
		t.Error("Job should be deleted after Cancel")
	}
	_, err = cs.CoreV1().ConfigMaps(ns).Get(ctx, "fm-config-canceltest-v1", metav1.GetOptions{})
	if err == nil {
		t.Error("ConfigMap should be deleted after Cancel")
	}
	pvc, pvcErr := cs.CoreV1().PersistentVolumeClaims(ns).Get(ctx, "fm-pvc-canceltest-v1", metav1.GetOptions{})
	if pvcErr == nil && pvc.DeletionTimestamp == nil {
		t.Error("PVC should be deleted or terminating after Cancel")
	}

	t.Log("Cancel cleanup verified")
}
