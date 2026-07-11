// SPDX-License-Identifier: Apache-2.0

//go:build kind

package controlplane_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/fsaintjacques/mcfreeze/go/api"
	v1alpha1 "github.com/fsaintjacques/mcfreeze/go/api/v1alpha1"
)

var (
	gvrDatasets        = schema.GroupVersionResource{Group: "mcfreeze.dev", Version: "v1alpha1", Resource: "datasets"}
	gvrDatasetVersions = schema.GroupVersionResource{Group: "mcfreeze.dev", Version: "v1alpha1", Resource: "datasetversions"}
)

// TestKindE2E_CRDStore exercises the CRD-backed Store end-to-end:
//
//  1. Build pipeline runs with --store=crd (default).
//  2. Dataset + DatasetVersion CRs exist after the build.
//  3. Restarting the control-plane preserves state.
//  4. Deleting the Dataset CR cascade-deletes its DatasetVersion CRs
//     (and via ownerRefs, the Job/ConfigMap/PVC).
func TestKindE2E_CRDStore(t *testing.T) {
	cs, config := kindClientAndConfig(t)
	kc := kindCRClient(t, config)
	dyn, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatalf("create dynamic client: %v", err)
	}
	ns := kindE2ENamespace(t, cs)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// 1. Deploy.
	deployRBAC(t, ctx, cs, ns)
	deployService(t, ctx, cs, ns)
	deployControlPlane(t, ctx, cs, ns)
	deployDaemonSet(t, ctx, cs, ns)
	waitForDeploymentReady(t, ctx, cs, ns, "mcfreeze-control-plane", 3*time.Minute)

	cpLocalPort := portForwardPod(t, ctx, cs, config, ns, "app=mcfreeze-control-plane", 8080)
	cpURL := fmt.Sprintf("http://127.0.0.1:%d", cpLocalPort)

	// 2. Apply Dataset CR (configuration), then trigger v1 build (instance).
	usersSpec := api.DatasetSpec{
		Name:       "users",
		KeyPrefix:  "users",
		ShardCount: 2,
		Retention:  2,
		Source: api.SourceSpec{
			KeyColumn:   "key",
			ValueColumn: "value",
			CSV:         &api.CsvSource{Data: "key,value\nuser-1,Alice"},
		},
	}
	applyDataset(t, ctx, kc, ns, usersSpec)
	triggerBuild(t, cpURL, "users", "v1", usersSpec)

	waitForActiveVersion(t, ctx, kc, ns, "users", "v1", 5*time.Minute)
	waitForDaemonSetReady(t, ctx, cs, ns, "mcfreeze-node-agent", 2*time.Minute)
	waitForRolloutConverged(t, ctx, kc, ns, "users", "v1", 3*time.Minute)

	// 3. Assert Dataset CR exists.
	if _, err := dyn.Resource(gvrDatasets).Namespace(ns).Get(ctx, "users", metav1.GetOptions{}); err != nil {
		t.Fatalf("Dataset CR users not found: %v", err)
	}
	t.Log("Dataset CR exists")

	// 4. Assert DatasetVersion CR exists with state=active.
	dvName := v1alpha1.VersionCRName("users", "v1")
	dv, err := dyn.Resource(gvrDatasetVersions).Namespace(ns).Get(ctx, dvName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("DatasetVersion CR %s not found: %v", dvName, err)
	}
	state, _, _ := unstructuredString(dv.Object, "status", "state")
	if state != string(api.StateActive) {
		t.Fatalf("DatasetVersion %s state = %q, want active", dvName, state)
	}
	// ownerRef → Dataset.
	owners := dv.GetOwnerReferences()
	if len(owners) != 1 || owners[0].Kind != "Dataset" || owners[0].Name != "users" {
		t.Fatalf("DatasetVersion ownerRefs = %+v, want Dataset/users", owners)
	}
	t.Log("DatasetVersion CR has correct state and ownerRef")

	// 5. Restart the control-plane and verify state is preserved.
	restartControlPlane(t, ctx, cs, ns)
	waitForDeploymentReady(t, ctx, cs, ns, "mcfreeze-control-plane", 2*time.Minute)
	_ = portForwardPod(t, ctx, cs, config, ns, "app=mcfreeze-control-plane", 8080)

	// State is reconstructed by the controller-manager from CRDs in the
	// apiserver — no need to talk to the new pod's HTTP endpoint.
	waitForActiveVersion(t, ctx, kc, ns, "users", "v1", 2*time.Minute)
	t.Log("control-plane restart preserved active version")

	// 6. Delete the Dataset CR → cascade should remove DatasetVersion.
	if err := dyn.Resource(gvrDatasets).Namespace(ns).Delete(ctx, "users", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete Dataset CR: %v", err)
	}

	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		_, err := dyn.Resource(gvrDatasetVersions).Namespace(ns).Get(ctx, dvName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			t.Log("DatasetVersion CR cascade-deleted")
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("DatasetVersion %s not cascade-deleted within deadline", dvName)
}

func restartControlPlane(t *testing.T, ctx context.Context, cs kubernetes.Interface, ns string) {
	t.Helper()
	pods, err := cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "app=mcfreeze-control-plane"})
	if err != nil {
		t.Fatalf("list control-plane pods: %v", err)
	}
	deleted := make(map[string]bool)
	zero := int64(0)
	for _, p := range pods.Items {
		if err := cs.CoreV1().Pods(ns).Delete(ctx, p.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
			t.Fatalf("delete control-plane pod %s: %v", p.Name, err)
		}
		deleted[p.Name] = true
	}
	// Wait for the deleted pods to actually disappear so the next
	// portForwardPod call doesn't race onto a terminating pod.
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		cur, err := cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "app=mcfreeze-control-plane"})
		if err != nil {
			t.Fatalf("list control-plane pods: %v", err)
		}
		stillThere := false
		for _, p := range cur.Items {
			if deleted[p.Name] {
				stillThere = true
				break
			}
		}
		if !stillThere {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("deleted control-plane pods did not terminate within deadline")
}

func unstructuredString(obj map[string]interface{}, path ...string) (string, bool, error) {
	var cur interface{} = obj
	for _, p := range path {
		m, ok := cur.(map[string]interface{})
		if !ok {
			return "", false, nil
		}
		cur, ok = m[p]
		if !ok {
			return "", false, nil
		}
	}
	s, ok := cur.(string)
	return s, ok, nil
}
