//go:build kind

package controlplane_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/fsaintjacques/mcfreeze/go/api"
	v1alpha1 "github.com/fsaintjacques/mcfreeze/go/api/v1alpha1"
)

const (
	defaultMcFreezeImage = "mcfreeze:dev"
	storageClass         = "csi-hostpath-sc"
	csiDriver            = "hostpath.csi.k8s.io"
)

// mcfreezeImageRef honors MCFREEZE_IMAGE so podman-based local dev (which
// tags images as localhost/mcfreeze:dev) can override the default.
func mcfreezeImageRef() string {
	if v := os.Getenv("MCFREEZE_IMAGE"); v != "" {
		return v
	}
	return defaultMcFreezeImage
}

// TestKindE2E_FullPipeline exercises the complete Phase 3 pipeline in KIND:
//
//	deploy control-plane + node-agent →
//	trigger v1 build via HTTP API →
//	Job writes snapshot to PVC → finalize PV →
//	node-agent attaches, mounts, writes catalog.json →
//	KV server serves data via memcache →
//	trigger v2 build → hot-swap → verify new data →
//	check v1 eligible for retirement
func TestKindE2E_FullPipeline(t *testing.T) {
	cs, config := kindClientAndConfig(t)
	kc := kindCRClient(t, config)
	ns := kindE2ENamespace(t, cs)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// 1. Deploy RBAC, Service, control-plane, node-agent.
	deployRBAC(t, ctx, cs, ns)
	deployService(t, ctx, cs, ns)
	deployControlPlane(t, ctx, cs, ns)
	deployDaemonSet(t, ctx, cs, ns)

	// 2. Wait for control-plane Deployment to be ready.
	waitForDeploymentReady(t, ctx, cs, ns, "mcfreeze-control-plane", 3*time.Minute)

	// 3. Port-forward to control-plane.
	cpLocalPort := portForwardPod(t, ctx, cs, config, ns, "app=mcfreeze-control-plane", 8080)
	cpURL := fmt.Sprintf("http://127.0.0.1:%d", cpLocalPort)
	t.Logf("control-plane port-forwarded to %s", cpURL)

	// 4. Apply Dataset CR (configuration), then trigger v1 build (instance).
	v1CSV := "key,value\nuser-1,Alice\nuser-2,Bob"
	usersSpec := api.DatasetSpec{
		Name:       "users",
		KeyPrefix:  "users",
		ShardCount: 2,
		Retention:  2,
		Source: api.SourceSpec{
			KeyColumn:   "key",
			ValueColumn: "value",
			CSV:         &api.CsvSource{Data: v1CSV},
		},
	}
	applyDataset(t, ctx, kc, ns, usersSpec)
	triggerBuild(t, cpURL, "users", "v1", usersSpec)

	// 5. Wait for v1 to become active.
	waitForActiveVersion(t, ctx, kc, ns, "users", "v1", 5*time.Minute)
	t.Log("v1 is active")

	// 6. Wait for DaemonSet to be ready.
	waitForDaemonSetReady(t, ctx, cs, ns, "mcfreeze-node-agent", 2*time.Minute)

	// 7. Wait for node-agent convergence.
	waitForRolloutConverged(t, ctx, kc, ns, "users", "v1", 3*time.Minute)
	t.Log("v1 rolled out to all nodes")

	// 8. Verify memcache responses.
	mcLocalPort := portForwardPod(t, ctx, cs, config, ns, "app=mcfreeze-node-agent", 11211)
	mcAddr := fmt.Sprintf("127.0.0.1:%d", mcLocalPort)
	assertMcGet(t, mcAddr, "users:user-1", "Alice")
	assertMcGet(t, mcAddr, "users:user-2", "Bob")
	t.Log("v1 memcache responses verified")

	// 9. Trigger v2 build (Dataset CR already applied; v2 reuses it with
	// updated source data).
	v2CSV := "key,value\nuser-1,Alice-v2\nuser-2,Bob-v2\nuser-3,Charlie"
	usersV2Spec := usersSpec
	usersV2Spec.Source.CSV = &api.CsvSource{Data: v2CSV}
	applyDataset(t, ctx, kc, ns, usersV2Spec)
	triggerBuild(t, cpURL, "users", "v2", usersV2Spec)

	// 10. Wait for v2 active + converged.
	waitForActiveVersion(t, ctx, kc, ns, "users", "v2", 5*time.Minute)
	waitForRolloutConverged(t, ctx, kc, ns, "users", "v2", 3*time.Minute)
	t.Log("v2 rolled out")

	// 11. Verify v2 memcache responses.
	assertMcGet(t, mcAddr, "users:user-1", "Alice-v2")
	assertMcGet(t, mcAddr, "users:user-2", "Bob-v2")
	assertMcGet(t, mcAddr, "users:user-3", "Charlie")
	t.Log("v2 memcache responses verified")

	// 12. Verify v1 is fully retired. With the VolumeAttachment-driven
	// retirement reconciler, "retirement complete" means the CR has been
	// deleted (the previous broker-based model left it in state=retired).
	// Poll because retirement happens asynchronously after v2's rollout
	// causes the node-agent to detach v1's PV.
	deadline := time.Now().Add(2 * time.Minute)
	v1Key := ctrlclient.ObjectKey{Namespace: ns, Name: v1alpha1.VersionCRName("users", "v1")}
	for time.Now().Before(deadline) {
		var dv v1alpha1.DatasetVersion
		err := kc.Get(ctx, v1Key, &dv)
		if apierrors.IsNotFound(err) {
			t.Log("v1 retirement complete (CR deleted) — PASS")
			return
		}
		if err != nil {
			t.Fatalf("get DatasetVersion users-v1: %v", err)
		}
		t.Logf("v1 still present, state=%s pv=%s", dv.Status.State, dv.Status.PVName)
		select {
		case <-ctx.Done():
			t.Fatalf("context cancelled waiting for v1 retirement")
		case <-time.After(3 * time.Second):
		}
	}
	t.Fatal("v1 was not retired (CR still exists) within 2 minutes")
}

// ---------------------------------------------------------------------------
// Infrastructure helpers
// ---------------------------------------------------------------------------

func kindClientAndConfig(t *testing.T) (kubernetes.Interface, *rest.Config) {
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
	return cs, config
}

func kindE2ENamespace(t *testing.T, cs kubernetes.Interface) string {
	t.Helper()
	ctx := context.Background()
	name := fmt.Sprintf("test-e2e-%d", time.Now().UnixNano()%100000)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if _, err := cs.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	t.Cleanup(func() {
		cleanCtx := context.Background()

		// Delete VolumeAttachments created by the node-agent (cluster-scoped).
		vas, _ := cs.StorageV1().VolumeAttachments().List(cleanCtx, metav1.ListOptions{})
		if vas != nil {
			for _, va := range vas.Items {
				if strings.HasPrefix(va.Name, "mcf-va-") {
					cs.StorageV1().VolumeAttachments().Delete(cleanCtx, va.Name, metav1.DeleteOptions{})
				}
			}
		}

		// Delete cluster-scoped PVs created during this test.
		pvs, _ := cs.CoreV1().PersistentVolumes().List(cleanCtx, metav1.ListOptions{})
		if pvs != nil {
			for _, pv := range pvs.Items {
				if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Namespace == name {
					cs.CoreV1().PersistentVolumes().Delete(cleanCtx, pv.Name, metav1.DeleteOptions{})
				}
			}
		}
		// Also delete unbound PVs that were finalized (claimRef cleared).
		pvs2, _ := cs.CoreV1().PersistentVolumes().List(cleanCtx, metav1.ListOptions{})
		if pvs2 != nil {
			for _, pv := range pvs2.Items {
				if pv.Spec.ClaimRef == nil && pv.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimRetain {
					cs.CoreV1().PersistentVolumes().Delete(cleanCtx, pv.Name, metav1.DeleteOptions{})
				}
			}
		}

		cs.CoreV1().Namespaces().Delete(cleanCtx, name, metav1.DeleteOptions{})
	})
	return name
}

// ---------------------------------------------------------------------------
// Deploy helpers
// ---------------------------------------------------------------------------

func deployRBAC(t *testing.T, ctx context.Context, cs kubernetes.Interface, ns string) {
	t.Helper()

	// Control-plane ServiceAccount.
	cs.CoreV1().ServiceAccounts(ns).Create(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "mcfreeze-control-plane"},
	}, metav1.CreateOptions{})

	// Control-plane namespaced Role.
	cs.RbacV1().Roles(ns).Create(ctx, &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "mcfreeze-control-plane"},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{"batch"}, Resources: []string{"jobs"}, Verbs: []string{"create", "get", "update", "delete", "list", "watch"}},
			{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"create", "get", "delete"}},
			{APIGroups: []string{""}, Resources: []string{"persistentvolumeclaims"}, Verbs: []string{"create", "get", "update", "delete", "list", "watch"}},
			{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"list", "delete"}},
			{APIGroups: []string{"mcfreeze.dev"}, Resources: []string{"datasets", "datasetversions", "datasetbindings"}, Verbs: []string{"create", "get", "update", "delete", "list", "watch", "patch"}},
			{APIGroups: []string{"mcfreeze.dev"}, Resources: []string{"datasets/status", "datasetversions/status"}, Verbs: []string{"get", "update", "patch"}},
			// Phase 5: lease-based leader election + events.
			{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
			{APIGroups: []string{""}, Resources: []string{"events"}, Verbs: []string{"create", "patch"}},
		},
	}, metav1.CreateOptions{})

	cs.RbacV1().RoleBindings(ns).Create(ctx, &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "mcfreeze-control-plane"},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "mcfreeze-control-plane", Namespace: ns}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "mcfreeze-control-plane"},
	}, metav1.CreateOptions{})

	// Control-plane cluster-scoped ClusterRole for PVs.
	crName := "mcfreeze-cp-" + ns // unique per test namespace
	cs.RbacV1().ClusterRoles().Create(ctx, &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: crName},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"nodes"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{""}, Resources: []string{"persistentvolumes"}, Verbs: []string{"create", "get", "update", "delete", "list"}},
			{APIGroups: []string{"storage.k8s.io"}, Resources: []string{"volumeattachments"}, Verbs: []string{"get", "list", "watch"}},
		},
	}, metav1.CreateOptions{})
	t.Cleanup(func() { cs.RbacV1().ClusterRoles().Delete(context.Background(), crName, metav1.DeleteOptions{}) })

	cs.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: crName},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "mcfreeze-control-plane", Namespace: ns}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: crName},
	}, metav1.CreateOptions{})
	t.Cleanup(func() { cs.RbacV1().ClusterRoleBindings().Delete(context.Background(), crName, metav1.DeleteOptions{}) })

	// Node-agent ServiceAccount.
	cs.CoreV1().ServiceAccounts(ns).Create(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "mcfreeze-node-agent"},
	}, metav1.CreateOptions{})

	// Node-agent ClusterRole for VolumeAttachments.
	naCRName := "mcfreeze-na-" + ns
	cs.RbacV1().ClusterRoles().Create(ctx, &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: naCRName},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{"storage.k8s.io"}, Resources: []string{"volumeattachments"}, Verbs: []string{"create", "get", "delete", "list", "watch"}},
			{APIGroups: []string{""}, Resources: []string{"persistentvolumes"}, Verbs: []string{"get"}},
		},
	}, metav1.CreateOptions{})
	t.Cleanup(func() { cs.RbacV1().ClusterRoles().Delete(context.Background(), naCRName, metav1.DeleteOptions{}) })

	cs.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: naCRName},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "mcfreeze-node-agent", Namespace: ns}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: naCRName},
	}, metav1.CreateOptions{})
	t.Cleanup(func() {
		cs.RbacV1().ClusterRoleBindings().Delete(context.Background(), naCRName, metav1.DeleteOptions{})
	})
}

func deployService(t *testing.T, ctx context.Context, cs kubernetes.Interface, ns string) {
	t.Helper()
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "mcfreeze-control-plane"},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "mcfreeze-control-plane"},
			Ports:    []corev1.ServicePort{{Port: 8080, TargetPort: intstr.FromInt32(8080)}},
		},
	}
	if _, err := cs.CoreV1().Services(ns).Create(ctx, svc, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create service: %v", err)
	}
}

func deployControlPlane(t *testing.T, ctx context.Context, cs kubernetes.Interface, ns string) {
	t.Helper()
	replicas := int32(1)
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "mcfreeze-control-plane"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "mcfreeze-control-plane"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "mcfreeze-control-plane"}},
				Spec: corev1.PodSpec{
					ServiceAccountName: "mcfreeze-control-plane",
					Containers: []corev1.Container{{
						Name:            "control-plane",
						Image:           mcfreezeImageRef(),
						ImagePullPolicy: corev1.PullNever,
						Args: []string{
							"control-plane",
							"--listen=:8080",
							"--namespace=" + ns,
							"--image=" + mcfreezeImageRef(),
							"--image-pull-policy=Never",
							"--storage-class=" + storageClass,
							"--disk-size-gb=1",
							"--leader-elect=true",
							"--metrics-bind-address=:8081",
							"--health-probe-bind-address=:8082",
						},
						Ports: []corev1.ContainerPort{
							{Name: "http", ContainerPort: 8080},
							{Name: "metrics", ContainerPort: 8081},
							{Name: "health", ContainerPort: 8082},
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/healthz",
									Port: intstr.FromString("health"),
								},
							},
							InitialDelaySeconds: 5,
							PeriodSeconds:       10,
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/readyz",
									Port: intstr.FromString("health"),
								},
							},
							InitialDelaySeconds: 5,
							PeriodSeconds:       5,
						},
					}},
				},
			},
		},
	}
	if _, err := cs.AppsV1().Deployments(ns).Create(ctx, dep, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create deployment: %v", err)
	}
}

func deployDaemonSet(t *testing.T, ctx context.Context, cs kubernetes.Interface, ns string) {
	t.Helper()
	privileged := true
	bidir := corev1.MountPropagationBidirectional
	h2c := corev1.MountPropagationHostToContainer
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: "mcfreeze-node-agent"},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "mcfreeze-node-agent"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "mcfreeze-node-agent"}},
				Spec: corev1.PodSpec{
					ServiceAccountName: "mcfreeze-node-agent",
					Containers: []corev1.Container{
						{
							Name:            "node-agent",
							Image:           mcfreezeImageRef(),
							ImagePullPolicy: corev1.PullNever,
							Args: []string{
								"node-agent",
								"--control-plane-url=http://mcfreeze-control-plane:8080",
								"--csi-driver=" + csiDriver,
								"--mounter=fs",
								"--mount-base=/mnt/kv",
								"--catalog-dir=/run/kv",
							},
							Env: []corev1.EnvVar{{
								Name: "NODE_NAME",
								ValueFrom: &corev1.EnvVarSource{
									FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"},
								},
							}},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "catalog", MountPath: "/run/kv"},
								{Name: "mounts", MountPath: "/mnt/kv", MountPropagation: &bidir},
								{Name: "csi-data", MountPath: "/var/lib/csi-hostpath-data", ReadOnly: true},
							},
							SecurityContext: &corev1.SecurityContext{Privileged: &privileged},
						},
						{
							Name:            "kv-server",
							Image:           mcfreezeImageRef(),
							ImagePullPolicy: corev1.PullNever,
							Command:         []string{"mcf"},
							Args: []string{
								"serve", "catalog",
								"--catalog=/run/kv/catalog.json",
								"--tcp=0.0.0.0:11211",
								"--metrics=0.0.0.0:7777",
							},
							Ports: []corev1.ContainerPort{
								{ContainerPort: 11211, Name: "memcache"},
								{ContainerPort: 7777, Name: "metrics"},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "catalog", MountPath: "/run/kv", ReadOnly: true},
								{Name: "mounts", MountPath: "/mnt/kv", MountPropagation: &h2c},
								{Name: "csi-data", MountPath: "/var/lib/csi-hostpath-data", ReadOnly: true},
							},
						},
					},
					Volumes: []corev1.Volume{
						{Name: "catalog", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
						{Name: "mounts", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
						{Name: "csi-data", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
							Path: "/var/lib/csi-hostpath-data",
							Type: hostPathTypePtr(corev1.HostPathDirectoryOrCreate),
						}}},
					},
				},
			},
		},
	}
	if _, err := cs.AppsV1().DaemonSets(ns).Create(ctx, ds, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create daemonset: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Wait helpers
// ---------------------------------------------------------------------------

func waitForDeploymentReady(t *testing.T, ctx context.Context, cs kubernetes.Interface, ns, name string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		dep, err := cs.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
		if err == nil && dep.Status.ReadyReplicas >= 1 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context cancelled waiting for deployment %s", name)
		case <-time.After(2 * time.Second):
		}
	}
	t.Fatalf("deployment %s not ready within %v", name, timeout)
}

func waitForDaemonSetReady(t *testing.T, ctx context.Context, cs kubernetes.Interface, ns, name string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ds, err := cs.AppsV1().DaemonSets(ns).Get(ctx, name, metav1.GetOptions{})
		if err == nil && ds.Status.NumberReady >= 1 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context cancelled waiting for daemonset %s", name)
		case <-time.After(2 * time.Second):
		}
	}
	t.Fatalf("daemonset %s not ready within %v", name, timeout)
}

// waitForActiveVersion polls the apiserver until a DatasetVersion CR for
// (dataset, version) reaches state=active.
func waitForActiveVersion(t *testing.T, ctx context.Context, kc ctrlclient.Client, ns, dataset, version string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	key := ctrlclient.ObjectKey{Namespace: ns, Name: v1alpha1.VersionCRName(dataset, version)}
	for time.Now().Before(deadline) {
		var v v1alpha1.DatasetVersion
		if err := kc.Get(ctx, key, &v); err == nil && v.Status.State == string(api.StateActive) {
			return
		}
		t.Logf("waiting for %s/%s to reach active", dataset, version)
		select {
		case <-ctx.Done():
			t.Fatalf("context cancelled waiting for active version %s", version)
		case <-time.After(3 * time.Second):
		}
	}
	t.Fatalf("version %s did not become active within %v", version, timeout)
}

// waitForRolloutConverged polls the active DatasetVersion CR's status.rollout
// until ConvergedNodes > 0 and Pending/Error are zero.
func waitForRolloutConverged(t *testing.T, ctx context.Context, kc ctrlclient.Client, ns, dataset, version string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	key := ctrlclient.ObjectKey{Namespace: ns, Name: v1alpha1.VersionCRName(dataset, version)}
	for time.Now().Before(deadline) {
		var v v1alpha1.DatasetVersion
		if err := kc.Get(ctx, key, &v); err == nil {
			r := v.Status.Rollout
			if v.Status.State == string(api.StateActive) && r != nil &&
				r.ConvergedNodes > 0 && r.PendingNodes == 0 && r.ErrorNodes == 0 {
				return
			}
			if r != nil {
				t.Logf("rollout %s/%s: state=%s total=%d converged=%d pending=%d error=%d",
					dataset, version, v.Status.State, r.TotalNodes, r.ConvergedNodes, r.PendingNodes, r.ErrorNodes)
			} else {
				t.Logf("rollout %s/%s: state=%s rollout=nil", dataset, version, v.Status.State)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context cancelled waiting for rollout convergence")
		case <-time.After(3 * time.Second):
		}
	}
	t.Fatalf("rollout for %s/%s did not converge within %v", dataset, version, timeout)
}

// applyDataset creates a Dataset CR from an api.DatasetSpec, or updates the
// spec if one already exists. Tests must call this before triggerBuild —
// the build endpoint is a pure trigger and refuses to create the parent.
func applyDataset(t *testing.T, ctx context.Context, kc ctrlclient.Client, ns string, spec api.DatasetSpec) {
	t.Helper()
	desired := v1alpha1.FromAPIDatasetSpec(spec)
	desired.Namespace = ns
	desired.Name = spec.Name

	var existing v1alpha1.Dataset
	err := kc.Get(ctx, ctrlclient.ObjectKey{Namespace: ns, Name: spec.Name}, &existing)
	if err == nil {
		existing.Spec = desired.Spec
		if err := kc.Update(ctx, &existing); err != nil {
			t.Fatalf("update Dataset %q: %v", spec.Name, err)
		}
		return
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("get Dataset %q: %v", spec.Name, err)
	}
	if err := kc.Create(ctx, desired); err != nil {
		t.Fatalf("create Dataset %q: %v", spec.Name, err)
	}
}

// kindCRClient builds a controller-runtime client from a rest.Config with the
// mcfreeze v1alpha1 scheme registered. Used by the kind e2e tests to read
// Dataset / DatasetVersion CRs directly from the apiserver.
func kindCRClient(t *testing.T, config *rest.Config) ctrlclient.Client {
	t.Helper()
	scheme := apiruntime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
	c, err := ctrlclient.New(config, ctrlclient.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("ctrlclient.New: %v", err)
	}
	return c
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

func triggerBuild(t *testing.T, cpURL, dataset, versionID string, spec api.DatasetSpec) {
	t.Helper()
	body := struct {
		Spec      api.DatasetSpec `json:"spec"`
		VersionID string          `json:"version_id"`
	}{Spec: spec, VersionID: versionID}

	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal build request: %v", err)
	}

	resp, err := http.Post(cpURL+"/api/v1/dataset/"+dataset+"/build", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("POST build: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("build trigger returned %d: %s", resp.StatusCode, respBody)
	}
	t.Logf("triggered build %s/%s", dataset, versionID)
}

// ---------------------------------------------------------------------------
// Port-forward helper
// ---------------------------------------------------------------------------

func portForwardPod(t *testing.T, ctx context.Context, cs kubernetes.Interface, config *rest.Config, ns, labelSelector string, remotePort int) int {
	t.Helper()

	// Find a running pod matching the label selector.
	var podName string
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		pods, err := cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
			LabelSelector: labelSelector,
		})
		if err == nil {
			for _, p := range pods.Items {
				if p.Status.Phase == corev1.PodRunning {
					podName = p.Name
					break
				}
			}
		}
		if podName != "" {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if podName == "" {
		t.Fatalf("no running pod found for selector %q in namespace %s", labelSelector, ns)
	}

	// Find a free local port.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	localPort := l.Addr().(*net.TCPAddr).Port
	l.Close()

	// Set up SPDY port-forward.
	url := cs.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(ns).
		Name(podName).
		SubResource("portforward").
		URL()

	transport, upgrader, err := spdy.RoundTripperFor(config)
	if err != nil {
		t.Fatalf("create round tripper: %v", err)
	}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, url)

	stopCh := make(chan struct{})
	readyCh := make(chan struct{})

	ports := []string{fmt.Sprintf("%d:%d", localPort, remotePort)}
	fw, err := portforward.New(dialer, ports, stopCh, readyCh, io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("create port-forward: %v", err)
	}

	go func() {
		if err := fw.ForwardPorts(); err != nil {
			// Only log if not due to stop.
			select {
			case <-stopCh:
			default:
				t.Logf("port-forward error: %v", err)
			}
		}
	}()

	select {
	case <-readyCh:
	case <-time.After(30 * time.Second):
		t.Fatal("port-forward not ready within 30s")
	}

	t.Cleanup(func() { close(stopCh) })
	return localPort
}

// ---------------------------------------------------------------------------
// Memcache assertion helpers
// ---------------------------------------------------------------------------

func assertMcGet(t *testing.T, addr, key, want string) {
	t.Helper()

	// Retry a few times — the KV server may still be loading.
	var lastErr error
	for range 10 {
		got, err := mcGet(addr, key)
		if err == nil && got == want {
			return
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("mg %s = %q, want %q", key, got, want)
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("assertMcGet %s: %v", key, lastErr)
}

func mcGet(addr, key string) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return "", fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))

	fmt.Fprintf(conn, "mg %s v\r\n", key)

	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("read status: %w", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "VA ") {
		return "", fmt.Errorf("expected VA, got %q", line)
	}
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return "", fmt.Errorf("malformed VA: %q", line)
	}
	vlen, err := strconv.Atoi(fields[1])
	if err != nil {
		return "", fmt.Errorf("bad length: %w", err)
	}
	buf := make([]byte, vlen+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", fmt.Errorf("read body: %w", err)
	}
	return string(buf[:vlen]), nil
}

func hostPathTypePtr(t corev1.HostPathType) *corev1.HostPathType { return &t }
