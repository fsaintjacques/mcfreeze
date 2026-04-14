//go:build gke

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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
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
	// gkeNamespace is the namespace where the Helm chart deploys mcfreeze.
	// The test reuses this namespace rather than creating its own.
	gkeNamespace = "mcfreeze-system"
)

// gkeRequireEnv returns the value of the environment variable key, or skips the test.
func gkeRequireEnv(t *testing.T, key string) string {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		t.Skipf("required env var %s is not set", key)
	}
	return v
}

// TestGKEE2E_GCSProtobufDescriptor exercises the protobuf descriptor_uri
// flow end-to-end on a GKE cluster using the BigQuery public stackoverflow
// users dataset.
//
// Prerequisites: Helm chart already deployed to mcfreeze-system via
// `make helm-install-gke`.
//
//	Terraform uploads compiled stackoverflow.desc to GCS →
//	create Dataset with BigQuery source + descriptor_uri + includeKeyInValue →
//	builder reads BQ, downloads descriptor from GCS, encodes protobuf →
//	node-agent serves protobuf-encoded values →
//	memcache GET returns non-empty protobuf bytes for known user ids
//
// Environment variables:
//
//	GCP_PROJECT                 — GCP billing project for BigQuery read sessions (required)
//	MCFREEZE_E2E_DESCRIPTOR_URI — gs:// URI to stackoverflow.desc (required; from terraform output stackoverflow_descriptor_uri)
func TestGKEE2E_GCSProtobufDescriptor(t *testing.T) {
	gcpProject := gkeRequireEnv(t, "GCP_PROJECT")
	descriptorURI := gkeRequireEnv(t, "MCFREEZE_E2E_DESCRIPTOR_URI")

	cs, config := gkeClientAndConfig(t)
	kc := gkeCRClient(t, config)
	ns := gkeNamespace
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	// 1. Verify the Helm-deployed control-plane is ready.
	gkeWaitForDeploymentReady(t, ctx, cs, ns, "mcfreeze-control-plane", 3*time.Minute)

	// 2. Port-forward to control-plane.
	cpLocalPort := gkePortForwardPod(t, ctx, cs, config, ns, "app.kubernetes.io/component=control-plane", 8080)
	cpURL := fmt.Sprintf("http://127.0.0.1:%d", cpLocalPort)
	t.Logf("control-plane port-forwarded to %s", cpURL)

	// 3. Apply Dataset CR with BigQuery source + protobuf descriptor_uri.
	// Reads the full stackoverflow.users table (~24M rows), selecting only the
	// columns present in the protobuf schema. IncludeKeyInValue is set so the
	// id column appears in the encoded protobuf value alongside the other fields.
	spec := api.DatasetSpec{
		Name:       "so-users",
		KeyPrefix:  "so-users",
		ShardCount: 64,
		Retention:  1,
		Source: api.SourceSpec{
			KeyColumn:         "id",
			IncludeKeyInValue: true,
			Encoding: &api.EncodingSpec{
				Protobuf: &api.ProtobufEncoding{
					DescriptorURI: descriptorURI,
					MessageName:   "stackoverflow.UserProfile",
				},
			},
			BigQuery: &api.BigQuerySource{
				Project:        gcpProject,
				Table:          "bigquery-public-data.stackoverflow.users",
				SelectedFields: []string{"id", "display_name", "age", "creation_date", "last_access_date", "location", "reputation", "up_votes", "down_votes", "views"},
			},
		},
	}
	gkeApplyDataset(t, ctx, kc, ns, spec)
	gkeTriggerBuild(t, cpURL, "so-users", "v1", spec)

	// 4. Wait for v1 to become active.
	gkeWaitForActiveVersion(t, ctx, kc, ns, "so-users", "v1", 5*time.Minute)
	t.Log("v1 is active")

	// 5. Wait for DaemonSet and rollout convergence.
	gkeWaitForDaemonSetReady(t, ctx, cs, ns, "mcfreeze-node-agent", 2*time.Minute)
	gkeWaitForRolloutConverged(t, ctx, kc, ns, "so-users", "v1", 3*time.Minute)
	t.Log("v1 rolled out to all nodes")

	// 6. Verify memcache responses contain non-empty protobuf-encoded data.
	// User 1 = Jeff Atwood (co-founder), 22656 = Jon Skeet — both guaranteed
	// present in the stackoverflow.users table.
	mcLocalPort := gkePortForwardPod(t, ctx, cs, config, ns, "app.kubernetes.io/component=node-agent", 11211)
	mcAddr := fmt.Sprintf("127.0.0.1:%d", mcLocalPort)
	gkeAssertMcGetNonEmpty(t, mcAddr, "so-users:1")
	gkeAssertMcGetNonEmpty(t, mcAddr, "so-users:22656")
	t.Log("v1 memcache responses verified (non-empty protobuf values for user 1 and 22656)")
}

// ---------------------------------------------------------------------------
// Kubernetes client helpers
// ---------------------------------------------------------------------------

func gkeClientAndConfig(t *testing.T) (kubernetes.Interface, *rest.Config) {
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

func gkeCRClient(t *testing.T, config *rest.Config) ctrlclient.Client {
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
// Wait helpers
// ---------------------------------------------------------------------------

func gkeWaitForDeploymentReady(t *testing.T, ctx context.Context, cs kubernetes.Interface, ns, name string, timeout time.Duration) {
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

func gkeWaitForDaemonSetReady(t *testing.T, ctx context.Context, cs kubernetes.Interface, ns, name string, timeout time.Duration) {
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

func gkeWaitForActiveVersion(t *testing.T, ctx context.Context, kc ctrlclient.Client, ns, dataset, version string, timeout time.Duration) {
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

func gkeWaitForRolloutConverged(t *testing.T, ctx context.Context, kc ctrlclient.Client, ns, dataset, version string, timeout time.Duration) {
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

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

func gkeApplyDataset(t *testing.T, ctx context.Context, kc ctrlclient.Client, ns string, spec api.DatasetSpec) {
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
	if err := kc.Create(ctx, desired); err != nil {
		t.Fatalf("create Dataset %q: %v", spec.Name, err)
	}
}

func gkeTriggerBuild(t *testing.T, cpURL, dataset, versionID string, spec api.DatasetSpec) {
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

func gkePortForwardPod(t *testing.T, ctx context.Context, cs kubernetes.Interface, config *rest.Config, ns, labelSelector string, remotePort int) int {
	t.Helper()

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

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	localPort := l.Addr().(*net.TCPAddr).Port
	l.Close()

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
		_ = fw.ForwardPorts()
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

// gkeAssertMcGetNonEmpty verifies that a memcache GET for key returns a
// non-empty value. Used for protobuf-encoded values where the exact bytes
// depend on the transcoder.
func gkeAssertMcGetNonEmpty(t *testing.T, addr, key string) {
	t.Helper()
	var lastErr error
	for range 10 {
		got, err := gkeMcGet(addr, key)
		if err == nil && len(got) > 0 {
			return
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("mg %s returned empty value", key)
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("gkeAssertMcGetNonEmpty %s: %v", key, lastErr)
}

func gkeMcGet(addr, key string) (string, error) {
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
