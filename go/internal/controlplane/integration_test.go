//go:build integration

package controlplane_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/fsaintjacques/mcfreeze/go/api"
	v1alpha1 "github.com/fsaintjacques/mcfreeze/go/api/v1alpha1"
	"github.com/fsaintjacques/mcfreeze/go/internal/controlplane/builder"
	"github.com/fsaintjacques/mcfreeze/go/internal/nodeagent"
	"github.com/fsaintjacques/mcfreeze/go/internal/nodeagent/assignment"
	"github.com/fsaintjacques/mcfreeze/go/internal/nodeagent/mount"
	"github.com/fsaintjacques/mcfreeze/go/internal/nodeagent/version"
	"github.com/fsaintjacques/mcfreeze/go/internal/nodeagent/volume"
	"github.com/fsaintjacques/mcfreeze/go/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestFullLoop exercises the entire pipeline against a real apiserver via
// envtest:
//
//	test creates Dataset + DatasetVersion CRs →
//	DatasetVersionReconciler runs the fork builder →
//	NodeAssignmentReconciler pushes to broker →
//	node-agent reconciles (attach, mount, catalog, version confirm) →
//	KV server serves data via memcache →
//	control-plane sees PhaseActive in NodeState →
//	upgrade to v2 → re-verify
func TestFullLoop(t *testing.T) {
	testutil.EnvtestSkipIfBinariesMissing(t)

	volumeBase := t.TempDir()
	buildBase := t.TempDir()

	b := &builder.Fake{
		FMBinary:   testutil.FMBinary(t),
		Partitions: 4,
		OutputBase: buildBase,
		Data: map[string][][2][]byte{
			"users": {
				{[]byte("user-1"), []byte("Alice")},
				{[]byte("user-2"), []byte("Bob")},
			},
		},
	}

	cp := testutil.NewControlPlane(t, b, volumeBase)

	// Dataset CR (drives the reconcilers).
	cp.CreateDataset(t, &v1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{Name: "users"},
		Spec: v1alpha1.DatasetSpec{
			KeyPrefix:  "users",
			ShardCount: 4,
			Retention:  2,
			Source:     v1alpha1.SourceSpec{KeyColumn: "key", ValueColumn: "value"},
		},
	})

	// --- KV server ---
	kvSrv := testutil.StartEmptyCatalogServer(t)

	// --- node-agent ---
	nodeName := "test-node"
	mountBase := t.TempDir()

	agent := nodeagent.New(
		nodeagent.Config{
			NodeName:       nodeName,
			MountBase:      mountBase,
			CatalogDir:     kvSrv.CatalogDir(),
			ReportInterval: time.Hour,
		},
		volume.NewFSManager(volumeBase),
		mount.NewFSMounter(),
		assignment.NewHTTPSource("http://"+cp.Addr(), nodeName),
		assignment.NewHTTPStateReporter("http://"+cp.Addr(), nodeName),
		version.NewHTTPChecker(fmt.Sprintf("http://%s", kvSrv.HTTPAddr)),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create the K8s Node object so that syncBroker can read its labels
	// when evaluating DatasetBindings.
	if err := cp.Client.Create(context.Background(), &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName},
	}); err != nil {
		t.Fatalf("create Node %q: %v", nodeName, err)
	}
	cp.Broker.RegisterNode(nodeName)

	agentDone := make(chan error, 1)
	go func() { agentDone <- agent.Run(ctx) }()

	// --- v1: auto-created by DatasetReconciler.ensureVersion ---
	cp.WaitForVersionState(t, "users", "v1", string(api.StateActive), 30*time.Second)

	// Wait for the node-agent to report PhaseActive on v1.
	waitForNodePhase(t, cp, nodeName, "users", "v1", api.PhaseActive, 15*time.Second)

	// Verify memcache.
	assertMcGet(t, kvSrv.TCPAddr, "users:user-1", "Alice")
	assertMcGet(t, kvSrv.TCPAddr, "users:user-2", "Bob")

	// --- v2: upgrade ---
	b.Data["users"] = [][2][]byte{
		{[]byte("user-1"), []byte("Alice-v2")},
		{[]byte("user-2"), []byte("Bob-v2")},
		{[]byte("user-3"), []byte("Charlie")},
	}

	cp.CreateVersion(t, "users", "v2", 4)
	cp.WaitForVersionState(t, "users", "v2", string(api.StateActive), 30*time.Second)
	waitForNodePhase(t, cp, nodeName, "users", "v2", api.PhaseActive, 15*time.Second)

	assertMcGet(t, kvSrv.TCPAddr, "users:user-1", "Alice-v2")
	assertMcGet(t, kvSrv.TCPAddr, "users:user-2", "Bob-v2")
	assertMcGet(t, kvSrv.TCPAddr, "users:user-3", "Charlie")

	// --- shutdown ---
	cancel()
	<-agentDone
}

// --- helpers ---

func waitForNodePhase(t *testing.T, cp *testutil.ControlPlane, nodeName, dataset, versionID string, want api.DatasetPhase, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if state, ok := cp.Broker.GetNodeState(nodeName); ok {
			for _, ds := range state.Datasets {
				if ds.Dataset == dataset && ds.VersionID == versionID && ds.Phase == want {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("node %q dataset %q version %q did not reach phase %q within %v", nodeName, dataset, versionID, want, timeout)
}

func assertMcGet(t *testing.T, addr, key, want string) {
	t.Helper()
	got := mcGet(t, addr, key)
	if got != want {
		t.Errorf("mg %s = %q, want %q", key, got, want)
	}
}

func mcGet(t *testing.T, addr, key string) string {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))

	fmt.Fprintf(conn, "mg %s v\r\n", key)

	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("mg %s: read status: %v", key, err)
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "VA ") {
		t.Fatalf("mg %s: expected VA, got %q", key, line)
	}
	fields := strings.Fields(line)
	if len(fields) < 2 {
		t.Fatalf("mg %s: malformed VA: %q", key, line)
	}
	vlen, err := strconv.Atoi(fields[1])
	if err != nil {
		t.Fatalf("mg %s: bad length: %v", key, err)
	}
	buf := make([]byte, vlen+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("mg %s: read body: %v", key, err)
	}
	return string(buf[:vlen])
}
