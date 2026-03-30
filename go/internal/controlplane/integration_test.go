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

	"frostmap.io/fmtctl/api"
	"frostmap.io/fmtctl/internal/controlplane"
	"frostmap.io/fmtctl/internal/mount"
	"frostmap.io/fmtctl/internal/nodeagent"
	"frostmap.io/fmtctl/internal/testutil"
	"frostmap.io/fmtctl/internal/volume"
)

// TestFullLoop exercises the entire pipeline:
//
//	control-plane builds snapshot → assigns to node →
//	node-agent reconciles (attach, mount, catalog, version confirm) →
//	KV server serves data via memcache →
//	control-plane receives PhaseActive state report →
//	upgrade to v2 → re-verify
func TestFullLoop(t *testing.T) {
	volumeBase := t.TempDir()
	buildBase := t.TempDir()

	// --- control-plane ---
	builder := &controlplane.FakeVersionBuilder{
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

	orch, err := controlplane.NewOrchestrator(builder, volumeBase)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { orch.Close() })

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
		volume.NewFSVolumeManager(volumeBase),
		mount.NewFSMounter(),
		nodeagent.NewHTTPAssignmentSource(orch.Addr(), nodeName),
		nodeagent.NewHTTPStateReporter(orch.Addr(), nodeName),
		nodeagent.NewHTTPVersionChecker(fmt.Sprintf("http://%s", kvSrv.HTTPAddr)),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	orch.RegisterNode(nodeName)

	agentDone := make(chan error, 1)
	go func() { agentDone <- agent.Run(ctx) }()

	// --- v1: build and promote ---
	spec := api.DatasetSpec{Name: "users", KeyPrefix: "users"}
	if err := orch.BuildAndPromote(ctx, spec, "v1"); err != nil {
		t.Fatalf("BuildAndPromote v1: %v", err)
	}

	// Wait for control-plane to receive PhaseActive.
	waitForNodePhase(t, orch.Store, nodeName, "users", "v1", api.PhaseActive, 15*time.Second)

	// Verify memcache.
	assertMcGet(t, kvSrv.TCPAddr, "users:user-1", "Alice")
	assertMcGet(t, kvSrv.TCPAddr, "users:user-2", "Bob")

	// --- v2: upgrade ---
	builder.Data["users"] = [][2][]byte{
		{[]byte("user-1"), []byte("Alice-v2")},
		{[]byte("user-2"), []byte("Bob-v2")},
		{[]byte("user-3"), []byte("Charlie")},
	}

	if err := orch.BuildAndPromote(ctx, spec, "v2"); err != nil {
		t.Fatalf("BuildAndPromote v2: %v", err)
	}

	waitForNodePhase(t, orch.Store, nodeName, "users", "v2", api.PhaseActive, 15*time.Second)

	assertMcGet(t, kvSrv.TCPAddr, "users:user-1", "Alice-v2")
	assertMcGet(t, kvSrv.TCPAddr, "users:user-2", "Bob-v2")
	assertMcGet(t, kvSrv.TCPAddr, "users:user-3", "Charlie")

	// --- shutdown ---
	cancel()
	<-agentDone
}

// --- helpers ---

func waitForNodePhase(t *testing.T, store *controlplane.Store, nodeName, dataset, versionID string, want api.DatasetPhase, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if state, ok := store.GetNodeState(nodeName); ok {
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
