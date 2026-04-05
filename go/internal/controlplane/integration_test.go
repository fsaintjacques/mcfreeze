//go:build integration

package controlplane_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"frostmap.io/fmtctl/api"
	"frostmap.io/fmtctl/internal/controlplane"
	"frostmap.io/fmtctl/internal/controlplane/builder"
	"frostmap.io/fmtctl/internal/nodeagent"
	"frostmap.io/fmtctl/internal/nodeagent/assignment"
	"frostmap.io/fmtctl/internal/nodeagent/mount"
	"frostmap.io/fmtctl/internal/nodeagent/version"
	"frostmap.io/fmtctl/internal/nodeagent/volume"
	"frostmap.io/fmtctl/internal/testutil"
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
	builder := &builder.Fake{
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
		volume.NewFSManager(volumeBase),
		mount.NewFSMounter(),
		assignment.NewHTTPSource(orch.Addr(), nodeName),
		assignment.NewHTTPStateReporter(orch.Addr(), nodeName),
		version.NewHTTPChecker(fmt.Sprintf("http://%s", kvSrv.HTTPAddr)),
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

// TestMultiNodeConvergenceAndRetirement runs two node-agents against the same
// control-plane, verifies both converge, then upgrades and checks that the
// old version becomes eligible for retirement.
func TestMultiNodeConvergenceAndRetirement(t *testing.T) {
	buildBase := t.TempDir()

	builder := &builder.Fake{
		FMBinary:   testutil.FMBinary(t),
		Partitions: 4,
		OutputBase: buildBase,
		Data: map[string][][2][]byte{
			"ds": {
				{[]byte("k"), []byte("val-v1")},
			},
		},
	}

	// Shared volume base — each node gets its own subdirectory.
	vol1 := t.TempDir()
	vol2 := t.TempDir()

	orch, err := controlplane.NewOrchestrator(builder, vol1)
	if err != nil {
		t.Fatal(err)
	}
	// Override VolumeBase for node-2 symlinking later.
	t.Cleanup(func() { orch.Close() })

	orch.RegisterNode("node-1")
	orch.RegisterNode("node-2")

	// Each node gets its own KV server.
	kv1 := testutil.StartEmptyCatalogServer(t)
	kv2 := testutil.StartEmptyCatalogServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startAgent := func(name, volBase, mountBase, catalogDir, kvHTTP string) chan error {
		agent := nodeagent.New(
			nodeagent.Config{
				NodeName:       name,
				MountBase:      mountBase,
				CatalogDir:     catalogDir,
				ReportInterval: time.Hour,
			},
			volume.NewFSManager(volBase),
			mount.NewFSMounter(),
			assignment.NewHTTPSource(orch.Addr(), name),
			assignment.NewHTTPStateReporter(orch.Addr(), name),
			version.NewHTTPChecker(fmt.Sprintf("http://%s", kvHTTP)),
		)
		done := make(chan error, 1)
		go func() { done <- agent.Run(ctx) }()
		return done
	}

	mount1 := t.TempDir()
	mount2 := t.TempDir()
	done1 := startAgent("node-1", vol1, mount1, kv1.CatalogDir(), kv1.HTTPAddr)
	done2 := startAgent("node-2", vol2, mount2, kv2.CatalogDir(), kv2.HTTPAddr)

	// --- v1: build and promote ---
	spec := api.DatasetSpec{Name: "ds", KeyPrefix: "ds"}
	if err := orch.BuildAndPromote(ctx, spec, "v1"); err != nil {
		t.Fatalf("BuildAndPromote v1: %v", err)
	}

	// Symlink the snapshot into vol2 as well (Promote pushed to both nodes,
	// but only vol1 got the symlink from BuildAndPromote).
	symlinkPV(t, buildBase+"/ds/v1", vol2, "pv-ds-v1")

	// Wait for both nodes to converge.
	if err := orch.WaitForConvergence(ctx, "ds", "v1"); err != nil {
		t.Fatalf("WaitForConvergence v1: %v", err)
	}

	status := orch.Store.RolloutStatus("ds")
	if len(status.ConvergedNodes) != 2 {
		t.Fatalf("converged = %d, want 2: pending=%v error=%v", len(status.ConvergedNodes), status.PendingNodes, status.ErrorNodes)
	}

	// Verify both KV servers serve the data.
	assertMcGet(t, kv1.TCPAddr, "ds:k", "val-v1")
	assertMcGet(t, kv2.TCPAddr, "ds:k", "val-v1")

	// --- v2: upgrade ---
	builder.Data["ds"] = [][2][]byte{
		{[]byte("k"), []byte("val-v2")},
	}
	if err := orch.BuildAndPromote(ctx, spec, "v2"); err != nil {
		t.Fatalf("BuildAndPromote v2: %v", err)
	}
	symlinkPV(t, buildBase+"/ds/v2", vol2, "pv-ds-v2")

	if err := orch.WaitForConvergence(ctx, "ds", "v2"); err != nil {
		t.Fatalf("WaitForConvergence v2: %v", err)
	}

	assertMcGet(t, kv1.TCPAddr, "ds:k", "val-v2")
	assertMcGet(t, kv2.TCPAddr, "ds:k", "val-v2")

	// v1 should be eligible for retirement (both nodes on v2).
	eligible := orch.Store.CheckRetirement("ds")
	if len(eligible) != 1 || eligible[0].ID != "v1" {
		t.Fatalf("expected v1 eligible for retirement, got %+v", eligible)
	}

	if err := orch.Store.DeleteVersion("ds", "v1"); err != nil {
		t.Fatalf("DeleteVersion v1: %v", err)
	}

	cancel()
	<-done1
	<-done2
}

func symlinkPV(t *testing.T, snapPath, volBase, pvName string) {
	t.Helper()
	link := filepath.Join(volBase, pvName)
	if err := os.Symlink(snapPath, link); err != nil && !os.IsExist(err) {
		t.Fatalf("symlink PV %s: %v", pvName, err)
	}
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
