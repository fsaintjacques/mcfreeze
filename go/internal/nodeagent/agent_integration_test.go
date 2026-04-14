//go:build integration

package nodeagent_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/fsaintjacques/mcfreeze/go/api"
	"github.com/fsaintjacques/mcfreeze/go/internal/nodeagent"
	"github.com/fsaintjacques/mcfreeze/go/internal/nodeagent/assignment"
	"github.com/fsaintjacques/mcfreeze/go/internal/nodeagent/mount"
	"github.com/fsaintjacques/mcfreeze/go/internal/nodeagent/version"
	"github.com/fsaintjacques/mcfreeze/go/internal/nodeagent/volume"
	"github.com/fsaintjacques/mcfreeze/go/internal/testutil"
)

// TestAgentReconcileEndToEnd wires the node-agent with:
//   - FakeAssignmentSource (we control assignments)
//   - FSVolumeManager (filesystem-simulated disk attach)
//   - FSMounter (symlink-based mount)
//   - FakeStateReporter (records reported states)
//   - Real KV server (catalog mode, started empty)
//
// The test pushes an assignment, the agent reconciles it through the full
// pipeline, the real KV server loads the snapshot from catalog.json, and we
// verify end-to-end: state reports, GET /version, and memcache lookups.
func TestAgentReconcileEndToEnd(t *testing.T) {
	// Build a snapshot with known data.
	pairs := []testutil.KV{
		{Key: []byte("user-1"), Value: []byte("Alice")},
		{Key: []byte("user-2"), Value: []byte("Bob")},
	}
	snapDir := testutil.BuildSnapshot(t, pairs, 4)

	// Prepare the FS volume manager: pre-populate the PV directory with
	// the snapshot by symlinking to it.
	volumeBase := t.TempDir()
	pvName := "pv-users-v1"
	if err := os.Symlink(snapDir, filepath.Join(volumeBase, pvName)); err != nil {
		t.Fatalf("symlink snapshot into volume base: %v", err)
	}

	// Start the KV server with no initial catalog.
	srv := testutil.StartEmptyCatalogServer(t)

	// Wire the agent.
	assignments := assignment.NewFakeSource()
	reporter := &assignment.FakeStateReporter{}
	versionChecker := version.NewHTTPChecker(fmt.Sprintf("http://%s", srv.HTTPAddr))

	mountBase := t.TempDir()
	cfg := nodeagent.Config{
		NodeName:       "integration-node",
		MountBase:      mountBase,
		CatalogDir:     srv.CatalogDir(),
		ReportInterval: time.Hour, // only report on assignment changes
	}

	agent := nodeagent.New(
		cfg,
		volume.NewFSManager(volumeBase),
		mount.NewFSMounter(),
		assignments,
		reporter,
		versionChecker,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start the agent in a goroutine.
	agentDone := make(chan error, 1)
	go func() {
		agentDone <- agent.Run(ctx)
	}()

	// Push an assignment. The channel is buffered (cap 8), so this send
	// completes before the agent goroutine necessarily calls FetchAssignments.
	assignments.Responses <- &api.AssignmentsResponse{
		Generation: 1,
		Assignments: []api.NodeAssignment{{
			Dataset:   "users",
			KeyPrefix: "users",
			Version: api.VersionRecord{
				ID:     "v1",
				PVName: pvName,
			},
		}},
	}

	// Wait for the agent to report state with PhaseActive for v1.
	waitForPhase(t, reporter, "users", "v1", api.PhaseActive, 10*time.Second)

	// Verify GET /version reports the correct version.
	versionURL := fmt.Sprintf("http://%s/version", srv.HTTPAddr)
	resp, err := http.Get(versionURL)
	if err != nil {
		t.Fatalf("GET /version: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read /version body: %v", err)
	}
	var vr api.KVVersionResponse
	if err := json.Unmarshal(body, &vr); err != nil {
		t.Fatalf("decode /version: %v\n%s", err, body)
	}
	if len(vr.Datasets) != 1 || vr.Datasets[0].VersionID != "v1" {
		t.Fatalf("GET /version unexpected: %+v", vr)
	}

	// Verify memcache lookup returns the correct value.
	got := mcGet(t, srv.TCPAddr, "users:user-1")
	if got != "Alice" {
		t.Fatalf("mg users:user-1 = %q, want %q", got, "Alice")
	}
	got = mcGet(t, srv.TCPAddr, "users:user-2")
	if got != "Bob" {
		t.Fatalf("mg users:user-2 = %q, want %q", got, "Bob")
	}

	// Verify the reported state.
	last, _ := reporter.LastState()
	if last.Node != "integration-node" {
		t.Errorf("reported node = %q, want %q", last.Node, "integration-node")
	}

	// --- Upgrade to v2 with different data ---
	v2Pairs := []testutil.KV{
		{Key: []byte("user-1"), Value: []byte("Alice-v2")},
		{Key: []byte("user-2"), Value: []byte("Bob-v2")},
		{Key: []byte("user-3"), Value: []byte("Charlie")},
	}
	v2SnapDir := testutil.BuildSnapshot(t, v2Pairs, 4)
	v2PVName := "pv-users-v2"
	if err := os.Symlink(v2SnapDir, filepath.Join(volumeBase, v2PVName)); err != nil {
		t.Fatalf("symlink v2 snapshot: %v", err)
	}

	assignments.Responses <- &api.AssignmentsResponse{
		Generation: 2,
		Assignments: []api.NodeAssignment{{
			Dataset:   "users",
			KeyPrefix: "users",
			Version: api.VersionRecord{
				ID:     "v2",
				PVName: v2PVName,
			},
		}},
	}

	// Wait for v2 to become active.
	waitForPhase(t, reporter, "users", "v2", api.PhaseActive, 10*time.Second)

	// Verify new data is served.
	got = mcGet(t, srv.TCPAddr, "users:user-1")
	if got != "Alice-v2" {
		t.Fatalf("v2: mg users:user-1 = %q, want %q", got, "Alice-v2")
	}
	got = mcGet(t, srv.TCPAddr, "users:user-2")
	if got != "Bob-v2" {
		t.Fatalf("v2: mg users:user-2 = %q, want %q", got, "Bob-v2")
	}
	got = mcGet(t, srv.TCPAddr, "users:user-3")
	if got != "Charlie" {
		t.Fatalf("v2: mg users:user-3 = %q, want %q", got, "Charlie")
	}

	// Verify the agent reported v2 active.
	last, _ = reporter.LastState()
	found := false
	for _, ds := range last.Datasets {
		if ds.Dataset == "users" {
			found = true
			if ds.VersionID != "v2" {
				t.Errorf("reported version = %q, want v2", ds.VersionID)
			}
			break
		}
	}
	if !found {
		t.Error("dataset \"users\" not found in reported state")
	}

	// Stop the agent.
	cancel()
	if err := <-agentDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() = %v, want context.Canceled", err)
	}
}

func waitForPhase(t *testing.T, reporter *assignment.FakeStateReporter, dataset, versionID string, want api.DatasetPhase, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if last, ok := reporter.LastState(); ok {
			for _, ds := range last.Datasets {
				if ds.Dataset == dataset && ds.VersionID == versionID && ds.Phase == want {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("dataset %q version %q did not reach phase %q within %v", dataset, versionID, want, timeout)
}

// mcGet is a minimal memcache meta-get for integration tests.
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
