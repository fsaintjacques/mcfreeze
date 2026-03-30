package nodeagent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"frostmap.io/fmtctl/api"
	"frostmap.io/fmtctl/internal/mount"
	"frostmap.io/fmtctl/internal/volume"
)

// helpers

func newTestAgent(t *testing.T) (*Agent, *volume.FakeVolumeManager, *mount.FakeMounter, *FakeAssignmentSource, *FakeStateReporter, *FakeVersionChecker) {
	t.Helper()

	mountBase := t.TempDir()
	catalogDir := t.TempDir()

	disks := volume.NewFakeVolumeManager()
	mounter := mount.NewFakeMounter()
	assignments := NewFakeAssignmentSource()
	reporter := &FakeStateReporter{}
	versions := &FakeVersionChecker{}

	cfg := Config{
		NodeName:       "test-node",
		MountBase:      mountBase,
		CatalogDir:     catalogDir,
		ReportInterval: time.Hour, // don't fire during tests
	}

	agent := New(cfg, disks, mounter, assignments, reporter, versions)
	return agent, disks, mounter, assignments, reporter, versions
}

func makeAssignment(dataset, keyPrefix, versionID, pvName string) api.NodeAssignment {
	return api.NodeAssignment{
		Dataset:   dataset,
		KeyPrefix: keyPrefix,
		Version: api.VersionRecord{
			ID:     versionID,
			PVName: pvName,
		},
	}
}

// --- reconcile tests ---

func TestReconcile_HappyPath(t *testing.T) {
	agent, _, mounter, _, _, versions := newTestAgent(t)

	assign := makeAssignment("ds", "ds", "v1", "pv-ds-v1")
	agent.reconcile(context.Background(), assign)

	state := agent.datasetState("ds")
	if state.Phase != api.PhaseActive {
		t.Fatalf("expected PhaseActive, got %s", state.Phase)
	}
	if state.VersionID != "v1" {
		t.Fatalf("expected version v1, got %s", state.VersionID)
	}

	// Mounter should have been called.
	if len(mounter.Calls) != 1 || mounter.Calls[0].Op != "mount" {
		t.Fatalf("expected 1 mount call, got %v", mounter.Calls)
	}

	// Version checker should have been called.
	if len(versions.Calls) != 1 {
		t.Fatalf("expected 1 version check, got %d", len(versions.Calls))
	}
	if versions.Calls[0].Dataset != "ds" || versions.Calls[0].VersionID != "v1" {
		t.Fatalf("version check args: %+v", versions.Calls[0])
	}
}

func TestReconcile_AlreadyActive(t *testing.T) {
	agent, _, mounter, _, _, _ := newTestAgent(t)

	assign := makeAssignment("ds", "ds", "v1", "pv-ds-v1")
	agent.reconcile(context.Background(), assign)

	// Reconcile again with the same assignment — should be a no-op.
	agent.reconcile(context.Background(), assign)

	// Only one mount call.
	if len(mounter.Calls) != 1 {
		t.Fatalf("expected 1 mount call (idempotent), got %d", len(mounter.Calls))
	}
}

func TestReconcile_NoPVName(t *testing.T) {
	agent, _, _, _, _, _ := newTestAgent(t)

	assign := makeAssignment("ds", "ds", "v1", "") // no PV
	agent.reconcile(context.Background(), assign)

	state := agent.datasetState("ds")
	if state.Phase != api.PhaseError {
		t.Fatalf("expected PhaseError, got %s", state.Phase)
	}
}

func TestReconcile_AttachError(t *testing.T) {
	agent, disks, _, _, _, _ := newTestAgent(t)

	disks.InjectError(0, os.ErrPermission)

	assign := makeAssignment("ds", "ds", "v1", "pv-ds-v1")
	agent.reconcile(context.Background(), assign)

	state := agent.datasetState("ds")
	if state.Phase != api.PhaseError {
		t.Fatalf("expected PhaseError, got %s", state.Phase)
	}
}

func TestReconcile_MountError(t *testing.T) {
	agent, _, mounter, _, _, _ := newTestAgent(t)

	mounter.InjectError(0, os.ErrPermission)

	assign := makeAssignment("ds", "ds", "v1", "pv-ds-v1")
	agent.reconcile(context.Background(), assign)

	state := agent.datasetState("ds")
	if state.Phase != api.PhaseError {
		t.Fatalf("expected PhaseError, got %s", state.Phase)
	}
}

func TestReconcile_VersionCheckError(t *testing.T) {
	agent, _, _, _, _, versions := newTestAgent(t)

	versions.InjectError(os.ErrDeadlineExceeded)

	assign := makeAssignment("ds", "ds", "v1", "pv-ds-v1")
	agent.reconcile(context.Background(), assign)

	state := agent.datasetState("ds")
	if state.Phase != api.PhaseError {
		t.Fatalf("expected PhaseError, got %s", state.Phase)
	}
}

// --- writeCatalog tests ---

func TestWriteCatalog_SingleDataset(t *testing.T) {
	agent, _, _, _, _, _ := newTestAgent(t)

	if err := agent.writeCatalog("ds", "v1", "ds", "/mnt/kv/ds/v1"); err != nil {
		t.Fatal(err)
	}

	cat := readCatalog(t, agent.cfg.CatalogDir)
	if len(cat.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(cat.Entries))
	}
	e := cat.Entries[0]
	if e.Dataset != "ds" || e.VersionID != "v1" || e.KeyPrefix != "ds" || e.MountPath != "/mnt/kv/ds/v1" {
		t.Fatalf("unexpected entry: %+v", e)
	}
}

func TestWriteCatalog_MultiDataset(t *testing.T) {
	agent, _, _, _, _, _ := newTestAgent(t)

	// Simulate ds1 already active.
	agent.mu.Lock()
	agent.datasets["ds1"] = api.DatasetState{
		Dataset:   "ds1",
		VersionID: "v1",
		Phase:     api.PhaseActive,
		MountPath: "/mnt/kv/ds1/v1",
	}
	agent.mu.Unlock()

	// Write catalog for ds2 being promoted.
	if err := agent.writeCatalog("ds2", "v2", "ds2", "/mnt/kv/ds2/v2"); err != nil {
		t.Fatal(err)
	}

	cat := readCatalog(t, agent.cfg.CatalogDir)
	if len(cat.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(cat.Entries))
	}

	byDataset := map[string]api.CatalogEntry{}
	for _, e := range cat.Entries {
		byDataset[e.Dataset] = e
	}
	if _, ok := byDataset["ds1"]; !ok {
		t.Fatal("ds1 missing from catalog")
	}
	if _, ok := byDataset["ds2"]; !ok {
		t.Fatal("ds2 missing from catalog")
	}
}

// --- run loop tests ---

func TestRun_SingleAssignment(t *testing.T) {
	agent, _, _, assignments, reporter, _ := newTestAgent(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Push one assignment, then cancel.
	assignments.Responses <- &api.AssignmentsResponse{
		Generation: 1,
		Assignments: []api.NodeAssignment{
			makeAssignment("ds", "ds", "v1", "pv-ds-v1"),
		},
	}

	go func() {
		// Give the agent time to process, then stop.
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	_ = agent.Run(ctx) // returns context.Canceled

	state := agent.datasetState("ds")
	if state.Phase != api.PhaseActive {
		t.Fatalf("expected PhaseActive, got %s", state.Phase)
	}

	// Reporter should have been called at least once.
	last, ok := reporter.LastState()
	if !ok {
		t.Fatal("reporter was never called")
	}
	if last.Node != "test-node" {
		t.Fatalf("reported node = %q, want %q", last.Node, "test-node")
	}
}

// --- helpers ---

func readCatalog(t *testing.T, catalogDir string) api.CatalogFile {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(catalogDir, "catalog.json"))
	if err != nil {
		t.Fatalf("read catalog.json: %v", err)
	}
	var cat api.CatalogFile
	if err := json.Unmarshal(data, &cat); err != nil {
		t.Fatalf("parse catalog.json: %v\n%s", err, data)
	}
	return cat
}
