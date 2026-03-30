package controlplane

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"frostmap.io/fmtctl/api"
)

func startTestServer(t *testing.T) (*Server, *Store) {
	t.Helper()
	store := NewStore()
	srv, err := NewServer(store, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv, store
}

func TestServer_PostState(t *testing.T) {
	srv, store := startTestServer(t)

	state := api.NodeState{
		Node:       "node-1",
		Datasets:   []api.DatasetState{{Dataset: "ds", VersionID: "v1", Phase: api.PhaseActive}},
		ReportedAt: time.Now(),
	}
	body, _ := json.Marshal(state)

	resp, err := http.Post("http://"+srv.Addr()+"/api/v1/node/node-1/state", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	got, ok := store.GetNodeState("node-1")
	if !ok || got.Node != "node-1" {
		t.Fatalf("state not stored: %+v", got)
	}
}

func TestServer_AdminSetAndGetAssignments(t *testing.T) {
	srv, _ := startTestServer(t)

	assignments := []api.NodeAssignment{{
		Dataset:   "ds",
		KeyPrefix: "ds",
		Version:   api.VersionRecord{ID: "v1", PVName: "pv-1"},
	}}
	body, _ := json.Marshal(assignments)

	// Set via admin API.
	resp, err := http.Post("http://"+srv.Addr()+"/admin/node/node-1/assignments", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("admin set status = %d", resp.StatusCode)
	}

	// Get via node-agent API (generation=0 so it returns immediately).
	resp, err = http.Get("http://" + srv.Addr() + "/api/v1/node/node-1/assignments?generation=0")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var result api.AssignmentsResponse
	json.NewDecoder(resp.Body).Decode(&result)
	if result.Generation != 1 {
		t.Fatalf("generation = %d, want 1", result.Generation)
	}
	if len(result.Assignments) != 1 || result.Assignments[0].Dataset != "ds" {
		t.Fatalf("assignments = %+v", result.Assignments)
	}
}

func TestServer_LongPoll(t *testing.T) {
	srv, store := startTestServer(t)

	// Set initial assignment.
	store.SetAssignments("node-1", nil)

	// Start long-poll in background (generation=1 matches current, so it blocks).
	done := make(chan api.AssignmentsResponse, 1)
	go func() {
		resp, err := http.Get("http://" + srv.Addr() + "/api/v1/node/node-1/assignments?generation=1")
		if err != nil {
			return
		}
		defer resp.Body.Close()
		var result api.AssignmentsResponse
		json.NewDecoder(resp.Body).Decode(&result)
		done <- result
	}()

	// Should still be blocked.
	select {
	case <-done:
		t.Fatal("long-poll returned before assignment change")
	case <-time.After(100 * time.Millisecond):
	}

	// Update assignments — should wake the long-poll.
	store.SetAssignments("node-1", []api.NodeAssignment{{
		Dataset: "ds", KeyPrefix: "ds",
		Version: api.VersionRecord{ID: "v1", PVName: "pv-1"},
	}})

	select {
	case result := <-done:
		if result.Generation != 2 {
			t.Fatalf("generation = %d, want 2", result.Generation)
		}
		if len(result.Assignments) != 1 {
			t.Fatalf("assignments = %+v", result.Assignments)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("long-poll did not return after assignment change")
	}
}
