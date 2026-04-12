package ui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fsaintjacques/frostmap/go/api"
)

type fakeState struct {
	state       api.NodeState
	assignments []api.NodeAssignment
}

func (f *fakeState) NodeState() api.NodeState          { return f.state }
func (f *fakeState) Assignments() []api.NodeAssignment { return f.assignments }

func newTestHandler(state *fakeState) *Handler {
	cfg := Config{
		KVMemcacheAddr:       "localhost:11211",
		KVMetricsAddr:        "localhost:9090",
		CatalogDir:           "/tmp/nonexistent",
		AgentNodeName:        "test-node",
		AgentControlPlaneURL: "http://control-plane:8080",
		AgentStartTime:       time.Now(),
	}
	return NewHandler(state, cfg)
}

func TestHandleIndex(t *testing.T) {
	state := &fakeState{
		state: api.NodeState{
			Node: "test-node",
			Datasets: []api.DatasetState{
				{
					Dataset:   "users",
					KeyPrefix: "users",
					VersionID: "v42",
					Phase:     api.PhaseActive,
					PVName:    "pv-users-v42",
					MountPath: "/mnt/kv/users/v42",
					UpdatedAt: time.Now().Add(-5 * time.Minute),
				},
				{
					Dataset:   "products",
					KeyPrefix: "products",
					VersionID: "v7",
					Phase:     api.PhaseError,
					Error:     "mount failed: device not found",
					UpdatedAt: time.Now().Add(-30 * time.Second),
				},
			},
			ReportedAt: time.Now(),
		},
	}

	h := newTestHandler(state)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	body := w.Body.String()
	for _, want := range []string{"test-node", "users", "v42", "active", "products", "error", "mount failed"} {
		if !strings.Contains(body, want) {
			t.Errorf("expected body to contain %q", want)
		}
	}
}

func TestHandleAPIState(t *testing.T) {
	state := &fakeState{
		state: api.NodeState{
			Node: "test-node",
			Datasets: []api.DatasetState{
				{Dataset: "users", Phase: api.PhaseActive},
			},
		},
	}

	h := newTestHandler(state)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest("GET", "/api/state", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected application/json, got %s", ct)
	}

	var ns api.NodeState
	if err := json.NewDecoder(w.Body).Decode(&ns); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if ns.Node != "test-node" {
		t.Fatalf("expected test-node, got %s", ns.Node)
	}
	if len(ns.Datasets) != 1 || ns.Datasets[0].Dataset != "users" {
		t.Fatalf("unexpected datasets: %+v", ns.Datasets)
	}
}

func TestHandleQuery_NoKey(t *testing.T) {
	h := newTestHandler(&fakeState{})
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest("GET", "/query", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	body := w.Body.String()
	if strings.Contains(body, "HIT") || strings.Contains(body, "MISS") {
		t.Error("expected no result when no key is provided")
	}
}

func TestHandleIndex_NoDatasets(t *testing.T) {
	state := &fakeState{
		state: api.NodeState{Node: "empty-node"},
	}

	h := newTestHandler(state)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "No datasets") {
		t.Error("expected 'No datasets' message")
	}
}

func TestHandle404(t *testing.T) {
	h := newTestHandler(&fakeState{})
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest("GET", "/nonexistent", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}
