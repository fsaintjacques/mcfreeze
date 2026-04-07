package controlplane

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strconv"

	"github.com/fsaintjacques/frostmap/go/api"
)

const maxBodySize = 1 << 20 // 1 MiB

// BuildStarter can kick off a new dataset build.
type BuildStarter interface {
	StartBuild(ctx context.Context, spec api.DatasetSpec, versionID string) error
}

// Server is the control-plane HTTP server. It serves the node-agent API
// (assignments long-poll, state reporting) and an admin API for tests.
type Server struct {
	store    Store
	builds   BuildStarter
	mux      *http.ServeMux
	listener net.Listener
}

// SetBuildStarter wires the build trigger. Called after the Orchestrator is
// created to break the circular dependency (Server is created before
// Orchestrator, but Orchestrator holds Server).
func (s *Server) SetBuildStarter(bs BuildStarter) {
	s.builds = bs
}

// NewServer creates a Server bound to addr. Call Serve() to start accepting.
func NewServer(store Store, addr string) (*Server, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &Server{store: store, mux: http.NewServeMux(), listener: l}
	s.mux.HandleFunc("GET /api/v1/node/{node}/assignments", s.handleGetAssignments)
	s.mux.HandleFunc("POST /api/v1/node/{node}/state", s.handlePostState)
	s.mux.HandleFunc("POST /api/v1/dataset/{name}/build", s.handleBuild)
	s.mux.HandleFunc("POST /admin/node/{node}/assignments", s.handleAdminSetAssignments)
	s.mux.HandleFunc("GET /admin/dataset/{name}/rollout", s.handleAdminRollout)
	s.mux.HandleFunc("GET /admin/dataset/{name}/retired", s.handleAdminRetired)
	return s, nil
}

// Addr returns the listener address (useful when binding to :0).
func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

// Serve starts accepting connections. Blocks until the listener is closed.
func (s *Server) Serve() error {
	return http.Serve(s.listener, s.mux)
}

// Close stops the server.
func (s *Server) Close() error {
	return s.listener.Close()
}

// --- node-agent API ---

func (s *Server) handleGetAssignments(w http.ResponseWriter, r *http.Request) {
	node := r.PathValue("node")
	s.store.RegisterNode(node) // auto-register on first poll

	var generation int64
	if g := r.URL.Query().Get("generation"); g != "" {
		var err error
		generation, err = strconv.ParseInt(g, 10, 64)
		if err != nil {
			http.Error(w, "invalid generation parameter", http.StatusBadRequest)
			return
		}
	}

	resp, ch := s.store.GetAssignments(node, generation)

	if ch != nil {
		// Block until assignments change or client disconnects.
		select {
		case <-ch:
			resp, _ = s.store.GetAssignments(node, generation)
		case <-r.Context().Done():
			http.Error(w, "client disconnected", http.StatusRequestTimeout)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handlePostState(w http.ResponseWriter, r *http.Request) {
	node := r.PathValue("node")

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodySize))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var state api.NodeState
	if err := json.Unmarshal(body, &state); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.store.ReportState(node, state)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleBuild(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	if s.builds == nil {
		http.Error(w, "build trigger not configured", http.StatusServiceUnavailable)
		return
	}

	if !isValidName(name) {
		http.Error(w, "invalid dataset name", http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodySize))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req struct {
		Spec      api.DatasetSpec `json:"spec"`
		VersionID string          `json:"version_id"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if !isValidName(req.VersionID) {
		http.Error(w, "invalid version_id", http.StatusBadRequest)
		return
	}

	req.Spec.Name = name

	if err := s.builds.StartBuild(r.Context(), req.Spec, req.VersionID); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"version_id": req.VersionID})
}

// --- admin API (for tests) ---

func (s *Server) handleAdminSetAssignments(w http.ResponseWriter, r *http.Request) {
	node := r.PathValue("node")

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodySize))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var assignments []api.NodeAssignment
	if err := json.Unmarshal(body, &assignments); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.store.SetAssignments(node, assignments)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleAdminRollout(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	status := s.store.RolloutStatus(name)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (s *Server) handleAdminRetired(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	eligible := s.store.CheckRetirement(name)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(eligible)
}

// isValidName returns true if s is non-empty and contains only lowercase
// alphanumeric characters, hyphens, underscores, and dots.
func isValidName(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.') {
			return false
		}
	}
	return true
}
