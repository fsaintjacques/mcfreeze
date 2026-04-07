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

// StateReportCallback is invoked from handlePostState after the broker has
// recorded a node's state. Used by the NodeAssignmentReconciler to wake on
// state changes without polling.
type StateReportCallback func(ctx context.Context, node string)

// Server is the control-plane HTTP server. It serves the node-agent API
// (assignments long-poll, state reporting) and the build trigger. Per-node
// state lives in the AssignmentBroker; the Server holds no other state.
type Server struct {
	broker   *AssignmentBroker
	builds   BuildStarter
	onState  StateReportCallback
	mux      *http.ServeMux
	listener net.Listener
}

// SetBuildStarter wires the build trigger. Optional: callers that don't need
// to expose POST /api/v1/dataset/{name}/build can omit it.
func (s *Server) SetBuildStarter(bs BuildStarter) {
	s.builds = bs
}

// SetStateReportCallback wires a callback fired after every successful
// node-state report. Optional.
func (s *Server) SetStateReportCallback(cb StateReportCallback) {
	s.onState = cb
}

// NewServer creates a Server bound to addr backed by the given broker.
// Call Serve() to start accepting.
func NewServer(broker *AssignmentBroker, addr string) (*Server, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &Server{broker: broker, mux: http.NewServeMux(), listener: l}
	s.mux.HandleFunc("GET /api/v1/node/{node}/assignments", s.handleGetAssignments)
	s.mux.HandleFunc("POST /api/v1/node/{node}/state", s.handlePostState)
	s.mux.HandleFunc("POST /api/v1/dataset/{name}/build", s.handleBuild)
	s.mux.HandleFunc("POST /admin/node/{node}/assignments", s.handleAdminSetAssignments)
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
	s.broker.RegisterNode(node) // auto-register on first poll

	var generation int64
	if g := r.URL.Query().Get("generation"); g != "" {
		var err error
		generation, err = strconv.ParseInt(g, 10, 64)
		if err != nil {
			http.Error(w, "invalid generation parameter", http.StatusBadRequest)
			return
		}
	}

	resp, ch := s.broker.GetAssignments(node, generation)

	if ch != nil {
		// Block until assignments change or client disconnects.
		select {
		case <-ch:
			resp, _ = s.broker.GetAssignments(node, generation)
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

	s.broker.ReportState(node, state)
	if s.onState != nil {
		s.onState(r.Context(), node)
	}
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

	s.broker.SetAssignments(node, assignments)
	w.WriteHeader(http.StatusOK)
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
