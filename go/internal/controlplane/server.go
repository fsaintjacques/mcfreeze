package controlplane

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"

	"frostmap.io/fmtctl/api"
)

// Server is the control-plane HTTP server. It serves the node-agent API
// (assignments long-poll, state reporting) and an admin API for tests.
type Server struct {
	store    *Store
	mux      *http.ServeMux
	listener net.Listener
}

// NewServer creates a Server bound to addr. Call Serve() to start accepting.
func NewServer(store *Store, addr string) (*Server, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &Server{store: store, mux: http.NewServeMux(), listener: l}
	s.mux.HandleFunc("GET /api/v1/node/{node}/assignments", s.handleGetAssignments)
	s.mux.HandleFunc("POST /api/v1/node/{node}/state", s.handlePostState)
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

	var generation int64
	if g := r.URL.Query().Get("generation"); g != "" {
		fmt.Sscanf(g, "%d", &generation)
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

	body, err := io.ReadAll(r.Body)
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

// --- admin API (for tests) ---

func (s *Server) handleAdminSetAssignments(w http.ResponseWriter, r *http.Request) {
	node := r.PathValue("node")

	body, err := io.ReadAll(r.Body)
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
