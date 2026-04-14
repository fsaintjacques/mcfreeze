package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fsaintjacques/mcfreeze/go/api"
	"github.com/fsaintjacques/mcfreeze/go/internal/nodeagent/ui"
)

// staticState implements ui.StateProvider with static data for standalone UI
// testing without a running node agent or Kubernetes cluster.
type staticState struct {
	state       api.NodeState
	assignments []api.NodeAssignment
}

func (s *staticState) NodeState() api.NodeState          { return s.state }
func (s *staticState) Assignments() []api.NodeAssignment { return s.assignments }

func runUI(args []string) {
	fs := flag.NewFlagSet("ui", flag.ExitOnError)

	addr := fs.String("addr", ":8090", "address for the web UI")
	kvMemcacheAddr := fs.String("kv-memcache-addr", "localhost:11211", "kv-server memcache protocol address")
	kvMetricsAddr := fs.String("kv-metrics-addr", "localhost:9090", "kv-server HTTP metrics address")
	catalogDir := fs.String("catalog-dir", "", "directory containing catalog.json (optional)")
	stateFile := fs.String("state", "", "path to a JSON file containing NodeState (optional)")
	assignmentsFile := fs.String("assignments", "", "path to a JSON file containing []NodeAssignment (optional)")
	nodeName := fs.String("node-name", "local", "node name to display")
	fs.Parse(args)

	state := &staticState{
		state: api.NodeState{
			Node:       *nodeName,
			ReportedAt: time.Now(),
		},
	}

	if *stateFile != "" {
		data, err := os.ReadFile(*stateFile)
		if err != nil {
			slog.Error("read state file", "path", *stateFile, "err", err)
			os.Exit(1)
		}
		if err := json.Unmarshal(data, &state.state); err != nil {
			slog.Error("parse state file", "err", err)
			os.Exit(1)
		}
	}

	if *assignmentsFile != "" {
		data, err := os.ReadFile(*assignmentsFile)
		if err != nil {
			slog.Error("read assignments file", "path", *assignmentsFile, "err", err)
			os.Exit(1)
		}
		if err := json.Unmarshal(data, &state.assignments); err != nil {
			slog.Error("parse assignments file", "err", err)
			os.Exit(1)
		}
	}

	cfg := ui.Config{
		KVMemcacheAddr:       *kvMemcacheAddr,
		KVMetricsAddr:        *kvMetricsAddr,
		CatalogDir:           *catalogDir,
		AgentNodeName:        *nodeName,
		AgentControlPlaneURL: "(standalone)",
		AgentStartTime:       time.Now(),
	}

	handler := ui.NewHandler(state, cfg)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	l, err := net.Listen("tcp", *addr)
	if err != nil {
		slog.Error("listen", "addr", *addr, "err", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "UI server listening on http://%s\n", l.Addr().String())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	srv := &http.Server{Handler: mux}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()

	if err := srv.Serve(l); err != http.ErrServerClosed {
		slog.Error("serve", "err", err)
	}
}
