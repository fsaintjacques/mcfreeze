package testutil

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"frostmap.io/fmtctl/api"
)

// FMServerBinary returns the path to the frostmap-server binary.
func FMServerBinary(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("FM_SERVER"); p != "" {
		return p
	}
	return "frostmap-server"
}

// Server is a running frostmap-server in catalog mode.
type Server struct {
	// TCPAddr is the memcache protocol address.
	TCPAddr string
	// HTTPAddr is the metrics + /version HTTP address.
	HTTPAddr string

	catalogPath string
	cmd         *exec.Cmd
}

// StartCatalogServer starts frostmap-server in catalog mode with an initial
// catalog. It picks free ports for TCP (memcache) and HTTP (/metrics, /version).
//
// The server process is killed when the test finishes.
func StartCatalogServer(t *testing.T, entries []api.CatalogEntry) *Server {
	t.Helper()

	dir := t.TempDir()
	catalogPath := filepath.Join(dir, "catalog.json")

	// Write the initial catalog.
	writeCatalogFile(t, catalogPath, entries)

	tcpPort := freePort(t)
	httpPort := freePort(t)
	tcpAddr := fmt.Sprintf("127.0.0.1:%d", tcpPort)
	httpAddr := fmt.Sprintf("127.0.0.1:%d", httpPort)

	bin := FMServerBinary(t)
	// frostmap-server is invoked as `fm serve catalog ...`
	cmd := exec.Command(FMBinary(t), "serve", "catalog",
		"--catalog", catalogPath,
		"--tcp", tcpAddr,
		"--metrics", httpAddr,
	)
	_ = bin // FM_SERVER not used directly; fm binary has the serve subcommand
	cmd.Stdout = os.Stderr // show server logs in test output
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start frostmap-server: %v", err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})

	// Wait for TCP listener to be ready.
	waitForTCP(t, tcpAddr, 5*time.Second)

	return &Server{
		TCPAddr:     tcpAddr,
		HTTPAddr:    httpAddr,
		catalogPath: catalogPath,
		cmd:         cmd,
	}
}

// WriteCatalog atomically replaces the catalog.json with new entries.
// This triggers a hot-swap in the server.
func (s *Server) WriteCatalog(t *testing.T, entries []api.CatalogEntry) {
	t.Helper()
	writeCatalogFile(t, s.catalogPath, entries)
}

// CatalogFile is the top-level catalog document matching the Rust CatalogFile struct.
type CatalogFile struct {
	Entries []api.CatalogEntry `json:"entries"`
}

func writeCatalogFile(t *testing.T, path string, entries []api.CatalogEntry) {
	t.Helper()

	doc := CatalogFile{Entries: entries}
	data, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("failed to marshal catalog: %v", err)
	}

	// Atomic write: write to temp file, then rename.
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		t.Fatalf("failed to write temp catalog: %v", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("failed to rename catalog: %v", err)
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func waitForTCP(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server at %s did not become ready within %v", addr, timeout)
}
