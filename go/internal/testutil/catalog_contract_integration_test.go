//go:build integration

package testutil

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/fsaintjacques/frostmap/go/api"
)

// TestCatalogContract verifies the full Go → Rust contract:
//
//  1. Go marshals api.CatalogEntry (same code path as node-agent)
//  2. frostmap-server reads catalog.json and loads the snapshot
//  3. GET /version returns api.KVVersionResponse with the correct version
//  4. memcache mg returns the correct value
func TestCatalogContract(t *testing.T) {
	// Build a snapshot with known data.
	pairs := []KV{
		{Key: []byte("user-1"), Value: []byte("Alice")},
		{Key: []byte("user-2"), Value: []byte("Bob")},
	}
	snapDir := BuildSnapshot(t, pairs, 4)

	// Start frostmap-server in catalog mode with one dataset.
	entries := []api.CatalogEntry{{
		Dataset:   "users",
		KeyPrefix: "users",
		VersionID: "v1",
		MountPath: snapDir,
	}}
	srv := StartCatalogServer(t, entries)

	// --- Verify GET /version ---
	resp := httpGetJSON[api.KVVersionResponse](t, fmt.Sprintf("http://%s/version", srv.HTTPAddr))

	if len(resp.Datasets) != 1 {
		t.Fatalf("GET /version: expected 1 dataset, got %d", len(resp.Datasets))
	}
	ds := resp.Datasets[0]
	if ds.Dataset != "users" {
		t.Errorf("GET /version: dataset = %q, want %q", ds.Dataset, "users")
	}
	if ds.VersionID != "v1" {
		t.Errorf("GET /version: version_id = %q, want %q", ds.VersionID, "v1")
	}
	if ds.LoadedAt.IsZero() {
		t.Error("GET /version: loaded_at is zero")
	}

	// --- Verify memcache mg ---
	got := mcGet(t, srv.TCPAddr, "users:user-1")
	if got != "Alice" {
		t.Errorf("mg users:user-1 = %q, want %q", got, "Alice")
	}
	got = mcGet(t, srv.TCPAddr, "users:user-2")
	if got != "Bob" {
		t.Errorf("mg users:user-2 = %q, want %q", got, "Bob")
	}

	// --- Verify miss ---
	gotMiss := mcGetRaw(t, srv.TCPAddr, "users:nonexistent")
	if !strings.HasPrefix(gotMiss, "EN") {
		t.Errorf("mg users:nonexistent: expected EN, got %q", gotMiss)
	}
}

// TestCatalogContractVersionEndpointShape verifies the /version JSON shape
// matches the Go api.KVVersionResponse type exactly.
func TestCatalogContractVersionEndpointShape(t *testing.T) {
	snapDir := BuildSnapshot(t, []KV{{Key: []byte("k"), Value: []byte("v")}}, 4)

	entries := []api.CatalogEntry{{
		Dataset:   "ds",
		KeyPrefix: "ds",
		VersionID: "v42",
		MountPath: snapDir,
	}}
	srv := StartCatalogServer(t, entries)

	// Fetch raw JSON and verify field names.
	body := httpGetBody(t, fmt.Sprintf("http://%s/version", srv.HTTPAddr))

	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		t.Fatalf("failed to parse /version JSON: %v", err)
	}
	if _, ok := raw["datasets"]; !ok {
		t.Fatal("/version JSON missing 'datasets' field")
	}

	var entries2 []map[string]json.RawMessage
	if err := json.Unmarshal(raw["datasets"], &entries2); err != nil {
		t.Fatalf("failed to parse datasets array: %v", err)
	}
	if len(entries2) != 1 {
		t.Fatalf("expected 1 dataset entry, got %d", len(entries2))
	}
	entry := entries2[0]
	for _, field := range []string{"dataset", "version_id", "loaded_at"} {
		if _, ok := entry[field]; !ok {
			t.Errorf("/version dataset entry missing field %q", field)
		}
	}
}

// --- helpers ---

func httpGetJSON[T any](t *testing.T, url string) T {
	t.Helper()
	body := httpGetBody(t, url)
	var v T
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		t.Fatalf("failed to unmarshal %s: %v\nbody: %s", url, err, body)
	}
	return v
}

func httpGetBody(t *testing.T, url string) string {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("GET %s: reading body: %v", url, err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("GET %s: status %d, body: %s", url, resp.StatusCode, b)
	}
	return string(b)
}

// mcGet sends a memcache meta-get and returns the value string.
// It parses the "VA <len>\r\n<value>\r\n" response correctly by reading
// exactly <len> bytes for the value body.
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
		t.Fatalf("mg %s: read status line: %v", key, err)
	}
	line = strings.TrimRight(line, "\r\n")

	if !strings.HasPrefix(line, "VA ") {
		t.Fatalf("mg %s: expected VA, got %q", key, line)
	}

	// Parse value length from "VA <len> [flags...]"
	fields := strings.Fields(line)
	if len(fields) < 2 {
		t.Fatalf("mg %s: malformed VA line: %q", key, line)
	}
	vlen, err := strconv.Atoi(fields[1])
	if err != nil {
		t.Fatalf("mg %s: bad VA length %q: %v", key, fields[1], err)
	}

	// Read exactly vlen bytes + trailing \r\n
	valueBuf := make([]byte, vlen+2)
	if _, err := io.ReadFull(r, valueBuf); err != nil {
		t.Fatalf("mg %s: read value body: %v", key, err)
	}
	return string(valueBuf[:vlen])
}

// mcGetRaw sends `mg <key> v\r\n` and returns the first response line.
func mcGetRaw(t *testing.T, addr, key string) string {
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
		t.Fatalf("mg %s: read: %v", key, err)
	}
	return strings.TrimRight(line, "\r\n")
}
