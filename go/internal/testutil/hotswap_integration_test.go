//go:build integration

package testutil

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/fsaintjacques/frostmap/go/api"
)

// TestCatalogHotSwap verifies that writing a new catalog.json triggers the
// server to reload and serve the updated snapshot. This is the same flow the
// node-agent uses in production: write catalog.json → poll GET /version →
// verify memcache lookups return new data.
func TestCatalogHotSwap(t *testing.T) {
	// --- v1: initial snapshot ---
	v1Pairs := []KV{
		{Key: []byte("key-a"), Value: []byte("v1-alpha")},
		{Key: []byte("key-b"), Value: []byte("v1-beta")},
	}
	v1Dir := BuildSnapshot(t, v1Pairs, 4)

	entries := []api.CatalogEntry{{
		Dataset:   "ds",
		KeyPrefix: "ds",
		VersionID: "v1",
		MountPath: v1Dir,
	}}
	srv := StartCatalogServer(t, entries)

	// Confirm v1 is active.
	assertVersion(t, srv, "ds", "v1")
	assertMcGet(t, srv, "ds:key-a", "v1-alpha")
	assertMcGet(t, srv, "ds:key-b", "v1-beta")

	// --- v2: new snapshot with different data ---
	v2Pairs := []KV{
		{Key: []byte("key-a"), Value: []byte("v2-alpha")},
		{Key: []byte("key-b"), Value: []byte("v2-beta")},
		{Key: []byte("key-c"), Value: []byte("v2-gamma")},
	}
	v2Dir := BuildSnapshot(t, v2Pairs, 4)

	// Atomic catalog swap — this triggers the inotify watcher.
	srv.WriteCatalog(t, []api.CatalogEntry{{
		Dataset:   "ds",
		KeyPrefix: "ds",
		VersionID: "v2",
		MountPath: v2Dir,
	}})

	// Poll GET /version until the server reports v2.
	waitForVersion(t, srv, "ds", "v2", 5*time.Second)

	// Verify new data is served.
	assertMcGet(t, srv, "ds:key-a", "v2-alpha")
	assertMcGet(t, srv, "ds:key-b", "v2-beta")
	assertMcGet(t, srv, "ds:key-c", "v2-gamma")
}

// TestCatalogHotSwapMultiDataset verifies hot-swap with multiple datasets,
// swapping only one while the other remains unchanged.
func TestCatalogHotSwapMultiDataset(t *testing.T) {
	ds1Dir := BuildSnapshot(t, []KV{{Key: []byte("k"), Value: []byte("ds1-v1")}}, 4)
	ds2Dir := BuildSnapshot(t, []KV{{Key: []byte("k"), Value: []byte("ds2-v1")}}, 4)

	entries := []api.CatalogEntry{
		{Dataset: "ds1", KeyPrefix: "ds1", VersionID: "v1", MountPath: ds1Dir},
		{Dataset: "ds2", KeyPrefix: "ds2", VersionID: "v1", MountPath: ds2Dir},
	}
	srv := StartCatalogServer(t, entries)

	assertMcGet(t, srv, "ds1:k", "ds1-v1")
	assertMcGet(t, srv, "ds2:k", "ds2-v1")

	// Swap only ds1 to v2; ds2 stays at v1.
	ds1v2Dir := BuildSnapshot(t, []KV{{Key: []byte("k"), Value: []byte("ds1-v2")}}, 4)
	srv.WriteCatalog(t, []api.CatalogEntry{
		{Dataset: "ds1", KeyPrefix: "ds1", VersionID: "v2", MountPath: ds1v2Dir},
		{Dataset: "ds2", KeyPrefix: "ds2", VersionID: "v1", MountPath: ds2Dir},
	})

	waitForVersion(t, srv, "ds1", "v2", 5*time.Second)

	assertMcGet(t, srv, "ds1:k", "ds1-v2")
	assertMcGet(t, srv, "ds2:k", "ds2-v1") // unchanged
}

// TestCatalogLateArrival verifies that the server starts with an empty catalog
// when catalog.json does not exist, and loads it when it first appears.
// This matches the Kubernetes pod startup race: the KV server container starts
// before the node-agent has written catalog.json.
func TestCatalogLateArrival(t *testing.T) {
	srv := StartEmptyCatalogServer(t)

	// GET /version should return an empty dataset list.
	resp := httpGetJSON[api.KVVersionResponse](t, fmt.Sprintf("http://%s/version", srv.HTTPAddr))
	if len(resp.Datasets) != 0 {
		t.Fatalf("expected 0 datasets before catalog write, got %d", len(resp.Datasets))
	}

	// All lookups should miss.
	raw := mcGetRaw(t, srv.TCPAddr, "ds:anything")
	if raw != "EN" {
		t.Fatalf("expected EN before catalog write, got %q", raw)
	}

	// Node-agent writes the first catalog.
	snapDir := BuildSnapshot(t, []KV{
		{Key: []byte("hello"), Value: []byte("world")},
	}, 4)
	srv.WriteCatalog(t, []api.CatalogEntry{{
		Dataset:   "ds",
		KeyPrefix: "ds",
		VersionID: "v1",
		MountPath: snapDir,
	}})

	waitForVersion(t, srv, "ds", "v1", 5*time.Second)
	assertMcGet(t, srv, "ds:hello", "world")
}

// --- helpers ---

func assertVersion(t *testing.T, srv *Server, dataset, wantVersion string) {
	t.Helper()
	resp := httpGetJSON[api.KVVersionResponse](t, fmt.Sprintf("http://%s/version", srv.HTTPAddr))
	for _, ds := range resp.Datasets {
		if ds.Dataset == dataset {
			if ds.VersionID != wantVersion {
				t.Errorf("GET /version: %s version = %q, want %q", dataset, ds.VersionID, wantVersion)
			}
			return
		}
	}
	t.Errorf("GET /version: dataset %q not found in response", dataset)
}

func waitForVersion(t *testing.T, srv *Server, dataset, wantVersion string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		body := httpGetBody(t, fmt.Sprintf("http://%s/version", srv.HTTPAddr))
		var resp api.KVVersionResponse
		if err := json.Unmarshal([]byte(body), &resp); err == nil {
			for _, ds := range resp.Datasets {
				if ds.Dataset == dataset && ds.VersionID == wantVersion {
					return
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("dataset %q did not reach version %q within %v", dataset, wantVersion, timeout)
}

func assertMcGet(t *testing.T, srv *Server, key, want string) {
	t.Helper()
	got := mcGet(t, srv.TCPAddr, key)
	if got != want {
		t.Errorf("mg %s = %q, want %q", key, got, want)
	}
}
