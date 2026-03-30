//go:build integration

package testutil

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"frostmap.io/fmtctl/api"
)

// TestMemcachePipeline sends multiple mg commands in a single TCP write and
// verifies all responses arrive in order. This exercises the server's pipeline
// batch handling — a real memcache client will pipeline requests.
func TestMemcachePipeline(t *testing.T) {
	pairs := []KV{
		{Key: []byte("a"), Value: []byte("alpha")},
		{Key: []byte("b"), Value: []byte("beta")},
		{Key: []byte("c"), Value: []byte("gamma")},
	}
	snapDir := BuildSnapshot(t, pairs, 4)

	srv := StartCatalogServer(t, []api.CatalogEntry{{
		Dataset:   "ds",
		KeyPrefix: "ds",
		VersionID: "v1",
		MountPath: snapDir,
	}})

	conn, err := net.DialTimeout("tcp", srv.TCPAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Send 3 hits + 1 miss + version in a single write.
	var pipeline strings.Builder
	pipeline.WriteString("mg ds:a v\r\n")
	pipeline.WriteString("mg ds:b v\r\n")
	pipeline.WriteString("mg ds:nonexistent v\r\n")
	pipeline.WriteString("mg ds:c v\r\n")
	pipeline.WriteString("version\r\n")

	if _, err := fmt.Fprint(conn, pipeline.String()); err != nil {
		t.Fatalf("write pipeline: %v", err)
	}

	r := bufio.NewReader(conn)

	// Response 1: hit "alpha"
	assertPipelineHit(t, r, "alpha")
	// Response 2: hit "beta"
	assertPipelineHit(t, r, "beta")
	// Response 3: miss
	assertPipelineMiss(t, r)
	// Response 4: hit "gamma"
	assertPipelineHit(t, r, "gamma")
	// Response 5: VERSION line
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read version response: %v", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "VERSION ") {
		t.Errorf("expected VERSION response, got %q", line)
	}
}

// TestMemcachePipelineLargeBatch pipelines many requests to stress the
// server's batching and flush logic.
func TestMemcachePipelineLargeBatch(t *testing.T) {
	const n = 200
	pairs := make([]KV, n)
	for i := range pairs {
		pairs[i] = KV{
			Key:   []byte(fmt.Sprintf("k%04d", i)),
			Value: []byte(fmt.Sprintf("v%04d", i)),
		}
	}
	snapDir := BuildSnapshot(t, pairs, 8)

	srv := StartCatalogServer(t, []api.CatalogEntry{{
		Dataset:   "ds",
		KeyPrefix: "ds",
		VersionID: "v1",
		MountPath: snapDir,
	}})

	conn, err := net.DialTimeout("tcp", srv.TCPAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	// Send all requests in one write.
	var pipeline strings.Builder
	for i := range n {
		fmt.Fprintf(&pipeline, "mg ds:k%04d v\r\n", i)
	}
	if _, err := fmt.Fprint(conn, pipeline.String()); err != nil {
		t.Fatalf("write pipeline: %v", err)
	}

	r := bufio.NewReader(conn)
	for i := range n {
		want := fmt.Sprintf("v%04d", i)
		assertPipelineHit(t, r, want)
	}
}

// --- helpers ---

func assertPipelineHit(t *testing.T, r *bufio.Reader, wantValue string) {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read VA line: %v", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "VA ") {
		t.Fatalf("expected VA, got %q", line)
	}
	fields := strings.Fields(line)
	if len(fields) < 2 {
		t.Fatalf("malformed VA line: %q", line)
	}
	vlen, err := strconv.Atoi(fields[1])
	if err != nil {
		t.Fatalf("bad VA length %q: %v", fields[1], err)
	}
	buf := make([]byte, vlen+2) // value + \r\n
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("read value body: %v", err)
	}
	got := string(buf[:vlen])
	if got != wantValue {
		t.Errorf("value = %q, want %q", got, wantValue)
	}
}

func assertPipelineMiss(t *testing.T, r *bufio.Reader) {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read EN line: %v", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if line != "EN" {
		t.Errorf("expected EN, got %q", line)
	}
}
