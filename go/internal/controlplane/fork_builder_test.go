//go:build integration

package controlplane_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"frostmap.io/fmtctl/api"
	"frostmap.io/fmtctl/internal/controlplane"
)

func TestForkBuilder_StartPollComplete(t *testing.T) {
	outBase := t.TempDir()
	script := createFMScript(t, outBase)

	b := &controlplane.ForkBuilder{
		FMBinary:   script,
		OutputBase: outBase,
	}

	spec := api.DatasetSpec{
		Name:       "ds",
		ShardCount: 4,
		Source:     api.SourceSpec{BigQuery: &api.BigQuerySource{Project: "proj", Table: "tbl"}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	handle, err := b.Start(ctx, spec, "v1")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Poll until complete (the script should finish quickly).
	deadline := time.After(10 * time.Second)
	for {
		status, err := b.Poll(ctx, handle)
		if err != nil {
			t.Fatalf("Poll: %v", err)
		}
		if status.Phase == controlplane.BuildComplete {
			if status.Result.SnapshotPath != string(handle) {
				t.Fatalf("SnapshotPath = %q, want %q", status.Result.SnapshotPath, string(handle))
			}
			break
		}
		if status.Phase == controlplane.BuildFailed {
			t.Fatalf("build failed: %s", status.Error)
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for build to complete")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestForkBuilder_StartIdempotent(t *testing.T) {
	outBase := t.TempDir()
	script := createFMScript(t, outBase)

	b := &controlplane.ForkBuilder{
		FMBinary:   script,
		OutputBase: outBase,
	}

	spec := api.DatasetSpec{
		Name:       "ds",
		ShardCount: 4,
		Source:     api.SourceSpec{BigQuery: &api.BigQuerySource{Project: "p", Table: "t"}},
	}

	ctx := context.Background()

	h1, err := b.Start(ctx, spec, "v1")
	if err != nil {
		t.Fatalf("Start 1: %v", err)
	}

	// Wait for completion.
	waitForBuild(t, b, ctx, h1, 10*time.Second)

	// Second Start should return the same handle (meta.json exists).
	h2, err := b.Start(ctx, spec, "v1")
	if err != nil {
		t.Fatalf("Start 2: %v", err)
	}
	if h1 != h2 {
		t.Fatalf("handles differ: %q vs %q", h1, h2)
	}
}

func TestForkBuilder_Cancel(t *testing.T) {
	outBase := t.TempDir()
	// Create a script that sleeps forever.
	script := createSleeperScript(t, outBase)

	b := &controlplane.ForkBuilder{
		FMBinary:    script,
		OutputBase:  outBase,
		GracePeriod: 1 * time.Second,
	}

	spec := api.DatasetSpec{
		Name:       "ds",
		ShardCount: 4,
		Source:     api.SourceSpec{BigQuery: &api.BigQuerySource{Project: "p", Table: "t"}},
	}

	ctx := context.Background()

	handle, err := b.Start(ctx, spec, "v1")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Verify it's running.
	status, _ := b.Poll(ctx, handle)
	if status.Phase != controlplane.BuildRunning {
		t.Fatalf("expected running, got %s", status.Phase)
	}

	// Cancel.
	if err := b.Cancel(ctx, handle); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	// Output dir should be cleaned up.
	if _, err := os.Stat(string(handle)); !os.IsNotExist(err) {
		t.Fatalf("expected output dir removed, got err=%v", err)
	}

	// Poll should return not_found.
	status, _ = b.Poll(ctx, handle)
	if status.Phase != controlplane.BuildNotFound {
		t.Fatalf("expected not_found after cancel, got %s", status.Phase)
	}
}

func TestForkBuilder_RestartRecovery(t *testing.T) {
	outBase := t.TempDir()
	script := createSlowFMScript(t, outBase, 2*time.Second)

	spec := api.DatasetSpec{
		Name:       "ds",
		ShardCount: 4,
		Source:     api.SourceSpec{BigQuery: &api.BigQuerySource{Project: "p", Table: "t"}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start a build with builder instance 1.
	b1 := &controlplane.ForkBuilder{
		FMBinary:   script,
		OutputBase: outBase,
	}

	handle, err := b1.Start(ctx, spec, "v1")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Verify running.
	status, _ := b1.Poll(ctx, handle)
	if status.Phase != controlplane.BuildRunning {
		t.Fatalf("expected running, got %s", status.Phase)
	}

	// "Restart": create a new ForkBuilder with the same OutputBase (simulating
	// control-plane restart). The old builder instance is dropped.
	b2 := &controlplane.ForkBuilder{
		FMBinary:   script,
		OutputBase: outBase,
	}

	// Poll with the new instance — should detect the running process.
	status, _ = b2.Poll(ctx, handle)
	if status.Phase != controlplane.BuildRunning {
		t.Fatalf("after restart, expected running, got %s", status.Phase)
	}

	// Wait for the build to complete.
	waitForBuild(t, b2, ctx, handle, 15*time.Second)

	status, _ = b2.Poll(ctx, handle)
	if status.Phase != controlplane.BuildComplete {
		t.Fatalf("expected complete, got %s", status.Phase)
	}
}

func TestForkBuilder_PollNotFound(t *testing.T) {
	b := &controlplane.ForkBuilder{
		OutputBase: t.TempDir(),
	}

	status, err := b.Poll(context.Background(), controlplane.BuildHandle("/nonexistent/path"))
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if status.Phase != controlplane.BuildNotFound {
		t.Fatalf("expected not_found, got %s", status.Phase)
	}
}

// --- helpers ---

// createFMScript creates a shell script that mimics fm: reads -o flag,
// creates the output directory and writes meta.json.
func createFMScript(t *testing.T, baseDir string) string {
	t.Helper()
	script := filepath.Join(baseDir, "fake-fm.sh")
	content := `#!/bin/sh
# Parse -o flag
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o) shift; OUTDIR="$1"; shift;;
    *) shift;;
  esac
done
mkdir -p "$OUTDIR"
echo '{"format_version":3,"n_partitions":4}' > "$OUTDIR/meta.json"
`
	if err := os.WriteFile(script, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
	return script
}

// createSleeperScript creates a script that sleeps indefinitely (for cancel tests).
func createSleeperScript(t *testing.T, baseDir string) string {
	t.Helper()
	script := filepath.Join(baseDir, "sleeper-fm.sh")
	content := `#!/bin/sh
# Parse -o flag
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o) shift; OUTDIR="$1"; shift;;
    *) shift;;
  esac
done
mkdir -p "$OUTDIR"
sleep 3600
`
	if err := os.WriteFile(script, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
	return script
}

// createSlowFMScript creates a script that sleeps for the given duration
// before writing meta.json (for restart recovery tests).
func createSlowFMScript(t *testing.T, baseDir string, delay time.Duration) string {
	t.Helper()
	script := filepath.Join(baseDir, "slow-fm.sh")
	content := `#!/bin/sh
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o) shift; OUTDIR="$1"; shift;;
    *) shift;;
  esac
done
mkdir -p "$OUTDIR"
sleep ` + fmt.Sprintf("%d", int(delay.Seconds())) + `
echo '{"format_version":3,"n_partitions":4}' > "$OUTDIR/meta.json"
`
	if err := os.WriteFile(script, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
	return script
}

func waitForBuild(t *testing.T, b *controlplane.ForkBuilder, ctx context.Context, handle controlplane.BuildHandle, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		status, err := b.Poll(ctx, handle)
		if err != nil {
			t.Fatalf("Poll: %v", err)
		}
		if status.Phase == controlplane.BuildComplete {
			return
		}
		if status.Phase == controlplane.BuildFailed {
			t.Fatalf("build failed: %s", status.Error)
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for build")
		case <-time.After(100 * time.Millisecond):
		}
	}
}
