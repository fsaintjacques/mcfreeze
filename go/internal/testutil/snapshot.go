// Package testutil provides test helpers for integration tests.
package testutil

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// MCFBinary returns the path to the mcf binary. It reads the MCF environment
// variable set by the Makefile's test-integration target. If unset, it
// falls back to "mcf" in $PATH.
func MCFBinary(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("MCF"); p != "" {
		return p
	}
	return "mcf"
}

// KV is a single key-value pair (both are raw bytes).
type KV struct {
	Key   []byte
	Value []byte
}

// BuildSnapshot creates a snapshot directory from the given key-value pairs
// by invoking `mcf load csv` with the data piped via stdin.
//
// It returns the path to the snapshot directory. The directory is
// automatically cleaned up when the test finishes.
func BuildSnapshot(t *testing.T, pairs []KV, partitions int) string {
	t.Helper()

	dir := t.TempDir()

	if partitions <= 0 {
		partitions = 4
	}

	// Build CSV with header row; keys and values are written as raw strings.
	var csv strings.Builder
	csv.WriteString("key,value\n")
	for _, p := range pairs {
		fmt.Fprintf(&csv, "%s,%s\n", p.Key, p.Value)
	}

	mcf := MCFBinary(t)
	cmd := exec.Command(mcf, "load",
		"-o", dir,
		"--partitions", fmt.Sprintf("%d", partitions),
		"csv",
	)
	cmd.Stdin = strings.NewReader(csv.String())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mcf load csv failed: %v\n%s", err, out)
	}

	return dir
}
