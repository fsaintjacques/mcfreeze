// Package testutil provides test helpers for integration tests.
package testutil

import (
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// FMBinary returns the path to the fm binary. It reads the FM environment
// variable set by the Makefile's test-integration target. If unset, it
// falls back to "fm" in $PATH.
func FMBinary(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("FM"); p != "" {
		return p
	}
	return "fm"
}

// KV is a single key-value pair (both are raw bytes).
type KV struct {
	Key   []byte
	Value []byte
}

// BuildSnapshot creates a snapshot directory from the given key-value pairs
// by invoking `fm load csv` with the data piped via stdin.
//
// It returns the path to the snapshot directory. The directory is
// automatically cleaned up when the test finishes.
func BuildSnapshot(t *testing.T, pairs []KV, partitions int) string {
	t.Helper()

	dir := t.TempDir()

	if partitions <= 0 {
		partitions = 4
	}

	// Build CSV: each row is base64(key),base64(value)
	var csv strings.Builder
	for _, p := range pairs {
		fmt.Fprintf(&csv, "%s,%s\n",
			base64.StdEncoding.EncodeToString(p.Key),
			base64.StdEncoding.EncodeToString(p.Value),
		)
	}

	fm := FMBinary(t)
	cmd := exec.Command(fm, "load",
		"-o", dir,
		"--partitions", fmt.Sprintf("%d", partitions),
		"csv",
	)
	cmd.Stdin = strings.NewReader(csv.String())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("fm load csv failed: %v\n%s", err, out)
	}

	return dir
}
