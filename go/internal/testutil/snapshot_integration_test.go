//go:build integration

package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBuildSnapshot(t *testing.T) {
	pairs := []KV{
		{Key: []byte("hello"), Value: []byte("world")},
		{Key: []byte("foo"), Value: []byte("bar")},
	}

	dir := BuildSnapshot(t, pairs, 4)

	// Verify the snapshot directory contains expected artifacts.
	if _, err := os.Stat(filepath.Join(dir, "meta.json")); err != nil {
		t.Fatalf("meta.json not found in snapshot dir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "data")); err != nil {
		t.Fatalf("data/ not found in snapshot dir: %v", err)
	}
}
