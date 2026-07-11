// SPDX-License-Identifier: Apache-2.0

//go:build integration

package testutil

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildSnapshot(t *testing.T) {
	pairs := []KV{
		{Key: []byte("hello"), Value: []byte("world")},
		{Key: []byte("foo"), Value: []byte("bar")},
	}

	dir := BuildSnapshot(t, pairs, 4)

	if _, err := os.Stat(filepath.Join(dir, "meta.json")); err != nil {
		t.Fatalf("meta.json not found in snapshot dir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "data")); err != nil {
		t.Fatalf("data/ not found in snapshot dir: %v", err)
	}
}

func TestLoadAndGet(t *testing.T) {
	pairs := []KV{
		{Key: []byte("alpha"), Value: []byte("one")},
		{Key: []byte("beta"), Value: []byte("two")},
		{Key: []byte("gamma"), Value: []byte("three")},
	}

	dir := BuildSnapshot(t, pairs, 4)
	mcf := MCFBinary(t)

	for _, p := range pairs {
		got := mcfGet(t, mcf, dir, string(p.Key))
		if got != string(p.Value) {
			t.Errorf("mcf get %q: got %q, want %q", p.Key, got, p.Value)
		}
	}

	// A missing key must produce a non-zero exit.
	cmd := exec.Command(mcf, "get", "--snapshot", dir, "nonexistent")
	cmd.Stderr = nil
	if err := cmd.Run(); err == nil {
		t.Error("mcf get nonexistent: expected non-zero exit, got success")
	}
}

func TestLoadAndGetLargeSnapshot(t *testing.T) {
	const n = 10_000
	pairs := make([]KV, n)
	for i := range pairs {
		pairs[i] = KV{
			Key:   []byte(fmt.Sprintf("key-%05d", i)),
			Value: []byte(fmt.Sprintf("val-%05d", i)),
		}
	}

	dir := BuildSnapshot(t, pairs, 16)
	mcf := MCFBinary(t)

	// Spot-check a sample of keys across the range.
	for _, idx := range []int{0, 1, 100, 999, 5000, 9999} {
		got := mcfGet(t, mcf, dir, string(pairs[idx].Key))
		want := string(pairs[idx].Value)
		if got != want {
			t.Errorf("mcf get %q: got %q, want %q", pairs[idx].Key, got, want)
		}
	}
}

// mcfGet runs `mcf get --snapshot dir key` and returns the trimmed stdout.
func mcfGet(t *testing.T, mcf, dir, key string) string {
	t.Helper()
	cmd := exec.Command(mcf, "get", "--snapshot", dir, key)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("mcf get %q: %v", key, err)
	}
	return strings.TrimRight(string(out), "\n")
}
