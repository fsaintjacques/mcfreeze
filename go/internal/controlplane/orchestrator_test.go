package controlplane

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadDescriptorFromMeta(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "meta.json")

	// With encoding section present.
	meta := `{
		"format_version": 4,
		"encoding": {
			"protobuf": {
				"descriptor": "ChhteXBhY2thZ2UvTXlNZXNzYWdl",
				"message_name": "mypackage.MyMessage"
			}
		}
	}`
	if err := os.WriteFile(metaPath, []byte(meta), 0644); err != nil {
		t.Fatal(err)
	}

	desc, msgName := readDescriptorFromMeta(dir)
	if desc != "ChhteXBhY2thZ2UvTXlNZXNzYWdl" {
		t.Errorf("descriptor = %q, want %q", desc, "ChhteXBhY2thZ2UvTXlNZXNzYWdl")
	}
	if msgName != "mypackage.MyMessage" {
		t.Errorf("message_name = %q, want %q", msgName, "mypackage.MyMessage")
	}
}

func TestReadDescriptorFromMeta_NoEncoding(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "meta.json")

	// Raw-encoded snapshot — no encoding section.
	meta := `{"format_version": 4, "n_partitions": 4}`
	if err := os.WriteFile(metaPath, []byte(meta), 0644); err != nil {
		t.Fatal(err)
	}

	desc, msgName := readDescriptorFromMeta(dir)
	if desc != "" || msgName != "" {
		t.Errorf("expected empty, got descriptor=%q message_name=%q", desc, msgName)
	}
}

func TestReadDescriptorFromMeta_MissingFile(t *testing.T) {
	desc, msgName := readDescriptorFromMeta(t.TempDir())
	if desc != "" || msgName != "" {
		t.Errorf("expected empty for missing file, got descriptor=%q message_name=%q", desc, msgName)
	}
}
