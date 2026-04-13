package ui

import (
	"bufio"
	"strings"
	"testing"
)

func TestParseMetaGetResponse_Hit(t *testing.T) {
	// VA 5\r\nworld\r\n
	input := "VA 5\r\nworld\r\n"
	r := bufio.NewReader(strings.NewReader(input))

	value, hit, err := parseMetaGetResponse(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hit {
		t.Fatal("expected hit")
	}
	if string(value) != "world" {
		t.Fatalf("expected %q, got %q", "world", string(value))
	}
}

func TestParseMetaGetResponse_Miss(t *testing.T) {
	input := "EN\r\n"
	r := bufio.NewReader(strings.NewReader(input))

	value, hit, err := parseMetaGetResponse(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hit {
		t.Fatal("expected miss")
	}
	if value != nil {
		t.Fatalf("expected nil value, got %v", value)
	}
}

func TestParseMetaGetResponse_HitWithFlags(t *testing.T) {
	// VA 5 kmykey t-1\r\nhello\r\n
	input := "VA 5 kmykey t-1\r\nhello\r\n"
	r := bufio.NewReader(strings.NewReader(input))

	value, hit, err := parseMetaGetResponse(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hit {
		t.Fatal("expected hit")
	}
	if string(value) != "hello" {
		t.Fatalf("expected %q, got %q", "hello", string(value))
	}
}

func TestParseMetaGetResponse_ServerError(t *testing.T) {
	input := "SERVER_ERROR read-only\r\n"
	r := bufio.NewReader(strings.NewReader(input))

	_, _, err := parseMetaGetResponse(r)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("expected error containing 'read-only', got %v", err)
	}
}

func TestMemcacheGet_RejectsInvalidKeys(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{"empty", ""},
		{"contains space", "bad key"},
		{"contains CR", "bad\rkey"},
		{"contains LF", "bad\nkey"},
		{"contains CRLF", "inject\r\ndelete foo"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should reject without attempting a TCP connection.
			_, _, err := memcacheGet("localhost:0", tt.key)
			if err == nil {
				t.Fatal("expected error for invalid key")
			}
			if !strings.Contains(err.Error(), "invalid key") {
				t.Fatalf("expected 'invalid key' error, got: %v", err)
			}
		})
	}
}

func TestParseMetaGetResponse_BinaryValue(t *testing.T) {
	// Binary payload: 4 bytes
	payload := "\x00\x01\x02\x03"
	input := "VA 4\r\n" + payload + "\r\n"
	r := bufio.NewReader(strings.NewReader(input))

	value, hit, err := parseMetaGetResponse(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hit {
		t.Fatal("expected hit")
	}
	if len(value) != 4 {
		t.Fatalf("expected 4 bytes, got %d", len(value))
	}
	for i, b := range value {
		if b != byte(i) {
			t.Fatalf("byte %d: expected %d, got %d", i, i, b)
		}
	}
}
