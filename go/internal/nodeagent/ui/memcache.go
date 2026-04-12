package ui

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// memcacheGet performs a single memcache meta-protocol get against addr.
// It returns the value bytes and true on a hit, nil and false on a miss.
func memcacheGet(addr, key string) ([]byte, bool, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, false, fmt.Errorf("connect to kv-server %s: %w", addr, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Send meta-get: "mg <key> v\r\n"
	// The 'v' flag requests the value be returned inline.
	if _, err := fmt.Fprintf(conn, "mg %s v\r\n", key); err != nil {
		return nil, false, fmt.Errorf("write mg command: %w", err)
	}

	return parseMetaGetResponse(bufio.NewReader(conn))
}

// parseMetaGetResponse parses a memcache meta-protocol response.
// Hit:  "VA <len> [flags...]\r\n<value>\r\n"
// Miss: "EN\r\n"
func parseMetaGetResponse(r *bufio.Reader) ([]byte, bool, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, false, fmt.Errorf("read response line: %w", err)
	}
	line = strings.TrimRight(line, "\r\n")

	if line == "EN" {
		return nil, false, nil
	}

	if strings.HasPrefix(line, "SERVER_ERROR") || strings.HasPrefix(line, "CLIENT_ERROR") {
		return nil, false, fmt.Errorf("kv-server: %s", line)
	}

	if !strings.HasPrefix(line, "VA ") {
		return nil, false, fmt.Errorf("unexpected response: %q", line)
	}

	// Parse "VA <len> [flags...]"
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return nil, false, fmt.Errorf("malformed VA response: %q", line)
	}
	size, err := strconv.Atoi(fields[1])
	if err != nil {
		return nil, false, fmt.Errorf("parse value length %q: %w", fields[1], err)
	}

	// Read exactly size bytes + trailing \r\n
	buf := make([]byte, size+2)
	n, err := readFull(r, buf)
	if err != nil {
		return nil, false, fmt.Errorf("read value (%d/%d bytes): %w", n, size+2, err)
	}

	return buf[:size], true, nil
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
