package assignment

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"frostmap.io/fmtctl/api"
)

// HTTPSource implements Source via the control-plane HTTP API.
type HTTPSource struct {
	baseURL  string
	nodeName string
	client   *http.Client
}

func NewHTTPSource(baseURL, nodeName string) *HTTPSource {
	return &HTTPSource{
		baseURL:  baseURL,
		nodeName: nodeName,
		// Long timeout: the server holds the request until assignments change.
		client: &http.Client{Timeout: 5 * time.Minute},
	}
}

func (s *HTTPSource) FetchAssignments(ctx context.Context, generation int64) (*api.AssignmentsResponse, error) {
	url := fmt.Sprintf("%s/api/v1/node/%s/assignments?generation=%d", s.baseURL, s.nodeName, generation)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("assignments: HTTP %d: %s", resp.StatusCode, body)
	}
	var result api.AssignmentsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("assignments: decode: %w", err)
	}
	return &result, nil
}

// HTTPStateReporter implements StateReporter via the control-plane HTTP API.
type HTTPStateReporter struct {
	baseURL  string
	nodeName string
	client   *http.Client
}

func NewHTTPStateReporter(baseURL, nodeName string) *HTTPStateReporter {
	return &HTTPStateReporter{
		baseURL:  baseURL,
		nodeName: nodeName,
		client:   &http.Client{Timeout: 10 * time.Second},
	}
}

func (r *HTTPStateReporter) ReportState(ctx context.Context, state api.NodeState) error {
	url := fmt.Sprintf("%s/api/v1/node/%s/state", r.baseURL, r.nodeName)
	body, err := json.Marshal(state)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("report state: HTTP %d: %s", resp.StatusCode, respBody)
	}
	return nil
}
