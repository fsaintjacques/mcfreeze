package nodeagent

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

// HTTPAssignmentSource implements AssignmentSource via the control-plane HTTP API.
type HTTPAssignmentSource struct {
	baseURL  string
	nodeName string
	client   *http.Client
}

func NewHTTPAssignmentSource(baseURL, nodeName string) *HTTPAssignmentSource {
	return &HTTPAssignmentSource{
		baseURL:  baseURL,
		nodeName: nodeName,
		// Long timeout: the server holds the request until assignments change.
		client: &http.Client{Timeout: 5 * time.Minute},
	}
}

func (s *HTTPAssignmentSource) FetchAssignments(ctx context.Context, generation int64) (*api.AssignmentsResponse, error) {
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

// HTTPVersionChecker implements VersionChecker by polling the KV server's
// GET /version endpoint.
type HTTPVersionChecker struct {
	baseURL  string
	client   *http.Client
	interval time.Duration
}

func NewHTTPVersionChecker(baseURL string) *HTTPVersionChecker {
	return &HTTPVersionChecker{
		baseURL:  baseURL,
		client:   &http.Client{Timeout: 2 * time.Second},
		interval: 500 * time.Millisecond,
	}
}

func (c *HTTPVersionChecker) WaitForVersion(ctx context.Context, dataset, versionID string) error {
	url := fmt.Sprintf("%s/version", c.baseURL)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := c.client.Do(req)
		if err == nil {
			var vr api.KVVersionResponse
			if decErr := json.NewDecoder(resp.Body).Decode(&vr); decErr == nil {
				for _, ds := range vr.Datasets {
					if ds.Dataset == dataset && ds.VersionID == versionID {
						resp.Body.Close()
						return nil
					}
				}
			}
			resp.Body.Close()
		}

		select {
		case <-time.After(c.interval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
