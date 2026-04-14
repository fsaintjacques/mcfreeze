package version

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/fsaintjacques/mcfreeze/go/api"
)

// HTTPChecker implements Checker by polling the KV server's
// GET /version endpoint.
type HTTPChecker struct {
	baseURL  string
	client   *http.Client
	interval time.Duration
}

func NewHTTPChecker(baseURL string) *HTTPChecker {
	return &HTTPChecker{
		baseURL:  baseURL,
		client:   &http.Client{Timeout: 2 * time.Second},
		interval: 500 * time.Millisecond,
	}
}

func (c *HTTPChecker) WaitForVersion(ctx context.Context, dataset, versionID string) error {
	url := fmt.Sprintf("%s/version", c.baseURL)
	var failures int
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := c.client.Do(req)
		if err != nil {
			failures++
			if failures%10 == 1 { // log first failure, then every 10th
				slog.Warn("version check: KV server unreachable",
					"dataset", dataset, "version", versionID,
					"err", err, "consecutive_failures", failures)
			}
		} else {
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
			failures = 0
		}

		select {
		case <-time.After(c.interval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
