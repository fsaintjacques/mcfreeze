// Package version provides the KV server version-checking interface used
// by the node-agent to confirm that a dataset version is active.
package version

import "context"

// Checker confirms a dataset version is active on the KV server.
type Checker interface {
	// WaitForVersion polls the KV server until it reports the given version
	// for the dataset, or ctx is cancelled.
	WaitForVersion(ctx context.Context, dataset, versionID string) error
}
