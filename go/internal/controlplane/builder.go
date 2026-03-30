package controlplane

import (
	"context"

	"frostmap.io/fmtctl/api"
)

// VersionBuilder builds a snapshot for a dataset version.
// The production implementation (Phase 4) creates a Kubernetes Job running
// fm load bq. For tests, FakeVersionBuilder shells out to fm load csv.
type VersionBuilder interface {
	Build(ctx context.Context, spec api.DatasetSpec, versionID string) (snapshotPath string, err error)
}
