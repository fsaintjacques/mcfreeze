package controlplane

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	"encoding/base64"

	"frostmap.io/fmtctl/api"
)

// FakeVersionBuilder implements VersionBuilder by shelling out to fm load csv.
// It uses pre-configured test data keyed by dataset name.
type FakeVersionBuilder struct {
	// Data maps dataset name to key-value pairs to include in the snapshot.
	Data map[string][][2][]byte
	// FMBinary is the path to the fm binary. Defaults to "fm".
	FMBinary string
	// Partitions per snapshot. Defaults to 4.
	Partitions int
	// OutputBase is the root directory for snapshot output. Each build creates
	// a subdirectory: <OutputBase>/<dataset>/<versionID>/
	OutputBase string
}

func (b *FakeVersionBuilder) Build(_ context.Context, spec api.DatasetSpec, versionID string) (string, error) {
	pairs, ok := b.Data[spec.Name]
	if !ok {
		return "", fmt.Errorf("fake builder: no test data for dataset %q", spec.Name)
	}

	fm := b.FMBinary
	if fm == "" {
		fm = "fm"
	}
	partitions := b.Partitions
	if partitions <= 0 {
		partitions = 4
	}

	outDir := fmt.Sprintf("%s/%s/%s", b.OutputBase, spec.Name, versionID)

	var csv strings.Builder
	for _, kv := range pairs {
		fmt.Fprintf(&csv, "%s,%s\n",
			base64.StdEncoding.EncodeToString(kv[0]),
			base64.StdEncoding.EncodeToString(kv[1]),
		)
	}

	cmd := exec.Command(fm, "load",
		"-o", outDir,
		"--partitions", fmt.Sprintf("%d", partitions),
		"csv",
	)
	cmd.Stdin = strings.NewReader(csv.String())
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("fake builder: fm load csv failed: %v\n%s", err, out)
	}

	return outDir, nil
}
