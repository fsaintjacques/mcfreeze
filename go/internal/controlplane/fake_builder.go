package controlplane

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"

	"frostmap.io/fmtctl/api"
)

// FakeBuilder implements AsyncBuilder by running fm load csv synchronously
// in Start and storing the result. Used for tests where builds are fast.
//
// Concurrency: the internal mutex protects the results map only. Callers
// must serialize Start calls for the same (dataset, versionID) per the
// AsyncBuilder contract.
type FakeBuilder struct {
	// Data maps dataset name to key-value pairs to include in the snapshot.
	Data map[string][][2][]byte
	// FMBinary is the path to the fm binary. Defaults to "fm".
	FMBinary string
	// Partitions per snapshot. Defaults to 4.
	Partitions int
	// OutputBase is the root directory for snapshot output. Each build creates
	// a subdirectory: <OutputBase>/<dataset>/<versionID>/
	OutputBase string

	mu      sync.Mutex
	results map[BuildHandle]BuildStatus
}

func (b *FakeBuilder) Start(ctx context.Context, spec api.DatasetSpec, versionID string) (BuildHandle, error) {
	handle := BuildHandle(fmt.Sprintf("%s/%s/%s", b.OutputBase, spec.Name, versionID))

	b.mu.Lock()
	if b.results == nil {
		b.results = make(map[BuildHandle]BuildStatus)
	}
	if _, ok := b.results[handle]; ok {
		b.mu.Unlock()
		return handle, nil
	}
	b.mu.Unlock()

	pairs, ok := b.Data[spec.Name]
	if !ok {
		status := BuildStatus{Phase: BuildFailed, Error: fmt.Sprintf("fake builder: no test data for dataset %q", spec.Name)}
		b.mu.Lock()
		b.results[handle] = status
		b.mu.Unlock()
		return handle, fmt.Errorf("fake builder: no test data for dataset %q", spec.Name)
	}

	fm := b.FMBinary
	if fm == "" {
		fm = "fm"
	}
	partitions := b.Partitions
	if partitions <= 0 {
		partitions = 4
	}

	outDir := string(handle)

	var csv strings.Builder
	csv.WriteString("key,value\n")
	for _, kv := range pairs {
		fmt.Fprintf(&csv, "%s,%s\n", kv[0], kv[1])
	}

	cmd := exec.CommandContext(ctx, fm, "load",
		"-o", outDir,
		"--partitions", fmt.Sprintf("%d", partitions),
		"csv",
	)
	cmd.Stdin = strings.NewReader(csv.String())
	out, err := cmd.CombinedOutput()
	if err != nil {
		status := BuildStatus{Phase: BuildFailed, Error: fmt.Sprintf("fm load csv failed: %v\n%s", err, out)}
		b.mu.Lock()
		b.results[handle] = status
		b.mu.Unlock()
		return handle, fmt.Errorf("fake builder: fm load csv failed: %v\n%s", err, out)
	}

	b.mu.Lock()
	b.results[handle] = BuildStatus{
		Phase:  BuildComplete,
		Result: BuildResult{SnapshotPath: outDir},
	}
	b.mu.Unlock()

	return handle, nil
}

func (b *FakeBuilder) Poll(_ context.Context, handle BuildHandle) (BuildStatus, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.results == nil {
		return BuildStatus{Phase: BuildNotFound}, nil
	}
	status, ok := b.results[handle]
	if !ok {
		return BuildStatus{Phase: BuildNotFound}, nil
	}
	return status, nil
}

func (b *FakeBuilder) Cancel(_ context.Context, handle BuildHandle) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.results != nil {
		delete(b.results, handle)
	}
	return nil
}
