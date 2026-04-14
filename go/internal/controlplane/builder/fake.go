package builder

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"

	"github.com/fsaintjacques/mcfreeze/go/api"
)

// Fake implements Async by running mcf load csv synchronously
// in Start and storing the result. Used for tests where builds are fast.
//
// Concurrency: the internal mutex protects the results map only. Callers
// must serialize Start calls for the same (dataset, versionID) per the
// Async contract.
type Fake struct {
	// Data maps dataset name to key-value pairs to include in the snapshot.
	Data map[string][][2][]byte
	// MCFBinary is the path to the mcf binary. Defaults to "mcf".
	MCFBinary string
	// Partitions per snapshot. Defaults to 4.
	Partitions int
	// OutputBase is the root directory for snapshot output. Each build creates
	// a subdirectory: <OutputBase>/<dataset>/<versionID>/
	OutputBase string

	mu      sync.Mutex
	results map[Handle]Status
}

func (b *Fake) Start(ctx context.Context, spec api.DatasetSpec, versionID string) (Handle, error) {
	handle := Handle(fmt.Sprintf("%s/%s/%s", b.OutputBase, spec.Name, versionID))

	b.mu.Lock()
	if b.results == nil {
		b.results = make(map[Handle]Status)
	}
	if _, ok := b.results[handle]; ok {
		b.mu.Unlock()
		return handle, nil
	}
	b.mu.Unlock()

	pairs, ok := b.Data[spec.Name]
	if !ok {
		status := Status{Phase: Failed, Error: fmt.Sprintf("fake builder: no test data for dataset %q", spec.Name)}
		b.mu.Lock()
		b.results[handle] = status
		b.mu.Unlock()
		return handle, fmt.Errorf("fake builder: no test data for dataset %q", spec.Name)
	}

	mcf := b.MCFBinary
	if mcf == "" {
		mcf = "mcf"
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

	cmd := exec.CommandContext(ctx, mcf, "load",
		"-o", outDir,
		"--partitions", fmt.Sprintf("%d", partitions),
		"csv",
	)
	cmd.Stdin = strings.NewReader(csv.String())
	out, err := cmd.CombinedOutput()
	if err != nil {
		status := Status{Phase: Failed, Error: fmt.Sprintf("mcf load csv failed: %v\n%s", err, out)}
		b.mu.Lock()
		b.results[handle] = status
		b.mu.Unlock()
		return handle, fmt.Errorf("fake builder: mcf load csv failed: %v\n%s", err, out)
	}

	b.mu.Lock()
	b.results[handle] = Status{
		Phase:  Complete,
		Result: Result{SnapshotPath: outDir},
	}
	b.mu.Unlock()

	return handle, nil
}

func (b *Fake) Poll(_ context.Context, handle Handle) (Status, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.results == nil {
		return Status{Phase: NotFound}, nil
	}
	status, ok := b.results[handle]
	if !ok {
		return Status{Phase: NotFound}, nil
	}
	return status, nil
}

func (b *Fake) Cancel(_ context.Context, handle Handle) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.results != nil {
		delete(b.results, handle)
	}
	return nil
}
