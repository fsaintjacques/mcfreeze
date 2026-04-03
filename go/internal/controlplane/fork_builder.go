package controlplane

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"frostmap.io/fmtctl/api"
)

// ForkBuilder implements AsyncBuilder by forking an fm subprocess.
// The build handle is the output directory path, which is deterministic
// from (OutputBase, dataset, versionID), making Start naturally idempotent.
//
// Concurrency: ForkBuilder is safe for concurrent use across different
// (dataset, versionID) pairs. Callers must serialize Start calls for the
// same (dataset, versionID) — the orchestrator guarantees this.
//
// Known limitation: process identity is tracked by PID only. PID recycling
// could cause Poll to misidentify an unrelated process as the build, or
// Cancel to kill a wrong process. This is acceptable for the fork builder's
// expected lifetime (transitional until K8s Job builder).
type ForkBuilder struct {
	// FMBinary is the path to the fm binary. Defaults to "fm".
	FMBinary string
	// OutputBase is the root directory for build output.
	// Each build writes to <OutputBase>/<dataset>/<versionID>/.
	OutputBase string
	// GracePeriod is the time between SIGTERM and SIGKILL during Cancel.
	// Defaults to 10s.
	GracePeriod time.Duration
}

const pidFile = ".build.pid"

func (b *ForkBuilder) outDir(dataset, versionID string) string {
	return filepath.Join(b.OutputBase, dataset, versionID)
}

func (b *ForkBuilder) gracePeriod() time.Duration {
	if b.GracePeriod > 0 {
		return b.GracePeriod
	}
	return 10 * time.Second
}

func (b *ForkBuilder) fmBinary() string {
	if b.FMBinary != "" {
		return b.FMBinary
	}
	return "fm"
}

// Start kicks off an fm subprocess. Idempotent: if the build already
// completed (meta.json exists) or is still running (pid alive), returns
// the existing handle.
func (b *ForkBuilder) Start(ctx context.Context, spec api.DatasetSpec, versionID string) (BuildHandle, error) {
	dir := b.outDir(spec.Name, versionID)
	handle := BuildHandle(dir)

	// Already complete?
	if _, err := os.Stat(filepath.Join(dir, "meta.json")); err == nil {
		return handle, nil
	}

	// Already running?
	if pid, err := b.readPid(dir); err == nil {
		if processAlive(pid) {
			return handle, nil
		}
		// Dead process, no meta.json — clean up stale state.
		os.RemoveAll(dir)
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("fork builder: mkdir %s: %w", dir, err)
	}

	cmd := exec.CommandContext(ctx, b.fmBinary(), "load",
		"-o", dir,
		"--partitions", fmt.Sprintf("%d", spec.ShardCount),
		"bq",
		"--project", spec.Source.Project,
		"--table", spec.Source.Table,
	)
	// Detach from parent process group so the child survives if the
	// control-plane exits.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		os.RemoveAll(dir)
		return "", fmt.Errorf("fork builder: start fm: %w", err)
	}

	// Reap the child in the background so we don't leak a zombie or the
	// exec.Cmd internal goroutine. We track the process via the pid file,
	// not via the exec.Cmd.
	go cmd.Wait()

	// Write pid file.
	pidPath := filepath.Join(dir, pidFile)
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(cmd.Process.Pid)), 0o644); err != nil {
		cmd.Process.Kill()
		os.RemoveAll(dir)
		return "", fmt.Errorf("fork builder: write pid: %w", err)
	}

	return handle, nil
}

// Poll checks the build status by inspecting the output directory.
func (b *ForkBuilder) Poll(_ context.Context, handle BuildHandle) (BuildStatus, error) {
	dir := string(handle)

	// Completed?
	if _, err := os.Stat(filepath.Join(dir, "meta.json")); err == nil {
		return BuildStatus{
			Phase:  BuildComplete,
			Result: BuildResult{SnapshotPath: dir},
		}, nil
	}

	// Read pid.
	pid, err := b.readPid(dir)
	if err != nil {
		return BuildStatus{Phase: BuildNotFound}, nil
	}

	// Process alive?
	if processAlive(pid) {
		return BuildStatus{Phase: BuildRunning}, nil
	}

	// Dead process, no meta.json.
	return BuildStatus{Phase: BuildFailed, Error: "process exited without producing meta.json"}, nil
}

// Cancel sends SIGTERM, waits up to GracePeriod, then SIGKILL, then
// removes the output directory.
func (b *ForkBuilder) Cancel(_ context.Context, handle BuildHandle) error {
	dir := string(handle)

	pid, err := b.readPid(dir)
	if err != nil {
		// No pid file — nothing to kill; clean up dir if it exists.
		os.RemoveAll(dir)
		return nil
	}

	proc, err := os.FindProcess(pid)
	if err != nil {
		os.RemoveAll(dir)
		return nil
	}

	// SIGTERM — if it fails, the process is already gone.
	if err := proc.Signal(syscall.SIGTERM); err != nil {
		os.RemoveAll(dir)
		return nil
	}

	// Wait for exit with grace period.
	deadline := time.After(b.gracePeriod())
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	alive := true
	for alive {
		select {
		case <-deadline:
			proc.Signal(syscall.SIGKILL)
			alive = false
		case <-ticker.C:
			if !processAlive(pid) {
				alive = false
			}
		}
	}

	// Wait a bit for SIGKILL to take effect.
	for i := 0; i < 10 && processAlive(pid); i++ {
		time.Sleep(10 * time.Millisecond)
	}

	os.RemoveAll(dir)
	return nil
}

// readPid reads the pid from the .build.pid file in dir.
func (b *ForkBuilder) readPid(dir string) (int, error) {
	data, err := os.ReadFile(filepath.Join(dir, pidFile))
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(data)))
}

// processAlive checks whether a process with the given pid is alive
// by sending signal 0.
func processAlive(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = proc.Signal(syscall.Signal(0))
	return err == nil || errors.Is(err, syscall.EPERM)
}
