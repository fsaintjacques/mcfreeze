package main

import (
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
)

func runJob(args []string) {
	fs := flag.NewFlagSet("job", flag.ExitOnError)

	configPath := fs.String("config", "", "path to worker.json config file (required)")
	fmBinary := fs.String("fm-binary", "fm", "path to fm binary")
	fs.Parse(args)

	if *configPath == "" {
		fs.Usage()
		os.Exit(1)
	}

	// Forward SIGTERM/SIGINT to the child process.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	cmd := exec.Command(*fmBinary, "load", "config", "--config", *configPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	slog.Info("starting fm build", "config", *configPath, "binary", *fmBinary)

	if err := cmd.Start(); err != nil {
		slog.Error("failed to start fm", "err", err)
		os.Exit(1)
	}

	// Forward signals to child in a separate goroutine.
	go func() {
		for s := range sig {
			if cmd.Process != nil {
				cmd.Process.Signal(s)
			}
		}
	}()

	if err := cmd.Wait(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			os.Exit(exitErr.ExitCode())
		}
		slog.Error("fm process failed", "err", err)
		os.Exit(1)
	}

	slog.Info("fm build completed successfully")
}
