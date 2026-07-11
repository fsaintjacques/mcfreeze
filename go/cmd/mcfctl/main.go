// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"log/slog"
	"os"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "node-agent":
		runNodeAgent(os.Args[2:])
	case "control-plane":
		runControlPlane(os.Args[2:])
	case "job":
		runJob(os.Args[2:])
	case "ui":
		runUI(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "mcfctl: unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: mcfctl <command> [flags]

Commands:
  node-agent      Run the node-side dataset lifecycle agent
  control-plane   Run the control-plane server
  job             Run an mcf build job (wrapper for mcf load config)
  ui              Run the web UI standalone (for local testing without Kubernetes)
`)
}
