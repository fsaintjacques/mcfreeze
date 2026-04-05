package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "node-agent":
		runNodeAgent(os.Args[2:])
	case "control-plane":
		runControlPlane(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "fmtctl: unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: fmtctl <command> [flags]

Commands:
  node-agent      Run the node-side dataset lifecycle agent
  control-plane   Run the control-plane server
`)
}
