package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"tuba/product/internal/launcher"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "tuba-launcher:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return errors.New("command is required")
	}
	command := args[0]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	manifest := flags.String("manifest", "tuba-services.json", "service manifest path")
	service := flags.String("service", "", "service name for logs")
	tail := flags.Int("tail", 100, "number of log lines to print")
	timeout := flags.Duration("timeout", 30*time.Second, "maximum graceful stop wait")
	defaults := flags.String("defaults", "", "new release example manifest path")
	previousDefaults := flags.String("previous-defaults", "", "currently installed release example manifest path")
	output := flags.String("output", "", "candidate manifest output path")
	currentPath := flags.String("current-path", "", "currently active release path")
	releasePath := flags.String("release-path", "", "new release path used for validation")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if len(flags.Args()) != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	switch command {
	case "validate":
		return launcher.Validate(*manifest)
	case "merge-manifest":
		if *defaults == "" || *previousDefaults == "" || *output == "" || *currentPath == "" || *releasePath == "" {
			return errors.New("merge-manifest requires --defaults, --previous-defaults, --output, --current-path, and --release-path")
		}
		return launcher.MergeManifest(*manifest, *previousDefaults, *defaults, *output, *currentPath, *releasePath)
	case "start":
		return launcher.Start(*manifest)
	case "stop":
		return launcher.Stop(*manifest, *timeout)
	case "restart":
		return launcher.Restart(*manifest)
	case "status":
		return launcher.Status(*manifest)
	case "logs":
		if *service == "" {
			return errors.New("logs requires --service NAME (use launcher for supervisor log)")
		}
		return launcher.Logs(*manifest, *service, *tail)
	case "run":
		return launcher.Run(*manifest)
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", command)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: tuba-launcher <validate|merge-manifest|start|stop|restart|status|logs> [--manifest PATH]")
	fmt.Fprintln(os.Stderr, "       tuba-launcher merge-manifest --manifest OLD --previous-defaults OLD-EXAMPLE --defaults NEW-EXAMPLE --output CANDIDATE --current-path CURRENT --release-path RELEASE")
	fmt.Fprintln(os.Stderr, "       tuba-launcher logs --service NAME [--tail N] [--manifest PATH]")
}
