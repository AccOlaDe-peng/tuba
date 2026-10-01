// tuba-component is the managed component supervisor CLI (COL-09). It manages
// signed component packages under a supervisor root: verify, apply (with
// state-format compatibility checks), confirm after the health observation
// window, and guarded rollback. The same subcommands run identically on
// Windows and Linux; nothing here registers systemd units or Windows
// Services. Process supervision of the components themselves stays with
// tuba-launcher.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"tuba/product/internal/component"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "tuba-component:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: tuba-component <verify|apply|confirm|rollback|upgrade|status|stamp-format> [flags]")
	}
	command, rest := args[0], args[1:]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	root := flags.String("root", "", "supervisor root directory (components/, data/ live below it)")
	keyringPath := flags.String("keyring", "", "JSON keyring of trusted ed25519 signing keys")
	packageDir := flags.String("package", "", "component package directory containing manifest.json")
	componentName := flags.String("component", "", "component name (defaults to the package manifest)")
	instances := flags.String("instance", "", "comma-separated data instance names whose state format must stay readable")
	formatVersion := flags.Int("format", 0, "state format version to stamp")
	manifestPath := flags.String("manifest", "", "launcher manifest supervising the component (upgrade command)")
	serviceName := flags.String("service", "", "launcher service name of the component (upgrade command)")
	healthURL := flags.String("health-url", "", "readiness endpoint polled during the observation window (upgrade command)")
	observe := flags.Duration("observe", 5*time.Minute, "health observation window before confirm")
	grace := flags.Duration("grace", time.Minute, "time the component may take to become healthy after start")
	poll := flags.Duration("poll", 2*time.Second, "health/liveness poll interval")
	if err := flags.Parse(rest); err != nil {
		return err
	}
	if *root == "" {
		return errors.New("--root is required")
	}
	upgrader := &component.Upgrader{Root: *root}
	needKeyring := map[string]bool{"verify": true, "apply": true, "confirm": true, "rollback": true, "upgrade": true}
	if needKeyring[command] {
		if *keyringPath == "" {
			return errors.New("--keyring is required")
		}
		ring, err := component.LoadKeyring(*keyringPath)
		if err != nil {
			return err
		}
		upgrader.Keyring = ring
	}
	instanceList := splitNames(*instances)
	switch command {
	case "verify":
		if *packageDir == "" {
			return errors.New("--package is required")
		}
		manifest, err := component.LoadManifest(*packageDir, upgrader.Keyring)
		if err != nil {
			return err
		}
		fmt.Printf("package valid: %s %s (%s/%s), state format %d (reads >= %d), %d files\n",
			manifest.Component, manifest.Version, manifest.OS, manifest.Architecture,
			manifest.StateFormat.Version, manifest.StateFormat.MinReadable, len(manifest.Files))
		return nil
	case "apply":
		if *packageDir == "" {
			return errors.New("--package is required")
		}
		result, err := upgrader.Apply(*packageDir, *componentName, instanceList)
		if err != nil {
			return err
		}
		fmt.Printf("applied %s: %s -> %s (pending health confirmation)\n", result.Component, printable(result.FromVersion), result.ToVersion)
		return nil
	case "upgrade":
		if *packageDir == "" {
			return errors.New("--package is required")
		}
		if *manifestPath == "" || *serviceName == "" || *healthURL == "" {
			return errors.New("upgrade requires --manifest, --service and --health-url")
		}
		orchestrator := &component.Orchestrator{
			Upgrader:      upgrader,
			Processes:     &component.LauncherProcessManager{ManifestPath: *manifestPath, Service: *serviceName},
			Health:        &component.HTTPHealthChecker{URL: *healthURL},
			ObserveWindow: *observe,
			StartGrace:    *grace,
			PollInterval:  *poll,
		}
		err := orchestrator.Upgrade(context.Background(), *packageDir, *componentName, instanceList)
		var rolledBack *component.RolledBackError
		switch {
		case err == nil:
			fmt.Printf("upgraded %s and confirmed after %s observation\n", *componentName, *observe)
			return nil
		case errors.As(err, &rolledBack):
			fmt.Printf("upgrade of %s to %s failed health observation; rolled back to %s and recovered\n",
				rolledBack.Component, rolledBack.ToVersion, rolledBack.FromVersion)
			return err
		default:
			return err
		}
	case "confirm":
		if *componentName == "" {
			return errors.New("--component is required")
		}
		if err := upgrader.Confirm(*componentName, instanceList); err != nil {
			return err
		}
		fmt.Printf("confirmed %s\n", *componentName)
		return nil
	case "rollback":
		if *componentName == "" {
			return errors.New("--component is required")
		}
		result, err := upgrader.Rollback(*componentName, instanceList)
		if err != nil {
			return err
		}
		fmt.Printf("rolled back %s: %s -> %s\n", result.Component, result.FromVersion, result.ToVersion)
		return nil
	case "status":
		if *componentName == "" {
			return errors.New("--component is required")
		}
		status, err := upgrader.Status(*componentName)
		if err != nil {
			return err
		}
		data, err := json.MarshalIndent(status, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	case "stamp-format":
		if *componentName == "" || *instances == "" || *formatVersion < 1 {
			return errors.New("--component, --instance and --format >= 1 are required")
		}
		for _, instance := range instanceList {
			if err := component.StampStateFormat(*root, *componentName, instance, *formatVersion); err != nil {
				return err
			}
		}
		fmt.Printf("stamped %s instance(s) at format %d\n", *componentName, *formatVersion)
		return nil
	default:
		return fmt.Errorf("unknown command %q", command)
	}
}

func splitNames(value string) []string {
	if value == "" {
		return nil
	}
	var names []string
	for _, name := range strings.Split(value, ",") {
		if name = strings.TrimSpace(name); name != "" {
			names = append(names, name)
		}
	}
	return names
}

func printable(version string) string {
	if version == "" {
		return "(none)"
	}
	return version
}
