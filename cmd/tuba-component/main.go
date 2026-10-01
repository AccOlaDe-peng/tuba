// tuba-component is the managed component supervisor CLI (COL-09). It manages
// signed component packages under a supervisor root: verify, apply (with
// state-format compatibility checks), confirm after the health observation
// window, and guarded rollback. The same subcommands run identically on
// Windows and Linux; nothing here registers systemd units or Windows
// Services. Process supervision of the components themselves stays with
// tuba-launcher.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

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
		return errors.New("usage: tuba-component <verify|apply|confirm|rollback|status|stamp-format> [flags]")
	}
	command, rest := args[0], args[1:]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	root := flags.String("root", "", "supervisor root directory (components/, data/ live below it)")
	keyringPath := flags.String("keyring", "", "JSON keyring of trusted ed25519 signing keys")
	packageDir := flags.String("package", "", "component package directory containing manifest.json")
	componentName := flags.String("component", "", "component name (defaults to the package manifest)")
	instances := flags.String("instance", "", "comma-separated data instance names whose state format must stay readable")
	formatVersion := flags.Int("format", 0, "state format version to stamp")
	if err := flags.Parse(rest); err != nil {
		return err
	}
	if *root == "" {
		return errors.New("--root is required")
	}
	upgrader := &component.Upgrader{Root: *root}
	needKeyring := map[string]bool{"verify": true, "apply": true, "confirm": true, "rollback": true}
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
