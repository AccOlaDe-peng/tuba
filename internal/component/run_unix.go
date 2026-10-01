//go:build !windows

package component

import (
	"os"
	"syscall"
)

// ExecCurrent replaces the current process with the active component binary,
// preserving the PID so the launcher's process tracking stays exact.
func ExecCurrent(root, componentName, relBin string, args []string) error {
	dir, binary, err := ResolveCurrentBinary(root, componentName, relBin)
	if err != nil {
		return err
	}
	argv := append([]string{binary}, ResolveArgs(dir, args)...)
	return syscall.Exec(binary, argv, os.Environ())
}

// ExitCode mirrors the Windows helper; exec(3) never returns a child exit
// code, so this is always false on Unix.
func ExitCode(error) (int, bool) { return 0, false }
