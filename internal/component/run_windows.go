//go:build windows

package component

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
)

// execExitCode carries a child exit code so the caller can mirror it.
type execExitCode int

func (e execExitCode) Error() string { return fmt.Sprintf("component exited with code %d", int(e)) }

// ExecCurrent runs the active component binary as a child with inherited
// stdio and mirrors its exit code. Windows has no exec(3); the launcher
// delivers Ctrl-Break to the process group, which reaches the child, and the
// launcher's WaitDelay bounds any leftovers.
func ExecCurrent(root, componentName, relBin string, args []string) error {
	dir, binary, err := ResolveCurrentBinary(root, componentName, relBin)
	if err != nil {
		return err
	}
	cmd := exec.Command(binary, ResolveArgs(dir, args)...)
	cmd.Stdout, cmd.Stderr, cmd.Stdin = os.Stdout, os.Stderr, os.Stdin
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return execExitCode(exitErr.ExitCode())
		}
		return err
	}
	return nil
}

// ExitCode extracts a mirrored child exit code from an ExecCurrent error.
func ExitCode(err error) (int, bool) {
	var code execExitCode
	if errors.As(err, &code) {
		return int(code), true
	}
	return 0, false
}
