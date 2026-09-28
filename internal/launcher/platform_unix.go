//go:build !windows

package launcher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
)

func configureDetached(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}

func configureChild(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func terminateChild(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return os.ErrProcessDone
	}
	err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	if errors.Is(err, syscall.ESRCH) {
		return os.ErrProcessDone
	}
	return err
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func processIdentity(pid int) (string, error) {
	bootID, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", err
	}
	closingCommand := strings.LastIndexByte(string(stat), ')')
	if closingCommand < 0 {
		return "", errors.New("process stat has no command terminator")
	}
	fields := strings.Fields(string(stat[closingCommand+1:]))
	// The remainder begins at field 3 (state); field 22 is starttime.
	if len(fields) <= 19 {
		return "", errors.New("process stat is missing its start time")
	}
	return strings.TrimSpace(string(bootID)) + ":" + fields[19], nil
}

func signalNotifyContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func checkSecretFilePermissions(_ string, info os.FileInfo) error {
	if info.Mode().Perm()&0077 != 0 {
		return errors.New("environment_file permissions must deny all group and other access")
	}
	return nil
}

func securePrivateDirectory(path string) error {
	if err := os.Chmod(path, 0700); err != nil {
		return err
	}
	return nil
}
