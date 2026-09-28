//go:build windows

package launcher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

const detachedProcess = 0x00000008
const newConsole = 0x00000010
const stillActive = 259
const ctrlBreakEvent = 1

var kernel32 = windows.NewLazySystemDLL("kernel32.dll")
var attachConsoleProc = kernel32.NewProc("AttachConsole")
var freeConsoleProc = kernel32.NewProc("FreeConsole")
var getConsoleWindowProc = kernel32.NewProc("GetConsoleWindow")
var consoleAttachMu sync.Mutex

func configureDetached(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP | detachedProcess, HideWindow: true}
}

func configureChild(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP | newConsole,
		HideWindow:    true,
	}
}

func terminateChild(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return os.ErrProcessDone
	}
	consoleAttachMu.Lock()
	defer consoleAttachMu.Unlock()

	parentPID := uint32(os.Getppid())
	restoreParentConsole := hasConsole()
	if freeConsole() {
		restoreParentConsole = true
	}
	restore := func() {
		freeConsole()
		if restoreParentConsole && parentPID > 0 {
			_ = attachConsole(parentPID)
		}
	}
	pid := uint32(cmd.Process.Pid)
	if err := attachConsole(pid); err != nil {
		restore()
		if !processAlive(int(pid)) {
			return os.ErrProcessDone
		}
		return fmt.Errorf("attach to child console: %w", err)
	}
	err := windows.GenerateConsoleCtrlEvent(ctrlBreakEvent, pid)
	restore()
	return err
}

func attachConsole(pid uint32) error {
	result, _, callErr := attachConsoleProc.Call(uintptr(pid))
	if result != 0 {
		return nil
	}
	if callErr != syscall.Errno(0) {
		return callErr
	}
	return syscall.EINVAL
}

func freeConsole() bool {
	result, _, _ := freeConsoleProc.Call()
	return result != 0
}

func hasConsole() bool {
	window, _, _ := getConsoleWindowProc.Call()
	return window != 0
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	handle, err := syscall.OpenProcess(syscall.PROCESS_QUERY_INFORMATION, false, uint32(pid))
	if err != nil {
		return false
	}
	defer syscall.CloseHandle(handle)
	var code uint32
	if syscall.GetExitCodeProcess(handle, &code) != nil {
		return false
	}
	return code == stillActive
}

func processIdentity(pid int) (string, error) {
	if pid <= 0 {
		return "", errors.New("invalid process ID")
	}
	handle, err := syscall.OpenProcess(syscall.PROCESS_QUERY_INFORMATION, false, uint32(pid))
	if err != nil {
		return "", err
	}
	defer syscall.CloseHandle(handle)
	var creation, exit, kernel, user syscall.Filetime
	if err := syscall.GetProcessTimes(handle, &creation, &exit, &kernel, &user); err != nil {
		return "", err
	}
	identity := uint64(creation.HighDateTime)<<32 | uint64(creation.LowDateTime)
	return fmt.Sprintf("%016x", identity), nil
}

func signalNotifyContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt)
}

func checkSecretFilePermissions(path string, _ os.FileInfo) error {
	descriptor, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil || descriptor == nil {
		return errors.New("environment_file ACL could not be verified")
	}
	acl, _, err := descriptor.DACL()
	if err != nil || acl == nil {
		return errors.New("environment_file ACL is missing or permissive")
	}
	for _, sidType := range []windows.WELL_KNOWN_SID_TYPE{windows.WinWorldSid, windows.WinAuthenticatedUserSid, windows.WinBuiltinUsersSid} {
		broadSID, sidErr := windows.CreateWellKnownSid(sidType)
		if sidErr != nil {
			return errors.New("environment_file ACL could not be verified")
		}
		for i := uint32(0); i < uint32(acl.AceCount); i++ {
			var ace *windows.ACCESS_ALLOWED_ACE
			if err := windows.GetAce(acl, i, &ace); err != nil {
				return errors.New("environment_file ACL could not be verified")
			}
			if ace.Header.AceType == windows.ACCESS_ALLOWED_ACE_TYPE {
				aceSID := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
				if aceSID.Equals(broadSID) {
					return errors.New("environment_file ACL must not grant access to Everyone, Authenticated Users, or Users")
				}
			}
		}
	}
	return nil
}

func securePrivateDirectory(path string) error {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return errors.New("launcher account SID could not be resolved")
	}
	permissions := "*" + user.User.Sid.String() + ":(OI)(CI)F"
	command := exec.Command("icacls.exe", path, "/inheritance:r", "/grant:r", permissions,
		"*S-1-5-18:(OI)(CI)F", "*S-1-5-32-544:(OI)(CI)F")
	if _, err := command.CombinedOutput(); err != nil {
		return errors.New("could not restrict launcher directory ACL")
	}
	return nil
}
