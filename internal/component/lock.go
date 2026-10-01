package component

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"tuba/product/internal/launcher"
)

const lockFileName = "agent.lock"

// instanceLockContent binds the lock file to a specific process instance, not
// merely a PID, so a lock surviving a host reboot or a PID reuse cannot block
// the next supervisor forever — same fail-closed identity semantics as the
// launcher state file.
type instanceLockContent struct {
	PID      int       `json:"pid"`
	Identity string    `json:"identity"`
	Acquired time.Time `json:"acquired_at"`
}

// InstanceLock enforces one management agent per host (per root). The lock
// file lives at <root>/<lockFileName>.
type InstanceLock struct {
	path     string
	content  instanceLockContent
	acquired bool
}

// AcquireInstanceLock takes the host-wide agent lock. A live holder fails
// closed; a stale holder (dead PID, rebooted host, reused PID, or a legacy
// PID-only lock that cannot be verified) is replaced, since waiting on an
// unverifiable holder would deadlock recovery while launching a second agent
// over a live one is prevented by the identity check itself.
func AcquireInstanceLock(root string) (*InstanceLock, error) {
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, fmt.Errorf("create supervisor root: %w", err)
	}
	path := filepath.Join(root, lockFileName)
	existing, readErr := readLock(path)
	if readErr == nil && launcher.ProcessInstanceAlive(existing.PID, existing.Identity) {
		return nil, fmt.Errorf("another management agent (pid %d) holds the instance lock", existing.PID)
	}
	if readErr == nil || !os.IsNotExist(readErr) {
		// Stale (dead PID, previous boot, reused PID) or unparseable: a lock
		// that cannot be verified cannot belong to a live agent of this build.
		// Read errors other than a corrupt/dead lock (e.g. permissions) fail
		// closed below at lock creation.
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("remove stale instance lock: %w", err)
		}
	}
	identity, err := launcher.ProcessIdentity(os.Getpid())
	if err != nil || identity == "" {
		if err == nil {
			err = errors.New("process identity is empty")
		}
		return nil, fmt.Errorf("identify agent process: %w", err)
	}
	content := instanceLockContent{PID: os.Getpid(), Identity: identity, Acquired: time.Now().UTC()}
	data, err := json.Marshal(content)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		if os.IsExist(err) {
			return nil, errors.New("another management agent acquired the instance lock concurrently")
		}
		return nil, fmt.Errorf("create instance lock: %w", err)
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		file.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("write instance lock: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return nil, fmt.Errorf("write instance lock: %w", err)
	}
	return &InstanceLock{path: path, content: content, acquired: true}, nil
}

// Release removes the lock, but only if it still belongs to this process
// instance; it never removes another agent's lock.
func (l *InstanceLock) Release() error {
	if !l.acquired {
		return nil
	}
	l.acquired = false
	existing, err := readLock(l.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read instance lock before release: %w", err)
	}
	if existing.PID != l.content.PID || existing.Identity != l.content.Identity {
		return fmt.Errorf("instance lock is now held by pid %d; refusing to remove it", existing.PID)
	}
	return os.Remove(l.path)
}

// Held reports whether the lock file is still ours (guard against silent
// replacement by a recovering agent that treated us as stale).
func (l *InstanceLock) Held() bool {
	if !l.acquired {
		return false
	}
	existing, err := readLock(l.path)
	return err == nil && existing.PID == l.content.PID && existing.Identity == l.content.Identity
}

func readLock(path string) (instanceLockContent, error) {
	var content instanceLockContent
	data, err := os.ReadFile(path)
	if err != nil {
		return instanceLockContent{}, err
	}
	if err := json.Unmarshal(data, &content); err != nil {
		return instanceLockContent{}, fmt.Errorf("decode instance lock: %w", err)
	}
	return content, nil
}
