package launcher

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	serviceLogMaxBytes int64 = 16 << 20
	serviceLogBackups        = 3
)

// rollingLog bounds each managed service log without relying on an OS service
// manager or an external logrotate process. Writes from stdout and stderr may
// arrive concurrently, so file rotation and writes are serialized.
type rollingLog struct {
	mu        sync.Mutex
	path      string
	file      *os.File
	size      int64
	maxBytes  int64
	maxBackup int
}

func openRollingLog(path string, maxBytes int64, maxBackup int) (*rollingLog, error) {
	if maxBytes <= 0 || maxBackup < 0 {
		return nil, fmt.Errorf("invalid rolling log limits")
	}
	log := &rollingLog{path: path, maxBytes: maxBytes, maxBackup: maxBackup}
	info, err := os.Stat(path)
	if err == nil && info.Size() >= maxBytes {
		if err := log.rotate(); err != nil {
			return nil, err
		}
	} else if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("stat service log: %w", err)
	}
	if err := log.open(); err != nil {
		return nil, err
	}
	return log, nil
}

func rotateExistingLogIfNeeded(path string, maxBytes int64, maxBackup int) error {
	info, err := os.Stat(path)
	if os.IsNotExist(err) || (err == nil && info.Size() < maxBytes) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat launcher log: %w", err)
	}
	log := &rollingLog{path: path, maxBytes: maxBytes, maxBackup: maxBackup}
	return log.rotate()
}

func (l *rollingLog) open() error {
	file, err := os.OpenFile(l.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("open service log: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("stat service log: %w", err)
	}
	l.file = file
	l.size = info.Size()
	return nil
}

func (l *rollingLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return 0, os.ErrClosed
	}
	if l.size > 0 && l.size+int64(len(p)) > l.maxBytes {
		if err := l.file.Close(); err != nil {
			l.file = nil
			return 0, fmt.Errorf("close service log before rotation: %w", err)
		}
		l.file = nil
		if err := l.rotate(); err != nil {
			return 0, err
		}
		if err := l.open(); err != nil {
			return 0, err
		}
	}
	n, err := l.file.Write(p)
	l.size += int64(n)
	return n, err
}

func (l *rollingLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

func (l *rollingLog) rotate() error {
	if l.maxBackup == 0 {
		if err := os.Remove(l.path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove service log before rotation: %w", err)
		}
		return nil
	}
	oldest := fmt.Sprintf("%s.%d", l.path, l.maxBackup)
	if err := os.Remove(oldest); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove oldest service log: %w", err)
	}
	for i := l.maxBackup - 1; i >= 1; i-- {
		from := fmt.Sprintf("%s.%d", l.path, i)
		to := fmt.Sprintf("%s.%d", l.path, i+1)
		if err := os.Rename(from, to); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("rotate service log backup: %w", err)
		}
	}
	if err := os.Rename(l.path, filepath.Clean(l.path)+".1"); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("rotate active service log: %w", err)
	}
	return nil
}
