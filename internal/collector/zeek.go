package collector

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"tuba/product/internal/rawevent"
)

type ZeekSource struct {
	ID        string       `json:"source_instance_id"`
	ContextID string       `json:"source_context_id"`
	StreamID  string       `json:"stream_id"`
	Pattern   string       `json:"path_glob"`
	Filter    FilterPolicy `json:"filter"`
}

type fileCursor struct {
	Offset int64 `json:"offset"`
}

func (s ZeekSource) Poll(ctx context.Context, store *Store) error {
	paths, err := filepath.Glob(s.Pattern)
	if err != nil {
		return err
	}
	sort.Strings(paths)
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.readFile(ctx, store, path); err != nil {
			return err
		}
	}
	return nil
}

func (s ZeekSource) readFile(ctx context.Context, store *Store, path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return nil
	}
	gen, err := fileGeneration(info, path)
	if err != nil {
		return err
	}
	stream := s.StreamID + ":" + gen
	encoded, err := store.Cursor(ctx, s.ID, stream)
	if err != nil {
		return err
	}
	var cur fileCursor
	if encoded != "" {
		if err := CursorDecode(encoded, &cur); err != nil {
			return fmt.Errorf("invalid persisted Zeek cursor for %s: %w", stream, err)
		}
	}
	if info.Size() < cur.Offset {
		return fmt.Errorf("source gap: %s shrank from offset %d to %d", path, cur.Offset, info.Size())
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Seek(cur.Offset, io.SeekStart); err != nil {
		return err
	}
	reader := bufio.NewReaderSize(f, 64*1024)
	offset := cur.Offset
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		line, readErr := readBoundedLine(reader, rawevent.MaxPayloadBytes+2)
		if errors.Is(readErr, errLineTooLong) {
			return fmt.Errorf("Zeek line exceeds ingest limit at %s:%d", path, offset)
		}
		if len(line) > 0 && line[len(line)-1] == '\n' {
			end := offset + int64(len(line))
			payload := bytesTrimLine(line)
			if len(payload) > rawevent.MaxPayloadBytes {
				return fmt.Errorf("Zeek line exceeds ingest limit at %s:%d", path, offset)
			}
			if !json.Valid(payload) {
				return fmt.Errorf("invalid Zeek JSON line at %s:%d", path, offset)
			}
			next, _ := json.Marshal(fileCursor{Offset: end})
			drop, reason, filterErr := s.Filter.Evaluate(payload)
			if filterErr != nil {
				return filterErr
			}
			if drop {
				if err := store.DropFiltered(ctx, s.ID, stream, string(next), s.Filter.Version, reason); err != nil {
					return err
				}
			} else {
				position := fmt.Sprintf("file:%s:%s:%d", s.StreamID, gen, offset)
				hash := sha256.Sum256(payload)
				if err := store.Enqueue(ctx, s.ID, stream, s.ContextID, position, string(next), hex.EncodeToString(hash[:]), payload, s.Filter.Version, reason); err != nil {
					return err
				}
			}
			offset = end
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return readErr
		}
	}
}

var errLineTooLong = errors.New("line exceeds configured limit")

func readBoundedLine(r *bufio.Reader, limit int) ([]byte, error) {
	var line []byte
	for {
		part, err := r.ReadSlice('\n')
		if len(line)+len(part) > limit {
			return nil, errLineTooLong
		}
		line = append(line, part...)
		if err == bufio.ErrBufferFull {
			continue
		}
		return line, err
	}
}

func bytesTrimLine(b []byte) []byte {
	b = b[:len(b)-1]
	if len(b) > 0 && b[len(b)-1] == '\r' {
		b = b[:len(b)-1]
	}
	return b
}

func fileGeneration(info os.FileInfo, path string) (string, error) {
	v := reflect.ValueOf(info.Sys())
	if !v.IsValid() {
		return "", errors.New("file identity unavailable")
	}
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	dev, ino := v.FieldByName("Dev"), v.FieldByName("Ino")
	if dev.IsValid() && ino.IsValid() {
		return fmt.Sprintf("%x-%x", dev.Uint(), ino.Uint()), nil
	}
	// Platforms without device/inode fields use a stable per-path fallback and cannot detect inode reuse.
	name := strings.ToLower(filepath.Clean(path))
	sum := sha256.Sum256([]byte(name))
	return hex.EncodeToString(sum[:8]), nil
}
