package collector

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct{ db *sql.DB }

type Item struct {
	ID                            int64
	SourceID, ContextID, Position string
	StreamID                      string
	Sequence                      int64
	Payload                       []byte
	Attempts                      int
}

func OpenStore(path string) (*Store, error) {
	if path == ":memory:" || strings.HasPrefix(path, "file:") {
		return nil, errors.New("collector spool must use a private filesystem path")
	}
	if err := ensurePrivateSQLiteFile(path); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{"PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL", "PRAGMA foreign_keys=ON", "PRAGMA busy_timeout=5000"} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, err
		}
	}
	ddl := `CREATE TABLE IF NOT EXISTS streams (
	 source_id TEXT NOT NULL, stream_id TEXT NOT NULL, read_cursor TEXT NOT NULL DEFAULT '',
	 ack_watermark INTEGER NOT NULL DEFAULT 0, next_seq INTEGER NOT NULL DEFAULT 1,
	 PRIMARY KEY(source_id,stream_id));
	CREATE TABLE IF NOT EXISTS queue_items (
	 id INTEGER PRIMARY KEY AUTOINCREMENT, source_id TEXT NOT NULL, stream_id TEXT NOT NULL,
	 seq INTEGER NOT NULL, context_id TEXT NOT NULL, source_position TEXT NOT NULL,
	 payload BLOB NOT NULL, payload_hash TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
	 attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at INTEGER NOT NULL DEFAULT 0,
	 last_error TEXT NOT NULL DEFAULT '', receipt_id TEXT NOT NULL DEFAULT '',
	 UNIQUE(source_id,stream_id,source_position));
	CREATE INDEX IF NOT EXISTS queue_pending_idx ON queue_items(state,next_attempt_at,id);
	CREATE TABLE IF NOT EXISTS filter_counts (
	 source_id TEXT NOT NULL, rule_version TEXT NOT NULL, reason TEXT NOT NULL,
	 window_start TEXT NOT NULL, count INTEGER NOT NULL DEFAULT 0,
	 PRIMARY KEY(source_id,rule_version,reason,window_start));
	CREATE TABLE IF NOT EXISTS ack_ranges (
	 source_id TEXT NOT NULL, stream_id TEXT NOT NULL, start_seq INTEGER NOT NULL, end_seq INTEGER NOT NULL,
	 PRIMARY KEY(source_id,stream_id,start_seq));`
	if _, err := db.Exec(ddl); err != nil {
		db.Close()
		return nil, err
	}
	if err := ensurePrivateSQLiteFiles(path); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func ensurePrivateSQLiteFile(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Chmod(path, 0600)
}

func ensurePrivateSQLiteFiles(path string) error {
	for _, name := range []string{path, path + "-wal", path + "-shm"} {
		if err := os.Chmod(name, 0600); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (s *Store) Cursor(ctx context.Context, sourceID, streamID string) (string, error) {
	_, err := s.db.ExecContext(ctx, `INSERT INTO streams(source_id,stream_id) VALUES(?,?) ON CONFLICT DO NOTHING`, sourceID, streamID)
	if err != nil {
		return "", err
	}
	var cursor string
	err = s.db.QueryRowContext(ctx, `SELECT read_cursor FROM streams WHERE source_id=? AND stream_id=?`, sourceID, streamID).Scan(&cursor)
	return cursor, err
}

// Enqueue persists a payload and its next read cursor atomically.
func (s *Store) Enqueue(ctx context.Context, sourceID, streamID, contextID, position, nextCursor, hash string, payload []byte, filterVersion, filterReason string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO streams(source_id,stream_id) VALUES(?,?) ON CONFLICT DO NOTHING`, sourceID, streamID); err != nil {
		return err
	}
	var existingHash string
	err = tx.QueryRowContext(ctx, `SELECT payload_hash FROM queue_items WHERE source_id=? AND stream_id=? AND source_position=?`, sourceID, streamID, position).Scan(&existingHash)
	if err == nil {
		if existingHash != hash {
			return errors.New("source position payload conflict")
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	} else {
		var seq int64
		if err = tx.QueryRowContext(ctx, `SELECT next_seq FROM streams WHERE source_id=? AND stream_id=?`, sourceID, streamID).Scan(&seq); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO queue_items(source_id,stream_id,seq,context_id,source_position,payload,payload_hash) VALUES(?,?,?,?,?,?,?)`, sourceID, streamID, seq, contextID, position, payload, hash); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE streams SET next_seq=next_seq+1 WHERE source_id=? AND stream_id=?`, sourceID, streamID); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE streams SET read_cursor=? WHERE source_id=? AND stream_id=?`, nextCursor, sourceID, streamID); err != nil {
		return err
	}
	if filterReason != "" {
		window := time.Now().UTC().Truncate(time.Minute).Format(time.RFC3339)
		if _, err = tx.ExecContext(ctx, `INSERT INTO filter_counts(source_id,rule_version,reason,window_start,count) VALUES(?,?,?,?,1) ON CONFLICT(source_id,rule_version,reason,window_start) DO UPDATE SET count=count+1`, sourceID, filterVersion, filterReason, window); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// DropFiltered advances the cursor and records the aggregate counter in one transaction.
func (s *Store) DropFiltered(ctx context.Context, sourceID, streamID, nextCursor, version, reason string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO streams(source_id,stream_id) VALUES(?,?) ON CONFLICT DO NOTHING`, sourceID, streamID); err != nil {
		return err
	}
	window := time.Now().UTC().Truncate(time.Minute).Format(time.RFC3339)
	if _, err = tx.ExecContext(ctx, `INSERT INTO filter_counts(source_id,rule_version,reason,window_start,count) VALUES(?,?,?,?,1) ON CONFLICT(source_id,rule_version,reason,window_start) DO UPDATE SET count=count+1`, sourceID, version, reason, window); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE streams SET read_cursor=? WHERE source_id=? AND stream_id=?`, nextCursor, sourceID, streamID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) SetReadCursor(ctx context.Context, sourceID, streamID, cursor string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO streams(source_id,stream_id,read_cursor) VALUES(?,?,?) ON CONFLICT(source_id,stream_id) DO UPDATE SET read_cursor=excluded.read_cursor`, sourceID, streamID, cursor)
	return err
}

func CursorDecode(value string, target any) error {
	if value == "" {
		return sql.ErrNoRows
	}
	return json.Unmarshal([]byte(value), target)
}

func (s *Store) Next(ctx context.Context) (Item, error) {
	var it Item
	err := s.db.QueryRowContext(ctx, `SELECT id,source_id,stream_id,seq,context_id,source_position,payload,attempts FROM queue_items WHERE state='pending' AND next_attempt_at<=? ORDER BY id LIMIT 1`, time.Now().UTC().UnixNano()).Scan(&it.ID, &it.SourceID, &it.StreamID, &it.Sequence, &it.ContextID, &it.Position, &it.Payload, &it.Attempts)
	return it, err
}

func (s *Store) Ack(ctx context.Context, id int64, receipt string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var source, stream string
	var seq int64
	if err = tx.QueryRowContext(ctx, `SELECT source_id,stream_id,seq FROM queue_items WHERE id=?`, id).Scan(&source, &stream, &seq); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE queue_items SET state='acked',receipt_id=?,payload=x'' WHERE id=? AND state='pending'`, receipt, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO ack_ranges(source_id,stream_id,start_seq,end_seq) VALUES(?,?,?,?) ON CONFLICT(source_id,stream_id,start_seq) DO NOTHING`, source, stream, seq, seq); err != nil {
		return err
	}
	for {
		var watermark int64
		if err = tx.QueryRowContext(ctx, `SELECT ack_watermark FROM streams WHERE source_id=? AND stream_id=?`, source, stream).Scan(&watermark); err != nil {
			return err
		}
		var end int64
		err = tx.QueryRowContext(ctx, `SELECT end_seq FROM ack_ranges WHERE source_id=? AND stream_id=? AND start_seq<=? ORDER BY start_seq DESC LIMIT 1`, source, stream, watermark+1).Scan(&end)
		if errors.Is(err, sql.ErrNoRows) {
			break
		}
		if err != nil {
			return err
		}
		if end <= watermark {
			break
		}
		if _, err = tx.ExecContext(ctx, `UPDATE streams SET ack_watermark=? WHERE source_id=? AND stream_id=?`, end, source, stream); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM ack_ranges WHERE source_id=? AND stream_id=? AND end_seq<=?`, source, stream, end); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) Retry(ctx context.Context, id int64, attempts int, delay time.Duration, cause error) error {
	_, err := s.db.ExecContext(ctx, `UPDATE queue_items SET attempts=?,next_attempt_at=?,last_error=? WHERE id=? AND state='pending'`, attempts, time.Now().UTC().Add(delay).UnixNano(), cause.Error(), id)
	return err
}

func (s *Store) Reject(ctx context.Context, id int64, cause error) error {
	_, err := s.db.ExecContext(ctx, `UPDATE queue_items SET state='rejected',last_error=? WHERE id=? AND state='pending'`, cause.Error(), id)
	return err
}

func (s *Store) Close() error { return s.db.Close() }
