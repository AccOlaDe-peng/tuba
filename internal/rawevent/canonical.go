package rawevent

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
)

// CanonicalPayloadHash hashes the payload's canonical JSON form rather than its
// raw bytes.
//
// Beats build the event from a Go map and encode it on the way out, so the same
// record can arrive with its fields in a different order on every re-delivery —
// observed in practice as a re-read after a restart producing identical content
// and different bytes. Hashing raw bytes turns that into a content conflict, so
// the digest is taken over a key-sorted re-encoding instead. A genuinely changed
// value still hashes differently, which is what the conflict check is for.
//
// Numbers decode as json.Number so a record id or a Zeek timestamp keeps its
// exact literal instead of rounding through float64 into a neighbouring value.
func CanonicalPayloadHash(payload []byte) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return "", errors.New("payload must be valid JSON")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return "", errors.New("payload must be a single JSON value")
	}
	withoutCollectorMetadata(value)
	// encoding/json emits map keys in sorted order, which is what makes this
	// representation stable across re-deliveries.
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", errors.New("payload could not be re-encoded")
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

// collectorMetadata lists the paths the digest skips. Each one is bookkeeping the
// collector attaches rather than event content, and each one legitimately
// changes for an unchanged record: a rotated log is read once from its live path
// and once from the archive copy it was written to, and a restarted collector
// mints a new agent.ephemeral_id. The fields remain in the stored payload as
// evidence, so this narrows the digest rather than the record.
var collectorMetadata = [][]string{
	{"agent", "ephemeral_id"},
	{"event", "created"},
	{"log", "file", "path"},
	{"log", "file", "inode"},
	{"log", "file", "device_id"},
}

func withoutCollectorMetadata(value any) {
	object, ok := value.(map[string]any)
	if !ok {
		return
	}
	for _, path := range collectorMetadata {
		owner := object
		for _, key := range path[:len(path)-1] {
			next, ok := owner[key].(map[string]any)
			if !ok {
				owner = nil
				break
			}
			owner = next
		}
		if owner != nil {
			delete(owner, path[len(path)-1])
		}
	}
}
