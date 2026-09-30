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
//
// What canonicalization deliberately normalises, and what it therefore stops
// distinguishing: key order, insignificant whitespace, and unicode escaping
// ("é" vs "é"). It also collapses duplicate keys, because decoding into a
// map keeps the last one -- {"a":1,"a":2} hashes as {"a":2}. A collector emits
// none of these differences, which is why the digest is taken this way, but a
// source that produced them would no longer be flagged as a content change: the
// stored payload still carries the original bytes, so the difference remains
// visible in evidence even though the digest no longer reacts to it.
//
// The excluded paths below and the drop_fields list in
// deploy/components/windows-security-winlogbeat.example.yml overlap but are not
// copies: this list is what makes the digest stable for every collector, while
// the Winlogbeat list additionally keeps those fields out of the stored payload.
// A new collector does not have to edit this list to be correct, only to stop
// seeing conflicts on whatever metadata it varies between deliveries.
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
