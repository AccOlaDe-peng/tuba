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
	// encoding/json emits map keys in sorted order, which is what makes this
	// representation stable across re-deliveries.
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", errors.New("payload could not be re-encoded")
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}
