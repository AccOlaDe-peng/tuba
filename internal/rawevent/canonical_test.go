package rawevent

import "testing"

// Beats build the event from a Go map and encode it on the way out, so the same
// record can arrive with its fields in a different order on every re-delivery.
// Hashing the raw bytes would then report a content conflict for content that is
// identical, which is what turns a harmless re-read into a rejection.
func TestCanonicalPayloadHashIgnoresFieldOrder(t *testing.T) {
	first := []byte(`{"@timestamp":"2026-09-29T10:00:00Z","winlog":{"record_id":7,"channel":"Security"},"agent":{"id":"a"}}`)
	reordered := []byte(`{"agent":{"id":"a"},"winlog":{"channel":"Security","record_id":7},"@timestamp":"2026-09-29T10:00:00Z"}`)

	firstHash, err := CanonicalPayloadHash(first)
	if err != nil {
		t.Fatal(err)
	}
	reorderedHash, err := CanonicalPayloadHash(reordered)
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != reorderedHash {
		t.Fatalf("reordered fields hashed differently:\n  %s\n  %s", firstHash, reorderedHash)
	}
}

// The conflict check exists so a source position cannot be rewritten with
// different content. A changed value has to keep hashing differently or the
// check stops detecting anything.
func TestCanonicalPayloadHashDetectsContentChange(t *testing.T) {
	first, err := CanonicalPayloadHash([]byte(`{"@timestamp":"2026-09-29T10:00:00Z","winlog":{"record_id":7}}`))
	if err != nil {
		t.Fatal(err)
	}
	changed, err := CanonicalPayloadHash([]byte(`{"@timestamp":"2026-09-29T10:00:00Z","winlog":{"record_id":8}}`))
	if err != nil {
		t.Fatal(err)
	}
	if first == changed {
		t.Fatal("a changed record id hashed the same")
	}
}

// Values must reach the canonical form untouched. Decoding into float64 would
// round record ids and Zeek timestamps, letting two distinct records collide.
func TestCanonicalPayloadHashPreservesNumbers(t *testing.T) {
	first, err := CanonicalPayloadHash([]byte(`{"record_id":77914,"ts":1790647193.850051}`))
	if err != nil {
		t.Fatal(err)
	}
	reordered, err := CanonicalPayloadHash([]byte(`{"ts":1790647193.850051,"record_id":77914}`))
	if err != nil {
		t.Fatal(err)
	}
	if first != reordered {
		t.Fatal("reordered numeric fields hashed differently")
	}
	differing, err := CanonicalPayloadHash([]byte(`{"record_id":77914,"ts":1790647193.8500510000001}`))
	if err != nil {
		t.Fatal(err)
	}
	if differing == first {
		t.Fatal("number precision was lost: a distinct value hashed the same")
	}
}

func TestCanonicalPayloadHashRejectsNonJSON(t *testing.T) {
	if _, err := CanonicalPayloadHash([]byte(`not json`)); err == nil {
		t.Fatal("non-JSON payload was accepted")
	}
}
