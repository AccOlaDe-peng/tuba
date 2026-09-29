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

// The same record legitimately arrives from two places: a rotated log is read
// once while it was live and again from its archive copy, and the collector
// records which file it read from. Those fields describe the transport, not the
// event, so hashing them reports a content conflict for identical content. The
// fields stay in the stored payload; only the digest ignores them.
func TestCanonicalPayloadHashIgnoresCollectorLocationMetadata(t *testing.T) {
	live := []byte(`{"ts":1,"uid":"a","log":{"file":{"path":"/opt/zeek/spool/zeek/conn.log","inode":"1592008","device_id":"2053"},"offset":42}}`)
	archived := []byte(`{"ts":1,"uid":"a","log":{"file":{"path":"/archive/conn/conn.01.log","inode":"3941775","device_id":"2053"},"offset":42}}`)

	liveHash, err := CanonicalPayloadHash(live)
	if err != nil {
		t.Fatal(err)
	}
	archivedHash, err := CanonicalPayloadHash(archived)
	if err != nil {
		t.Fatal(err)
	}
	if liveHash != archivedHash {
		t.Fatalf("the same record read from two paths hashed differently:\n  %s\n  %s", liveHash, archivedHash)
	}
}

// agent.ephemeral_id is minted per process start and event.created records when
// the record was processed, so a re-read after a restart differs in both while
// the event itself is unchanged.
func TestCanonicalPayloadHashIgnoresProcessInstanceMetadata(t *testing.T) {
	first := []byte(`{"ts":1,"uid":"a","agent":{"id":"x","ephemeral_id":"aaa"},"event":{"created":"2026-09-30T01:00:00Z"}}`)
	second := []byte(`{"ts":1,"uid":"a","agent":{"id":"x","ephemeral_id":"bbb"},"event":{"created":"2026-09-30T02:00:00Z"}}`)

	firstHash, err := CanonicalPayloadHash(first)
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := CanonicalPayloadHash(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatalf("process instance metadata changed the hash:\n  %s\n  %s", firstHash, secondHash)
	}
}

// Excluding metadata must not blunt the conflict check: the values that say
// which record this is still have to hash differently when they change.
func TestCanonicalPayloadHashStillDetectsContentChanges(t *testing.T) {
	base, err := CanonicalPayloadHash([]byte(`{"ts":1,"uid":"a","log":{"file":{"path":"/p","inode":"1"},"offset":42}}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, changed := range [][]byte{
		[]byte(`{"ts":1,"uid":"b","log":{"file":{"path":"/p","inode":"1"},"offset":42}}`),
		[]byte(`{"ts":1,"uid":"a","log":{"file":{"path":"/p","inode":"1"},"offset":43}}`),
		[]byte(`{"ts":2,"uid":"a","log":{"file":{"path":"/p","inode":"1"},"offset":42}}`),
	} {
		hash, err := CanonicalPayloadHash(changed)
		if err != nil {
			t.Fatal(err)
		}
		if hash == base {
			t.Fatalf("a changed value hashed the same as the original: %s", changed)
		}
	}
}

// Only the exact known paths are excluded. A field that merely shares a name
// with excluded metadata is event content and must still count.
func TestCanonicalPayloadHashOnlyExcludesKnownMetadataPaths(t *testing.T) {
	pairs := [][2][]byte{
		{[]byte(`{"created":"a","ts":1}`), []byte(`{"created":"b","ts":1}`)},
		{[]byte(`{"agent":{"id":"1"},"ts":1}`), []byte(`{"agent":{"id":"2"},"ts":1}`)},
		{[]byte(`{"event":{"dataset":"a"},"ts":1}`), []byte(`{"event":{"dataset":"b"},"ts":1}`)},
		{[]byte(`{"log":{"offset":1},"ts":1}`), []byte(`{"log":{"offset":2},"ts":1}`)},
	}
	for _, pair := range pairs {
		first, err := CanonicalPayloadHash(pair[0])
		if err != nil {
			t.Fatal(err)
		}
		second, err := CanonicalPayloadHash(pair[1])
		if err != nil {
			t.Fatal(err)
		}
		if first == second {
			t.Fatalf("%s and %s hashed the same; only known metadata paths may be excluded", pair[0], pair[1])
		}
	}
}
