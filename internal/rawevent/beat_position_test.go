package rawevent

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestStableBeatPositionUsesFilestreamCoordinatesAcrossDeliveries(t *testing.T) {
	body := []byte(`{"log":{"file":{"device_id":"2053","inode":"8926348","path":"/logs/conn.log"},"offset":10118640}}`)
	first := StableBeatPosition(body, "kafka-v1:topic:0:99")
	retry := StableBeatPosition(body, "kafka-v1:topic:0:140")
	want := "filebeat-v1:2053:8926348:10118640"
	if first != want || retry != want {
		t.Fatalf("positions = %q and %q, want stable %q", first, retry, want)
	}
}

// A cursor built from device and inode is not unique over time. A rotated log
// file can be deleted and its inode handed to an unrelated file, and then two
// different records claim the same position: the second is rejected as a
// conflict and never reaches the index. Filebeat's fingerprint identity hashes
// the file's own prefix, which survives inode reuse.
func TestStableBeatPositionPrefersFileFingerprintWhenPresent(t *testing.T) {
	body := []byte(`{"log":{"file":{"device_id":"2053","inode":"1592008","fingerprint":"3f9a2c","path":"/logs/conn.log"},"offset":0}}`)
	first := StableBeatPosition(body, "kafka-v1:topic:0:99")
	retry := StableBeatPosition(body, "kafka-v1:topic:0:140")
	want := "filebeat-v2:3f9a2c:0"
	if first != want || retry != want {
		t.Fatalf("positions = %q and %q, want stable %q", first, retry, want)
	}
}

// The observed failure: conn.log and dns.log both resolved to inode 1592008. The
// fingerprint has to tell them apart or one of them is dropped.
func TestStableBeatPositionSeparatesFilesSharingAnInode(t *testing.T) {
	conn := []byte(`{"log":{"file":{"device_id":"2053","inode":"1592008","fingerprint":"aaaa"},"offset":0}}`)
	dns := []byte(`{"log":{"file":{"device_id":"2053","inode":"1592008","fingerprint":"bbbb"},"offset":0}}`)
	if StableBeatPosition(conn, "kafka:1") == StableBeatPosition(dns, "kafka:2") {
		t.Fatal("files sharing an inode must receive distinct source positions")
	}
}

// A collector that has not been switched to fingerprint identity still has to
// produce stable positions, and a fingerprint that would break the position
// grammar must not be trusted.
func TestStableBeatPositionUsesInodeCoordinateWhenFingerprintIsUnusable(t *testing.T) {
	for _, body := range [][]byte{
		[]byte(`{"log":{"file":{"device_id":"2053","inode":"8926348"},"offset":10118640}}`),
		[]byte(`{"log":{"file":{"device_id":"2053","inode":"8926348","fingerprint":""},"offset":10118640}}`),
		[]byte(`{"log":{"file":{"device_id":"2053","inode":"8926348","fingerprint":"a:b"},"offset":10118640}}`),
	} {
		if got, want := StableBeatPosition(body, "kafka:1"), "filebeat-v1:2053:8926348:10118640"; got != want {
			t.Fatalf("position for %s = %q, want %q", body, got, want)
		}
	}
}

func TestStableBeatPositionFallsBackToDeliveryCoordinate(t *testing.T) {
	fallback := "kafka-v1:topic:2:42"
	for _, body := range [][]byte{
		[]byte(`{"message":"not a file event"}`),
		[]byte(`{"log":{"file":{"inode":"9"},"offset":12}}`),
		[]byte(`{"log":{"file":{"device_id":"d:1","inode":"9"},"offset":12}}`),
		[]byte(`{"log":{"file":{"device_id":"d","inode":"9"},"offset":-1}}`),
	} {
		if got := StableBeatPosition(body, fallback); got != fallback {
			t.Fatalf("position for %s = %q, want fallback %q", body, got, fallback)
		}
	}
}

func TestStableBeatPositionUsesWindowsRecordIdentityAcrossDeliveries(t *testing.T) {
	body := []byte(`{"@timestamp":"2026-09-29T03:14:15.1234567Z","winlog":{"computer_name":"WIN-139","channel":"Security","record_id":"928144","event_id":"4624"}}`)
	first := StableBeatPosition(body, "kafka-v1:topic:0:99")
	retry := StableBeatPosition(body, "kafka-v1:topic:0:140")
	want := "winlogbeat-v1:77696e2d313339:7365637572697479:928144:2026-09-29T03:14:15.1234567Z"
	if first != want || retry != want {
		t.Fatalf("positions = %q and %q, want stable %q", first, retry, want)
	}
}

func TestStableBeatPositionDistinguishesWindowsLogGenerationsByEventTime(t *testing.T) {
	first := []byte(`{"@timestamp":"2026-09-29T03:14:15Z","winlog":{"computer_name":"WIN-139","channel":"Security","record_id":1}}`)
	afterClear := []byte(`{"@timestamp":"2026-09-29T04:14:15Z","winlog":{"computer_name":"WIN-139","channel":"Security","record_id":1}}`)
	if StableBeatPosition(first, "kafka:1") == StableBeatPosition(afterClear, "kafka:2") {
		t.Fatal("record ID reused after a log clear must receive a distinct source position")
	}
}

func TestStableBeatPositionFallsBackForIncompleteWindowsCoordinates(t *testing.T) {
	fallback := "kafka-v1:topic:0:7"
	body := []byte(`{"@timestamp":"2026-09-29T03:14:15Z","winlog":{"computer_name":"WIN-139","channel":"Security"}}`)
	if got := StableBeatPosition(body, fallback); got != fallback {
		t.Fatalf("position = %q, want fallback %q", got, fallback)
	}
}

// The ingest decodes the payload once and uses the event-taking form; every
// other caller keeps passing bytes. The two must not drift, or the position
// that decides a raw event's identity would depend on which entry point ran.
func TestStableBeatPositionFromEventMatchesPayloadForm(t *testing.T) {
	bodies := [][]byte{
		[]byte(`{"log":{"file":{"device_id":"2053","inode":"8926348","fingerprint":"3f9a2c","path":"/logs/conn.log"},"offset":10118640}}`),
		[]byte(`{"log":{"file":{"device_id":"2053","inode":"8926348"},"offset":0}}`),
		[]byte(`{"@timestamp":"2026-09-29T03:14:15.1234567Z","winlog":{"computer_name":"WIN-139","channel":"Security","record_id":"928144"}}`),
		[]byte(`{"@timestamp":"2026-09-29T03:14:15Z","winlog":{"computer_name":"WIN-139","channel":"Security"}}`),
		[]byte(`{"message":"not a file event"}`),
		[]byte(`not json at all`),
	}
	const fallback = "kafka-v1:topic:0:7"
	for _, body := range bodies {
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.UseNumber()
		var event map[string]any
		if decoder.Decode(&event) != nil {
			event = nil
		}
		fromBytes := StableBeatPosition(body, fallback)
		fromEvent := StableBeatPositionFromEvent(event, fallback)
		if fromBytes != fromEvent {
			t.Fatalf("position differs by entry point for %s:\n  bytes=%q\n  event=%q", body, fromBytes, fromEvent)
		}
	}
}
