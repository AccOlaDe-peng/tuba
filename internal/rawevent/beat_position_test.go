package rawevent

import "testing"

func TestStableBeatPositionUsesFilestreamCoordinatesAcrossDeliveries(t *testing.T) {
	body := []byte(`{"log":{"file":{"device_id":"2053","inode":"8926348","path":"/logs/conn.log"},"offset":10118640}}`)
	first := StableBeatPosition(body, "kafka-v1:topic:0:99")
	retry := StableBeatPosition(body, "kafka-v1:topic:0:140")
	want := "filebeat-v1:2053:8926348:10118640"
	if first != want || retry != want {
		t.Fatalf("positions = %q and %q, want stable %q", first, retry, want)
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
