package detection

import (
	"testing"
	"time"
	"tuba/product/internal/event"
)

func authEvent(id string, minute int, outcome, organization string) event.Authentication {
	e := event.Authentication{Timestamp: time.Date(2026, 9, 23, 2, minute, 0, 0, time.UTC).Format(time.RFC3339)}
	e.Organization.ID = organization
	e.Event.ID, e.Event.Outcome, e.User.ID = id, outcome, "u1"
	e.UEBA.Quality.Status = "qualified"
	return e
}

func TestFailureThenSuccess(t *testing.T) {
	events := []event.Authentication{authEvent("s", 5, "success", "tenant_a")}
	for i := 0; i < 5; i++ {
		events = append(events, authEvent(string(rune('a'+i)), i, "failure", "tenant_a"))
	}
	got, err := FailureThenSuccess(events, "tenant_a", 5, 30*time.Minute)
	if err != nil || len(got) != 1 {
		t.Fatalf("want one anomaly: %v, %v", got, err)
	}
	if got[0].Source["evidence"].(map[string]any)["count"] != 6 {
		t.Fatal("missing evidence")
	}
	if _, err := FailureThenSuccess(append(events, authEvent("foreign", 6, "failure", "tenant_b")), "tenant_a", 5, 30*time.Minute); err == nil {
		t.Fatal("foreign tenant accepted")
	}
}

func TestTimestampOffsetsUseActualTimeOrder(t *testing.T) {
	failure := authEvent("f", 0, "failure", "tenant_a")
	failure.Timestamp = "2026-09-23T10:00:00+08:00"
	success := authEvent("s", 1, "success", "tenant_a")
	success.Timestamp = "2026-09-23T02:01:00Z"
	got, err := FailureThenSuccess([]event.Authentication{success, failure}, "tenant_a", 1, 30*time.Minute)
	if err != nil || len(got) != 1 {
		t.Fatalf("timezone order failed: %v, %v", got, err)
	}
}
