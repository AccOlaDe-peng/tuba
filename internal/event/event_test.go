package event

import "testing"

const valid = `{"@timestamp":"2026-09-23T02:00:00Z","organization":{"id":"tenant_a"},"event":{"id":"event-1","kind":"event","category":["authentication"],"action":"logon","outcome":"failure","dataset":"authentication"},"vendor":{"dataset":"windows.security"},"ueba":{"schema":{"version":"1.0.0"},"quality":{"status":"qualified"},"route":{"domain":"authentication"}},"user":{"id":"u1"}}`

func TestParse(t *testing.T) {
	e, err := Parse([]byte(valid), "tenant_a")
	if err != nil || e.Event.ID != "event-1" {
		t.Fatalf("valid event rejected: %v", err)
	}
	if _, err := Parse([]byte(valid), "tenant_b"); err == nil {
		t.Fatal("cross-tenant event accepted")
	}
	if _, err := Parse([]byte(valid+` {}`), "tenant_a"); err == nil {
		t.Fatal("trailing JSON accepted")
	}
}
