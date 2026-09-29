package uim

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"tuba/product/internal/rawevent"
)

func zeekRaw(t *testing.T, dataset string, payload map[string]any) rawevent.Envelope {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	event, err := rawevent.New(rawevent.TrustedSource{
		OrganizationID: "tenant_a", Namespace: "tenant_a", SourceInstanceID: "zeek-01",
		SourceContextID: "ctx_0123456789abcdef0123456789abcdef", SourceEpoch: "epoch-1",
		VendorName: "zeek", VendorProduct: "zeek", VendorDataset: "zeek." + dataset,
		ReleaseID: "zeek-network-v1",
	}, "file:conn.log:offset:1", body, time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func TestNormalizeZeekDatasetsToUIM(t *testing.T) {
	tests := []struct {
		name, domain, semantic string
		payload                map[string]any
		assert                 func(*testing.T, map[string]any)
	}{
		{
			name: "conn", domain: "network", semantic: "network.connection",
			payload: map[string]any{"ts": 1790416800.25, "uid": "C-test", "id.orig_h": "192.0.2.10", "id.resp_h": "198.51.100.20", "id.orig_p": 54321, "id.resp_p": 443, "proto": "TCP", "service": "ssl", "duration": 1.75, "orig_bytes": 100, "resp_bytes": 200, "orig_pkts": 2, "resp_pkts": 3},
			assert: func(t *testing.T, event map[string]any) {
				if got := object(event["network"])["transport"]; got != "tcp" {
					t.Fatalf("network.transport=%v", got)
				}
				if got := object(event["network"])["protocol"]; got != "ssl" {
					t.Fatalf("network.protocol=%v", got)
				}
				if got := object(event["network"])["bytes"]; got != int64(300) {
					t.Fatalf("network.bytes=%v", got)
				}
				if got := object(event["network"])["packets"]; got != int64(5) {
					t.Fatalf("network.packets=%v", got)
				}
				if got := object(event["source"])["ip"]; got != "192.0.2.10" {
					t.Fatalf("source.ip=%v", got)
				}
				if got := object(event["source"])["port"]; got != 54321 {
					t.Fatalf("source.port=%v", got)
				}
			},
		},
		{
			name: "dns", domain: "dns", semantic: "dns.response",
			payload: map[string]any{"ts": 1790416800.25, "uid": "C-dns", "id.orig_h": "192.0.2.10", "id.resp_h": "192.0.2.53", "proto": "udp", "query": "example.test", "rcode_name": "NOERROR"},
			assert: func(t *testing.T, event map[string]any) {
				if got := object(object(event["dns"])["question"])["name"]; got != "example.test" {
					t.Fatalf("dns.question.name=%v", got)
				}
				if got := object(event["dns"])["response_code"]; got != "NOERROR" {
					t.Fatalf("dns.response_code=%v", got)
				}
			},
		},
		{
			name: "http", domain: "web", semantic: "web.request",
			payload: map[string]any{"ts": 1790416800.25, "uid": "C-http", "id.orig_h": "192.0.2.10", "id.resp_h": "198.51.100.80", "proto": "tcp", "host": "example.test", "uri": "/login", "method": "GET", "status_code": 200},
			assert: func(t *testing.T, event map[string]any) {
				if got := object(event["url"])["domain"]; got != "example.test" {
					t.Fatalf("url.domain=%v", got)
				}
				if got := object(object(event["http"])["response"])["status_code"]; got != 200 {
					t.Fatalf("http.response.status_code=%v", got)
				}
			},
		},
		{
			name: "ssl", domain: "tls", semantic: "tls.session",
			payload: map[string]any{"ts": 1790416800.25, "uid": "C-ssl", "id.orig_h": "192.0.2.10", "id.resp_h": "198.51.100.44", "proto": "tcp", "server_name": "example.test", "version": "TLSv13", "cipher": "TLS_AES_128_GCM_SHA256"},
			assert: func(t *testing.T, event map[string]any) {
				if got := object(event["destination"])["domain"]; got != "example.test" {
					t.Fatalf("destination.domain=%v", got)
				}
				if got := object(event["tls"])["version"]; got != "TLSv13" {
					t.Fatalf("tls.version=%v", got)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event, err := Normalize(zeekRaw(t, test.name, test.payload))
			if err != nil {
				t.Fatal(err)
			}
			if got := stringValue(object(event["ueba"].(map[string]any)["route"])["domain"]); got != test.domain {
				t.Fatalf("route.domain=%q, want %q", got, test.domain)
			}
			if got := stringValue(object(event["ueba"].(map[string]any)["event"])["type"]); got != test.semantic {
				t.Fatalf("semantic type=%q, want %q", got, test.semantic)
			}
			if object(event["ueba"].(map[string]any)["quality"])["status"] != "qualified" {
				t.Fatalf("event should be qualified: %+v", event["ueba"])
			}
			if object(event["ueba"].(map[string]any)["provenance"])["raw_event_id"] != zeekRaw(t, test.name, test.payload).RawEventID {
				t.Fatal("raw event provenance was not preserved")
			}
			test.assert(t, event)
		})
	}
}

func TestNormalizeZeekMissingSessionIDIsPartial(t *testing.T) {
	event, err := Normalize(zeekRaw(t, "conn", map[string]any{
		"ts": 1790416800.25, "id.orig_h": "192.0.2.10", "id.resp_h": "198.51.100.20", "proto": "tcp",
	}))
	if err != nil {
		t.Fatal(err)
	}
	quality := object(object(event["ueba"])["quality"])
	if quality["status"] != "partial" || len(stringValues(quality["reasons"])) != 1 {
		t.Fatalf("missing uid should be explicitly partial: %+v", quality)
	}
}

func TestNormalizeZeekUnsupportedDataset(t *testing.T) {
	_, err := Normalize(zeekRaw(t, "weird", map[string]any{"ts": 1790416800.25}))
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("err=%v, want ErrUnsupported", err)
	}
}

func TestNormalizeZeekPayloadCannotOverrideTrustedDataset(t *testing.T) {
	_, err := Normalize(zeekRaw(t, "conn", map[string]any{
		"_path": "dns", "ts": 1790416800.25, "uid": "C-test", "id.orig_h": "192.0.2.10",
		"id.resp_h": "192.0.2.53", "proto": "udp", "query": "example.test",
	}))
	if err == nil || err.Error() != "DIP_ZEEK_DATASET_MISMATCH" {
		t.Fatalf("err=%v, want trusted dataset mismatch", err)
	}
}

func TestNormalizeZeekBeatUsesOriginalRecord(t *testing.T) {
	original := `{"ts":1790416800.25,"uid":"C-beat","id.orig_h":"192.0.2.10","id.resp_h":"198.51.100.20","proto":"tcp"}`
	raw := zeekRaw(t, "conn", map[string]any{
		"agent":   map[string]any{"type": "filebeat", "version": "8.19.0", "id": "beat-21"},
		"event":   map[string]any{"dataset": "zeek.conn", "original": original},
		"message": original,
		"ts":      1790416800.25, "id.orig_h": "203.0.113.99", "id.resp_h": "203.0.113.100",
	})
	event, err := Normalize(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got := object(event["source"])["ip"]; got != "192.0.2.10" {
		t.Fatalf("source.ip=%v, want address from original Zeek record", got)
	}
	if got := object(event["destination"])["ip"]; got != "198.51.100.20" {
		t.Fatalf("destination.ip=%v, want address from original Zeek record", got)
	}
}

func TestNormalizeZeekBeatRejectsMismatchedDatasetAndEvidence(t *testing.T) {
	original := `{"ts":1790416800.25,"uid":"C-beat","id.orig_h":"192.0.2.10","id.resp_h":"198.51.100.20"}`
	base := func() map[string]any {
		return map[string]any{
			"agent":   map[string]any{"type": "filebeat"},
			"event":   map[string]any{"dataset": "zeek.conn", "original": original},
			"message": original,
		}
	}
	for _, test := range []struct {
		name, want string
		change     func(map[string]any)
	}{
		{"dataset", "DIP_ZEEK_DATASET_MISMATCH", func(p map[string]any) { p["event"].(map[string]any)["dataset"] = "zeek.dns" }},
		{"original", "DIP_ZEEK_ORIGINAL_MISMATCH", func(p map[string]any) { p["message"] = `{"ts":0}` }},
		{"missing", "DIP_ZEEK_ORIGINAL_MISSING", func(p map[string]any) { delete(p, "message") }},
		{"invalid", "DIP_ZEEK_ORIGINAL_INVALID", func(p map[string]any) { p["event"].(map[string]any)["original"] = "{"; p["message"] = "{" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := base()
			test.change(payload)
			_, err := Normalize(zeekRaw(t, "conn", payload))
			if err == nil || err.Error() != test.want {
				t.Fatalf("err=%v, want %s", err, test.want)
			}
		})
	}
}

func TestNormalizeWindowsSecurityCollectorEventScope(t *testing.T) {
	tests := []struct {
		id, route, action string
	}{
		{"4624", "authentication", "logon-success"}, {"4625", "authentication", "logon-failure"},
		{"4634", "session", "logoff"}, {"4647", "session", "logoff"},
		{"4648", "authentication", "explicit-credentials-use"}, {"4672", "authentication", "special-privileges-assigned"},
		{"4719", "iam", "audit-policy-change"}, {"4720", "iam", "user-create"}, {"4722", "iam", "user-enable"},
		{"4723", "iam", "password-change"}, {"4724", "iam", "password-reset"}, {"4725", "iam", "user-disable"},
		{"4726", "iam", "user-delete"}, {"4728", "directory", "group-member-add"},
		{"4729", "directory", "group-member-remove"}, {"4732", "directory", "group-member-add"}, {"4733", "directory", "group-member-remove"},
		{"4756", "directory", "group-member-add"}, {"4757", "directory", "group-member-remove"},
		{"4768", "authentication", "credential-validation"}, {"4769", "authentication", "credential-validation"},
		{"4771", "authentication", "kerberos-preauth-failure"}, {"4776", "authentication", "credential-validation"},
		{"1102", "iam", "security-log-cleared"},
	}
	for _, test := range tests {
		t.Run(test.id, func(t *testing.T) {
			provider := "Microsoft-Windows-Security-Auditing"
			if test.id == "1102" {
				provider = "Microsoft-Windows-Eventlog"
			}
			payload := map[string]any{
				"@timestamp": "2026-09-29T03:14:15.1234567Z",
				"event":      map[string]any{"dataset": "windows.security", "original": "<Event/>"},
				"winlog": map[string]any{
					"provider_name": provider, "channel": "Security", "event_id": test.id,
					"computer_name": "WIN-139", "record_id": "12345",
					"event_data": map[string]any{"SubjectUserName": "operator", "SubjectUserSid": "S-1-5-21-1000", "TargetUserName": "target", "TargetUserSid": "S-1-5-21-2000", "Status": "0x0", "IpAddress": "192.0.2.5"},
				},
			}
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := rawevent.New(rawevent.TrustedSource{
				OrganizationID: "tenant_a", Namespace: "tenant_a", SourceInstanceID: "windows-139",
				SourceContextID: "ctx_0123456789abcdef0123456789abcdef", SourceEpoch: "epoch-1",
				VendorName: "Microsoft", VendorProduct: "windows", VendorDataset: "windows.security", ReleaseID: "windows-security-v1",
			}, "winlog:"+test.id+":12345", body, time.Date(2026, 9, 29, 3, 14, 15, 123456700, time.UTC))
			if err != nil {
				t.Fatal(err)
			}
			event, err := Normalize(raw)
			if err != nil {
				t.Fatal(err)
			}
			if got := stringValue(object(object(event["ueba"])["route"])["domain"]); got != test.route {
				t.Fatalf("route.domain=%q want %q", got, test.route)
			}
			if got := stringValue(object(event["event"])["action"]); got != test.action {
				t.Fatalf("event.action=%q want %q", got, test.action)
			}
			if got := object(object(event["ueba"])["quality"])["status"]; got != "qualified" {
				t.Fatalf("quality.status=%v", got)
			}
		})
	}
}
