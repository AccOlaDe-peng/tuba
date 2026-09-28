package rawevent

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestEnvelopeJSONMarshalPreservesExactPayloadBytes(t *testing.T) {
	payload := []byte(`{ "path": "<zeek>&", "nested": { "b": 2, "a": 1 } }`)
	envelope, err := New(TrustedSource{
		OrganizationID: "org-test", Namespace: "zeek-test", SourceInstanceID: "source-1",
		SourceEpoch: "epoch-1", SourceContextID: "ctx_50000000000000000000000000000005",
		VendorName: "zeek", VendorProduct: "zeek", VendorDataset: "zeek.conn", ReleaseID: "release-1",
	}, "kafka-v1:tuba.source.ctx_50000000000000000000000000000005.v1:0:42", payload,
		time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Envelope
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.Payload, payload) {
		t.Fatalf("payload bytes changed during envelope marshal: got %q, want %q", decoded.Payload, payload)
	}
	if err := Validate(decoded, "org-test", "zeek-test"); err != nil {
		t.Fatalf("marshaled envelope does not validate: %v", err)
	}
}
