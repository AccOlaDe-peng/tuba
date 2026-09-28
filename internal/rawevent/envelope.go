// Package rawevent defines the trusted envelope accepted at the ingestion boundary.
package rawevent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const MaxPayloadBytes = 1 << 20

type Envelope struct {
	SchemaVersion    string          `json:"schema_version"`
	RawEventID       string          `json:"raw_event_id"`
	Organization     Organization    `json:"organization"`
	Namespace        string          `json:"namespace"`
	SourceInstanceID string          `json:"source_instance_id"`
	SourceContextID  string          `json:"source_context_id"`
	SourcePosition   string          `json:"source_position"`
	SourceEpoch      string          `json:"source_epoch"`
	Vendor           Vendor          `json:"vendor"`
	ReceivedAt       time.Time       `json:"received_at"`
	ReleaseID        string          `json:"release_id,omitempty"`
	PayloadHash      string          `json:"payload_hash"`
	Payload          json.RawMessage `json:"payload"`
	Encoding         string          `json:"encoding"`
}

// MarshalJSON embeds Payload without encoding/json compacting or HTML-escaping it.
// The payload digest and Raw ES evidence are defined over the exact bytes received
// by ingest, so a default json.Marshal of json.RawMessage is not sufficient.
func (e Envelope) MarshalJSON() ([]byte, error) {
	type metadata struct {
		SchemaVersion    string       `json:"schema_version"`
		RawEventID       string       `json:"raw_event_id"`
		Organization     Organization `json:"organization"`
		Namespace        string       `json:"namespace"`
		SourceInstanceID string       `json:"source_instance_id"`
		SourceContextID  string       `json:"source_context_id"`
		SourcePosition   string       `json:"source_position"`
		SourceEpoch      string       `json:"source_epoch"`
		Vendor           Vendor       `json:"vendor"`
		ReceivedAt       time.Time    `json:"received_at"`
		ReleaseID        string       `json:"release_id,omitempty"`
		PayloadHash      string       `json:"payload_hash"`
		Encoding         string       `json:"encoding"`
	}
	if len(e.Payload) == 0 || len(e.Payload) > MaxPayloadBytes || !json.Valid(e.Payload) {
		return nil, errors.New("raw payload must be valid JSON")
	}
	head, err := json.Marshal(metadata{
		SchemaVersion: e.SchemaVersion, RawEventID: e.RawEventID, Organization: e.Organization,
		Namespace: e.Namespace, SourceInstanceID: e.SourceInstanceID, SourceContextID: e.SourceContextID,
		SourcePosition: e.SourcePosition, SourceEpoch: e.SourceEpoch, Vendor: e.Vendor,
		ReceivedAt: e.ReceivedAt, ReleaseID: e.ReleaseID, PayloadHash: e.PayloadHash, Encoding: e.Encoding,
	})
	if err != nil {
		return nil, err
	}
	head = head[:len(head)-1]
	head = append(head, `,"payload":`...)
	head = append(head, e.Payload...)
	head = append(head, '}')
	return head, nil
}

// Marshal returns the envelope JSON directly. Calling json.Marshal on Envelope
// would compact and HTML-escape the returned custom JSON, altering Payload bytes.
func Marshal(e Envelope) ([]byte, error) {
	return e.MarshalJSON()
}

// Validate rejects envelopes whose claimed tenant, source position, ID, or payload digest changed in transit.
func Validate(e Envelope, organizationID, namespace string) error {
	if e.SchemaVersion != "1.0.0" || e.Organization.ID != organizationID || e.Namespace != namespace {
		return errors.New("raw envelope contract or tenant scope mismatch")
	}
	if !safeScopeID(e.Organization.ID) || !safeScopeID(e.Namespace) || len(e.SourceInstanceID) == 0 || len(e.SourceInstanceID) > 128 ||
		!validContextID(e.SourceContextID) || len(e.SourcePosition) == 0 || len(e.SourcePosition) > 512 || len(e.SourceEpoch) == 0 || len(e.SourceEpoch) > 128 ||
		len(e.Vendor.Name) == 0 || len(e.Vendor.Name) > 128 || len(e.Vendor.Product) == 0 || len(e.Vendor.Product) > 128 || len(e.Vendor.Dataset) == 0 || len(e.Vendor.Dataset) > 128 ||
		len(e.ReleaseID) == 0 || len(e.ReleaseID) > 128 || e.ReceivedAt.IsZero() {
		return errors.New("raw envelope is missing or exceeds source lineage limits")
	}
	var object map[string]json.RawMessage
	if len(e.Payload) == 0 || len(e.Payload) > MaxPayloadBytes || json.Unmarshal(e.Payload, &object) != nil || object == nil || e.Encoding != "json" {
		return errors.New("raw payload is invalid")
	}
	wantID := StableID(e.Organization.ID, e.SourceInstanceID, e.Vendor.Dataset, e.SourceEpoch, e.SourcePosition)
	if e.RawEventID != wantID {
		return errors.New("raw_event_id does not match source position")
	}
	digest := sha256.Sum256(e.Payload)
	if e.PayloadHash != hex.EncodeToString(digest[:]) {
		return errors.New("payload_hash mismatch")
	}
	return nil
}

type Vendor struct {
	Name    string `json:"name"`
	Product string `json:"product"`
	Dataset string `json:"dataset"`
}

type Organization struct {
	ID string `json:"id"`
}

type TrustedSource struct {
	OrganizationID, Namespace, SourceInstanceID, SourceEpoch string
	SourceContextID                                          string
	VendorName, VendorProduct, VendorDataset, ReleaseID      string
}

// New constructs an envelope using deployment-bound source metadata, never payload claims.
func New(src TrustedSource, position string, payload []byte, now time.Time) (Envelope, error) {
	if len(payload) == 0 || len(payload) > MaxPayloadBytes {
		return Envelope{}, errors.New("payload must be 1 byte to 1 MiB")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(payload, &object); err != nil || object == nil {
		return Envelope{}, errors.New("payload must be a JSON object")
	}
	if !safeScopeID(src.OrganizationID) || !safeScopeID(src.Namespace) || src.SourceInstanceID == "" || len(src.SourceInstanceID) > 128 || !validContextID(src.SourceContextID) ||
		src.SourceEpoch == "" || len(src.SourceEpoch) > 128 || len(position) == 0 || len(position) > 512 ||
		src.VendorName == "" || len(src.VendorName) > 128 || src.VendorProduct == "" || len(src.VendorProduct) > 128 || src.VendorDataset == "" || len(src.VendorDataset) > 128 ||
		src.ReleaseID == "" || len(src.ReleaseID) > 128 {
		return Envelope{}, errors.New("trusted source identity and source position are required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	id := StableID(src.OrganizationID, src.SourceInstanceID, src.VendorDataset, src.SourceEpoch, position)
	payloadCopy := append(json.RawMessage(nil), payload...)
	hash := sha256.Sum256(payloadCopy)
	return Envelope{
		SchemaVersion: "1.0.0", RawEventID: id, Organization: Organization{ID: src.OrganizationID}, Namespace: src.Namespace,
		SourceInstanceID: src.SourceInstanceID, SourceContextID: src.SourceContextID, SourcePosition: position, SourceEpoch: src.SourceEpoch,
		Vendor:     Vendor{Name: src.VendorName, Product: src.VendorProduct, Dataset: src.VendorDataset},
		ReceivedAt: now.UTC(), ReleaseID: src.ReleaseID, PayloadHash: hex.EncodeToString(hash[:]),
		Payload: payloadCopy, Encoding: "json",
	}, nil
}

func safeScopeID(value string) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}

func validContextID(value string) bool {
	if len(value) != len("ctx_")+32 || !strings.HasPrefix(value, "ctx_") || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "ctx_"))
	return err == nil
}

// StableID uses length-delimited fields to avoid ambiguous concatenations.
func StableID(organization, source, dataset, epoch, position string) string {
	fields := []string{"raw-v1", organization, source, dataset, epoch, position}
	h := sha256.New()
	for _, field := range fields {
		_, _ = fmt.Fprintf(h, "%d:", len(field))
		_, _ = h.Write([]byte(field))
	}
	return "raw:" + hex.EncodeToString(h.Sum(nil))
}
