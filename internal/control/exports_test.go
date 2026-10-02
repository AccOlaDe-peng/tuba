package control

import (
	"strings"
	"testing"
	"time"

	"tuba/product/internal/auth"
)

func TestExportPermissionFingerprint(t *testing.T) {
	base := auth.Principal{Subject: "u1", Roles: []string{"analyst"}}
	same := auth.Principal{Subject: "u1", Roles: []string{"analyst"}}
	otherSubject := auth.Principal{Subject: "u2", Roles: []string{"analyst"}}
	viewer := auth.Principal{Subject: "u1", Roles: []string{"viewer"}}
	admin := auth.Principal{Subject: "u1", Roles: []string{"tenant_admin"}}

	if ExportPermissionFingerprint(base) != ExportPermissionFingerprint(same) {
		t.Fatal("same permissions must fingerprint identically")
	}
	for name, p := range map[string]auth.Principal{"subject": otherSubject, "sensitive": viewer, "raw": admin} {
		if ExportPermissionFingerprint(base) == ExportPermissionFingerprint(p) {
			t.Fatalf("fingerprint must change with %s", name)
		}
	}
	if len(ExportPermissionFingerprint(base)) != 64 {
		t.Fatal("fingerprint must be a sha256 hex string")
	}
}

func TestValidateExportInput(t *testing.T) {
	valid := CreateExportInput{
		Dataset: "authentication", Format: ExportFormatNDJSON, Query: "search authentication",
		Generation: "g1", From: time.Now().Add(-time.Hour), To: time.Now(),
		RowLimit: 100, ByteLimit: 1024,
	}
	if err := validateExportInput(valid); err != nil {
		t.Fatalf("valid input rejected: %v", err)
	}
	cases := map[string]func(*CreateExportInput){
		"format":     func(in *CreateExportInput) { in.Format = "xml" },
		"empty query": func(in *CreateExportInput) { in.Query = "" },
		"long query":  func(in *CreateExportInput) { in.Query = strings.Repeat("x", 4097) },
		"range":       func(in *CreateExportInput) { in.From = in.To },
		"rows zero":   func(in *CreateExportInput) { in.RowLimit = 0 },
		"rows max":    func(in *CreateExportInput) { in.RowLimit = ExportMaxRows + 1 },
		"bytes zero":  func(in *CreateExportInput) { in.ByteLimit = 0 },
		"bytes max":   func(in *CreateExportInput) { in.ByteLimit = ExportMaxBytes + 1 },
		"generation":  func(in *CreateExportInput) { in.Generation = "" },
	}
	for name, mutate := range cases {
		in := valid
		mutate(&in)
		if err := validateExportInput(in); err == nil {
			t.Fatalf("%s: invalid input accepted", name)
		}
	}
}

func TestExportCursorRoundTrip(t *testing.T) {
	now := time.Date(2026, 10, 12, 1, 2, 3, 4, time.UTC)
	cursor := exportCursor(now, "3f6b6d2e-1234-4abc-8def-0123456789ab")
	t2, id, err := parseExportCursor(cursor)
	if err != nil || !t2.Equal(now) || id != "3f6b6d2e-1234-4abc-8def-0123456789ab" {
		t.Fatalf("round trip failed: %v %v %v", t2, id, err)
	}
	for _, bad := range []string{"", "!!!", "aGVsbG8="} {
		if _, _, err := parseExportCursor(bad); err == nil {
			t.Fatalf("bad cursor %q accepted", bad)
		}
	}
}
