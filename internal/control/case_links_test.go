package control

import (
	"strings"
	"testing"
)

var (
	validEntityID     = "ent:" + strings.Repeat("a1", 32)
	validContribution = "rc:" + strings.Repeat("b2", 32)
)

func TestValidateLinkShape(t *testing.T) {
	cases := []struct {
		name                      string
		linkType, refKind, target string
		wantErr                   bool
	}{
		{"entity ok", "entity", "entity", validEntityID, false},
		{"entity bad ref kind", "entity", "event_id", validEntityID, true},
		{"entity bad format", "entity", "entity", "account-1", true},
		{"entity short hash", "entity", "entity", "ent:abcd", true},
		{"contribution ok", "risk_contribution", "risk_contribution", validContribution, false},
		{"contribution wrong kind", "risk_contribution", "entity", validContribution, true},
		{"contribution bad format", "risk_contribution", "risk_contribution", "rc:xyz", true},
		{"evidence event_id", "evidence", "event_id", "evt-123", false},
		{"evidence raw_event_id", "evidence", "raw_event_id", "raw-abc", false},
		{"evidence attribution", "evidence", "attribution", "att:deadbeef", false},
		{"evidence job_id", "evidence", "job_id", "f47ac10b-58cc-4372-a567-0e02b2c3d479", false},
		{"evidence bad kind", "evidence", "entity", validEntityID, true},
		{"unknown link type", "document", "entity", validEntityID, true},
		{"empty target", "evidence", "event_id", "", true},
		{"oversized target", "evidence", "event_id", strings.Repeat("x", 257), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateLinkShape(tc.linkType, tc.refKind, tc.target)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateLinkShape(%q,%q,...) err=%v wantErr=%v", tc.linkType, tc.refKind, err, tc.wantErr)
			}
		})
	}
}
