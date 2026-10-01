package entity

import (
	"strings"
	"testing"
)

func TestNormalizePositive(t *testing.T) {
	cases := []struct {
		name      string
		id        Identifier
		canonical string
		strength  Strength
	}{
		{"sid case folded", Identifier{KindSID, "S-1-5-21-100-200-300-1001"}, "s-1-5-21-100-200-300-1001", StrengthStrong},
		{"guid braces stripped", Identifier{KindGUID, "{A1B2C3D4-E5F6-7890-ABCD-EF1234567890}"}, "a1b2c3d4-e5f6-7890-abcd-ef1234567890", StrengthStrong},
		{"device uuid", Identifier{KindDeviceUUID, "A1B2C3D4-E5F6-7890-ABCD-EF1234567890"}, "a1b2c3d4-e5f6-7890-abcd-ef1234567890", StrengthStrong},
		{"agent id", Identifier{KindAgentID, "AGENT-0A1B2C3D"}, "agent-0a1b2c3d", StrengthStrong},
		{"ntname split", Identifier{KindNTName, `CORP\JSmith`}, `corp\jsmith`, StrengthWeak},
		{"upn domain lowered", Identifier{KindUPN, "JSmith@CORP.Example.COM"}, "jsmith@corp.example.com", StrengthWeak},
		{"email", Identifier{KindEmail, "JSmith@Corp.Example.com"}, "jsmith@corp.example.com", StrengthWeak},
		{"username", Identifier{KindUsername, " JSmith "}, "jsmith", StrengthWeak},
		{"hostname trailing dot", Identifier{KindHostname, "WEB01.Corp.Example."}, "web01.corp.example", StrengthWeak},
		{"ipv6 canonical", Identifier{KindIP, "2001:0DB8:0:0:0:0:0:1"}, "2001:db8::1", StrengthWeak},
		{"ipv4 leading zero rejected form kept canonical", Identifier{KindIP, "10.0.0.8"}, "10.0.0.8", StrengthWeak},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n, err := Normalize(tc.id)
			if err != nil {
				t.Fatalf("Normalize: %v", err)
			}
			if n.Canonical != tc.canonical {
				t.Errorf("canonical = %q, want %q", n.Canonical, tc.canonical)
			}
			if n.Strength != tc.strength {
				t.Errorf("strength = %q, want %q", n.Strength, tc.strength)
			}
			if n.Value != tc.id.Value {
				t.Errorf("original value not preserved: %q", n.Value)
			}
		})
	}
}

func TestNormalizeNegative(t *testing.T) {
	cases := []struct {
		name string
		id   Identifier
	}{
		{"unknown kind", Identifier{Kind("fingerprint"), "abc"}},
		{"empty", Identifier{KindSID, "   "}},
		{"sid garbage", Identifier{KindSID, "S-1-5-"}},
		{"sid no prefix", Identifier{KindSID, "1-5-21-100"}},
		{"guid short", Identifier{KindGUID, "a1b2c3d4-e5f6-7890"}},
		{"guid non-hex", Identifier{KindGUID, "zzzzzzzz-e5f6-7890-abcd-ef1234567890"}},
		{"agent id too short", Identifier{KindAgentID, "abc"}},
		{"ntname no domain", Identifier{KindNTName, "jsmith"}},
		{"ntname too many parts", Identifier{KindNTName, `a\b\c`}},
		{"upn no at", Identifier{KindUPN, "jsmith.corp.example"}},
		{"upn two at", Identifier{KindUPN, "a@b@c"}},
		{"username with backslash", Identifier{KindUsername, `corp\jsmith`}},
		{"username with at", Identifier{KindUsername, "jsmith@corp"}},
		{"hostname empty label", Identifier{KindHostname, "web01..corp"}},
		{"hostname leading dash", Identifier{KindHostname, "-web01"}},
		{"ip garbage", Identifier{KindIP, "999.1.2.3"}},
		{"ip empty", Identifier{KindIP, ""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Normalize(tc.id); err == nil {
				t.Fatalf("Normalize(%v) succeeded, want fail-closed rejection", tc.id)
			}
		})
	}
}

func TestSelectCanonicalStrongOutranksWeak(t *testing.T) {
	best, all, err := SelectCanonical([]Identifier{
		{KindUsername, "jsmith"},
		{KindSID, "S-1-5-21-100-200-300-1001"},
		{KindUPN, "jsmith@corp.example"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if best.Kind != KindSID || best.Strength != StrengthStrong {
		t.Errorf("best = %v/%v, want sid/strong", best.Kind, best.Strength)
	}
	if len(all) != 3 {
		t.Errorf("all = %d, want 3", len(all))
	}
}

func TestSelectCanonicalWeakWhenNoStrong(t *testing.T) {
	best, _, err := SelectCanonical([]Identifier{
		{KindHostname, "WEB01"},
		{KindUsername, "jsmith"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if best.Kind != KindHostname || best.Strength != StrengthWeak {
		t.Errorf("best = %v/%v, want hostname/weak (deterministic tie-break)", best.Kind, best.Strength)
	}
}

func TestSelectCanonicalDuplicateSameValueDeduplicates(t *testing.T) {
	best, all, err := SelectCanonical([]Identifier{
		{KindSID, "S-1-5-21-1-2-3-4"},
		{KindSID, "s-1-5-21-1-2-3-4"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if best.Kind != KindSID || len(all) != 1 {
		t.Errorf("best=%v all=%d, want sid/1", best.Kind, len(all))
	}
}

func TestSelectCanonicalConflictingSameKindRejected(t *testing.T) {
	_, _, err := SelectCanonical([]Identifier{
		{KindSID, "S-1-5-21-1-2-3-4"},
		{KindSID, "S-1-5-21-9-9-9-9"},
	})
	if err == nil {
		t.Fatal("two distinct SIDs accepted, want fail-closed rejection")
	}
}

func TestSelectCanonicalEmptyRejected(t *testing.T) {
	if _, _, err := SelectCanonical(nil); err == nil {
		t.Fatal("empty identifier set accepted")
	}
}

func TestEntityIDStabilityAndIsolation(t *testing.T) {
	org := "11111111-1111-1111-1111-111111111111"
	first := EntityID(org, TypeAccount, "corp.example", "sid:s-1-5-21-1-2-3-4")
	second := EntityID(org, TypeAccount, "corp.example", "sid:s-1-5-21-1-2-3-4")
	if first != second {
		t.Fatal("same canonical input produced different entity ids")
	}
	if !strings.HasPrefix(first, "ent:") || len(first) != len("ent:")+64 {
		t.Fatalf("entity id format: %q", first)
	}
	variants := map[string]string{
		"other tenant":  EntityID("22222222-2222-2222-2222-222222222222", TypeAccount, "corp.example", "sid:s-1-5-21-1-2-3-4"),
		"other type":    EntityID(org, TypeDevice, "corp.example", "sid:s-1-5-21-1-2-3-4"),
		"other space":   EntityID(org, TypeAccount, "other.example", "sid:s-1-5-21-1-2-3-4"),
		"other key":     EntityID(org, TypeAccount, "corp.example", "sid:s-1-5-21-9-9-9-9"),
		"weak reuse #2": EntityID(org, TypeAccount, "corp.example", "sid:s-1-5-21-1-2-3-4#2"),
	}
	for name, id := range variants {
		if id == first {
			t.Errorf("%s collided with base id", name)
		}
	}
}

func TestSpaceIDStability(t *testing.T) {
	org := "11111111-1111-1111-1111-111111111111"
	a := SpaceID(org, "corp.example")
	b := SpaceID(org, "corp.example")
	if a != b || !strings.HasPrefix(a, "is:") {
		t.Fatalf("space id not stable or malformed: %q vs %q", a, b)
	}
	if SpaceID(org, "other.example") == a {
		t.Fatal("different space names collide")
	}
}

func TestNormalizeSpaceName(t *testing.T) {
	name, err := NormalizeSpaceName(" CORP.Example ")
	if err != nil || name != "corp.example" {
		t.Fatalf("name=%q err=%v", name, err)
	}
	for _, bad := range []string{"", "-corp", "corp example", strings.Repeat("a", 200), "corp\\x"} {
		if _, err := NormalizeSpaceName(bad); err == nil {
			t.Errorf("space name %q accepted", bad)
		}
	}
}
