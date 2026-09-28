package kafkaadmin

import (
	"encoding/hex"
	"os"
	"strings"
	"testing"
)

func TestBuildSecurityPlanUsesLiteralBoundedServiceACLs(t *testing.T) {
	contract, err := os.ReadFile("../../contracts/events/topics.v1.json")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildSecurityPlan(contract, "zeek_validation_single_node", "test_validation", "r2")
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Principals) != 6 {
		t.Fatalf("principals=%d, want 6", len(plan.Principals))
	}
	if len(plan.ACLs) == 0 {
		t.Fatal("expected service ACLs")
	}
	for _, acl := range plan.ACLs {
		if strings.Contains(acl.ResourceName, "ctx_") || strings.ContainsAny(acl.ResourceName, "*+?") {
			t.Errorf("security plan included source context or wildcard resource: %+v", acl)
		}
		if strings.Contains(acl.ResourceName, "<") || strings.Contains(acl.ResourceName, "{") {
			t.Errorf("unexpanded contract placeholder: %+v", acl)
		}
	}
	var foundGroup bool
	standardGroups := 0
	for _, acl := range plan.ACLs {
		if acl.ResourceType == "group" {
			if strings.HasSuffix(acl.ResourceName, "-r2") {
				foundGroup = true
			}
			if strings.HasPrefix(acl.ResourceName, "tuba-standard-indexer-") {
				standardGroups++
			}
		}
	}
	if !foundGroup {
		t.Fatal("expected exact suffixed consumer-group ACLs")
	}
	if standardGroups != 8 {
		t.Fatalf("standard indexer group ACLs=%d, want 8", standardGroups)
	}
}

func TestBuildSecurityPlanRejectsProductionProfile(t *testing.T) {
	contract, err := os.ReadFile("../../contracts/events/topics.v1.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BuildSecurityPlan(contract, "production", "prod", ""); err == nil {
		t.Fatal("production profile was accepted")
	}
}

func TestGenerateSaltedPassword(t *testing.T) {
	password := strings.Repeat("x", 40)
	salt1, salted1, err := GenerateSaltedPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	salt2, salted2, err := GenerateSaltedPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	if len(salt1) != 32 || len(salted1) != 64 || string(salt1) == string(salt2) || string(salted1) == string(salted2) {
		t.Fatal("SCRAM credentials need random salt and SHA-512 output")
	}
	if _, _, err := GenerateSaltedPassword("short"); err == nil {
		t.Fatal("short service password was accepted")
	}
}

func TestPBKDF2SHA512MatchesReferenceVector(t *testing.T) {
	got := pbkdf2SHA512([]byte("password"), []byte("salt"), 4096)
	const want = "d197b1b33db0143e018b12f3d1d1479e6cdebdcc97c5c0f87f6902e072f457b5143f30602641b3d55cd335988cb36b84376060ecd532e039b742a239434af2d5"
	if hex.EncodeToString(got) != want {
		t.Fatalf("PBKDF2-SHA-512 mismatch: %x", got)
	}
}
