package analysis

import "testing"

func TestParseResultEnforcesTenant(t *testing.T) {
	raw := []byte(`{"contract_version":"1.0.0","result_type":"anomaly","result_id":"a1","organization_id":"tenant_a","namespace":"tenant_a","rule_id":"r1","rule_version":"1.0.0","run_id":"run-1","document":{"organization":{"id":"tenant_a"}}}`)
	if _, err := Parse(raw, "tenant_a", "tenant_a"); err != nil {
		t.Fatal(err)
	}
	if _, err := Parse(raw, "tenant_b", "tenant_b"); err == nil {
		t.Fatal("expected cross-tenant result to be rejected")
	}
}
