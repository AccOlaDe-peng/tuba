package auth

import "testing"

func TestRolePermissionMatrix(t *testing.T) {
	tests := []struct {
		role                          string
		read, write, feedback, manage bool
		sensitive, raw                bool
	}{
		{"viewer", true, false, false, false, false, false},
		{"analyst", true, true, true, false, true, false},
		{"tenant_admin", true, true, true, true, true, true},
	}
	for _, x := range tests {
		p := Principal{Roles: []string{x.role}}
		if p.Can("event:read") != x.read || p.Can("case:read") != x.read || p.Can("case:write") != x.write ||
			p.Can("analysis:feedback") != x.feedback || p.Can("user:manage") != x.manage ||
			p.Can("sensitive:read") != x.sensitive || p.Can("raw:read") != x.raw {
			t.Fatalf("role %s mismatch", x.role)
		}
	}
}
