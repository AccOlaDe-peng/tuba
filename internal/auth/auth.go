package auth

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// Permission naming follows <resource>:<verb>. Q03 export redaction adds two
// data-access scopes (design baseline §7): sensitive:read unlocks catalog
// fields declared sensitivity=sensitive (e.g. user.id/user.name), raw:read
// unlocks original payloads (event.original / the raw dataset). Neither is
// granted to viewer; raw:read is tenant_admin only because original payloads
// are the least-derived, most sensitive tier.
var permissions = map[string]map[string]bool{
	"viewer":             {"event:read": true, "anomaly:read": true, "case:read": true},
	"analyst":            {"event:read": true, "anomaly:read": true, "case:read": true, "case:write": true, "analysis:feedback": true, "sensitive:read": true},
	"tenant_admin":       {"event:read": true, "anomaly:read": true, "case:read": true, "case:write": true, "analysis:feedback": true, "sensitive:read": true, "raw:read": true, "user:manage": true, "operations:read": true, "source:manage": true},
	"platform_admin":     {"tenant:manage": true, "user:manage": true, "operations:read": true},
	"platform_publisher": {"release:read": true, "release:manage": true},
}

type Principal struct {
	Subject, Organization, Namespace string
	Roles                            []string
}

func (p Principal) Can(permission string) bool {
	for _, r := range p.Roles {
		if permissions[r][permission] {
			return true
		}
	}
	return false
}
func (p Principal) Permissions() []string {
	s := map[string]bool{}
	for _, r := range p.Roles {
		for x := range permissions[r] {
			s[x] = true
		}
	}
	o := make([]string, 0, len(s))
	for x := range s {
		o = append(o, x)
	}
	sort.Strings(o)
	return o
}

// LocalIssuer identifies accounts owned by this TUBA installation. External
// identity tokens are never accepted by the system login endpoints.
const LocalIssuer = "tuba:local"
const SessionCookie = "tuba_session"

type Verifier interface {
	Verify(context.Context, string) (Principal, error)
}

func safeID(s string) bool {
	if len(s) < 1 || len(s) > 64 {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}
func Bearer(r *http.Request) (string, error) {
	v := r.Header.Get("Authorization")
	if !strings.HasPrefix(v, "Bearer ") || strings.TrimSpace(v[7:]) == "" {
		return "", fmt.Errorf("bearer token required")
	}
	return strings.TrimSpace(v[7:]), nil
}
