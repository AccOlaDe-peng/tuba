package auth

import (
	"context"
	"errors"
	"fmt"
	"github.com/coreos/go-oidc/v3/oidc"
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

type Verifier struct {
	Issuer, Audience, JWKSURL string
	oidc                      *oidc.IDTokenVerifier
}

func NewVerifier(issuer, audience, jwksURL string) (*Verifier, error) {
	if issuer == "" || audience == "" {
		return nil, errors.New("OIDC_ISSUER and OIDC_AUDIENCE are required")
	}
	issuer = strings.TrimRight(issuer, "/")
	if jwksURL == "" {
		jwksURL = issuer + "/protocol/openid-connect/certs"
	}
	if !strings.HasPrefix(jwksURL, "https://") && !strings.HasPrefix(jwksURL, "http://") {
		return nil, errors.New("invalid OIDC_JWKS_URL")
	}
	keys := oidc.NewRemoteKeySet(context.Background(), jwksURL)
	return &Verifier{issuer, audience, jwksURL, oidc.NewVerifier(issuer, keys, &oidc.Config{ClientID: audience, SupportedSigningAlgs: []string{oidc.RS256}})}, nil
}
func (v *Verifier) Verify(ctx context.Context, raw string) (Principal, error) {
	var empty Principal
	if len(raw) > 16*1024 {
		return empty, errors.New("invalid access token")
	}
	token, err := v.oidc.Verify(ctx, raw)
	if err != nil {
		return empty, err
	}
	var c struct {
		Subject      string   `json:"sub"`
		Organization string   `json:"organization_id"`
		Namespace    string   `json:"namespace"`
		Roles        []string `json:"tuba_roles"`
	}
	if err := token.Claims(&c); err != nil {
		return empty, err
	}
	if c.Subject == "" || !safeID(c.Organization) || !safeID(c.Namespace) {
		return empty, errors.New("invalid tenant claims")
	}
	roles := make([]string, 0, len(c.Roles))
	for _, r := range c.Roles {
		if _, ok := permissions[r]; ok {
			roles = append(roles, r)
		}
	}
	return Principal{c.Subject, c.Organization, c.Namespace, roles}, nil
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
