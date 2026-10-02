package auth

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func signedToken(t *testing.T, key *rsa.PrivateKey, claims map[string]any) string {
	t.Helper()
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "kid": "test-key"})
	payload, _ := json.Marshal(claims)
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	hash := sha256.Sum256([]byte(unsigned))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, hash[:])
	if err != nil {
		t.Fatal(err)
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func TestOIDCVerificationAndRoleIsolation(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	n := base64.RawURLEncoding.EncodeToString(key.N.Bytes())
	e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes())
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]string{"kty": "RSA", "kid": "test-key", "alg": "RS256", "use": "sig", "n": n, "e": e}}})
	}))
	defer jwks.Close()
	v, err := NewVerifier("https://identity.example/realms/tuba", "tuba-api", jwks.URL)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	claims := map[string]any{"sub": "u1", "iss": v.Issuer, "aud": v.Audience, "iat": now, "exp": now + 300, "organization_id": "tenant_a", "namespace": "ns_a", "tuba_roles": []string{"viewer"}}
	p, err := v.Verify(context.Background(), signedToken(t, key, claims))
	if err != nil {
		t.Fatal(err)
	}
	if !p.Can("case:read") || p.Can("case:write") || p.Organization != "tenant_a" {
		t.Fatal("incorrect permissions")
	}
	claims["aud"] = "another-api"
	if _, err := v.Verify(context.Background(), signedToken(t, key, claims)); err == nil {
		t.Fatal("wrong audience accepted")
	}
	claims["aud"] = v.Audience
	claims["exp"] = now - 60
	if _, err := v.Verify(context.Background(), signedToken(t, key, claims)); err == nil {
		t.Fatal("expired token accepted")
	}
	claims["exp"] = now + 300
	claims["organization_id"] = "../other"
	if _, err := v.Verify(context.Background(), signedToken(t, key, claims)); err == nil {
		t.Fatal("unsafe tenant accepted")
	}
	claims["organization_id"] = "tenant_a"
	bad := signedToken(t, key, claims)
	parts := strings.Split(bad, ".")
	signature, _ := base64.RawURLEncoding.DecodeString(parts[2])
	signature[0] ^= 1
	parts[2] = base64.RawURLEncoding.EncodeToString(signature)
	if _, err := v.Verify(context.Background(), strings.Join(parts, ".")); err == nil {
		t.Fatal("modified signature accepted")
	}
}
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
