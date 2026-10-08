package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"tuba/product/internal/auth"
)

type fakeLogin struct {
	token           string
	revoked         bool
	passwordChanged bool
	calls           int
}

func (l *fakeLogin) Verify(_ context.Context, token string) (auth.Principal, error) {
	if token != l.token || l.revoked {
		return auth.Principal{}, auth.ErrCredentials
	}
	return auth.Principal{Subject: "user1", Organization: "tenant_a", Namespace: "tenant_a", Roles: []string{"viewer"}}, nil
}
func (l *fakeLogin) Login(_ context.Context, name, password, _ string) (string, auth.Principal, time.Time, error) {
	l.calls++
	if name != "user" || password != "valid-password" {
		return "", auth.Principal{}, time.Time{}, auth.ErrCredentials
	}
	l.revoked = false
	p, _ := l.Verify(context.Background(), l.token)
	return l.token, p, time.Now().Add(time.Hour), nil
}
func (l *fakeLogin) Logout(_ context.Context, token, _ string) error {
	if token == l.token {
		l.revoked = true
	}
	return nil
}
func (l *fakeLogin) ChangePassword(_ context.Context, _ auth.Principal, current, password, _ string) error {
	if current != "valid-password" {
		return auth.ErrCredentials
	}
	l.passwordChanged = true
	l.revoked = true
	return nil
}
func loginRequest(handler http.Handler, method, path, body, origin, cookie string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	if cookie != "" {
		r.Header.Set("Cookie", auth.SessionCookie+"="+cookie)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}
func TestNativeLoginCookieRestoreRevokeAndCSRF(t *testing.T) {
	service := &fakeLogin{token: "opaque-session"}
	handler := (Server{Verifier: service, Login: service, PublicOrigin: "https://tuba.example", LoginLimiter: &LoginLimiter{}}).Handler()
	if w := loginRequest(handler, "GET", "/api/v1/me", "", "", ""); w.Code != 401 {
		t.Fatal(w.Code)
	}
	w := loginRequest(handler, "POST", "/api/v1/auth/login", `{"username":"user","password":"valid-password"}`, "https://tuba.example", "")
	if w.Code != 200 {
		t.Fatalf("login: %d %s", w.Code, w.Body.String())
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatal("session cookie missing")
	}
	c := cookies[0]
	if !c.Secure || !c.HttpOnly || c.SameSite != http.SameSiteStrictMode || c.Path != "/api/v1" {
		t.Fatalf("unsafe cookie: %+v", c)
	}
	if strings.Contains(w.Body.String(), service.token) {
		t.Fatal("session leaked in JSON")
	}
	if w := loginRequest(handler, "GET", "/api/v1/me", "", "", service.token); w.Code != 200 {
		t.Fatal("restore", w.Code)
	}
	for _, origin := range []string{"https://evil.example", ""} {
		if w := loginRequest(handler, "POST", "/api/v1/auth/password", `{"current_password":"valid-password","password":"new-password-123"}`, origin, service.token); w.Code != 403 {
			t.Fatal("CSRF accepted", origin, w.Code)
		}
	}
	if w := loginRequest(handler, "POST", "/api/v1/auth/logout", "", "https://evil.example", service.token); w.Code != 403 || service.revoked {
		t.Fatal("cross-origin logout accepted")
	}
	if w := loginRequest(handler, "POST", "/api/v1/auth/logout", "", "https://tuba.example", service.token); w.Code != 204 || !service.revoked {
		t.Fatal("logout failed", w.Code)
	}
	if w := loginRequest(handler, "GET", "/api/v1/me", "", "", service.token); w.Code != 401 {
		t.Fatal("revoked session accepted")
	}
	r := httptest.NewRequest("GET", "/api/v1/me", nil)
	r.Header.Set("Authorization", "Bearer external.jwt.token")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("external token accepted")
	}
}
func TestNativeLoginInputLimitsAndPasswordChange(t *testing.T) {
	service := &fakeLogin{token: "opaque-session"}
	handler := (Server{Verifier: service, Login: service, PublicOrigin: "https://tuba.example", LoginLimiter: &LoginLimiter{}}).Handler()
	for _, body := range []string{`{"username":"user","password":"valid-password","organization":"foreign"}`, `{"username":"user"} {}`, strings.Repeat("x", 5000)} {
		if w := loginRequest(handler, "POST", "/api/v1/auth/login", body, "https://tuba.example", ""); w.Code != 400 {
			t.Fatal("invalid input accepted", w.Code)
		}
	}
	if service.calls != 0 {
		t.Fatal("invalid input reached login service")
	}
	if w := loginRequest(handler, "POST", "/api/v1/auth/login", `{"username":"user","password":"wrong"}`, "https://tuba.example", ""); w.Code != 401 {
		t.Fatal(w.Code)
	}
	if w := loginRequest(handler, "POST", "/api/v1/auth/password", `{"current_password":"valid-password","password":"new-password-123"}`, "https://tuba.example", service.token); w.Code != 204 || !service.passwordChanged {
		t.Fatal("change password failed", w.Code, w.Body.String())
	}
	if w := loginRequest(handler, "GET", "/api/v1/me", "", "", service.token); w.Code != 401 {
		t.Fatal("password change failed to revoke session")
	}
	limiter := &LoginLimiter{}
	for i := 0; i < 30; i++ {
		if !limiter.Allow("127.0.0.1:1234") {
			t.Fatal("premature limit")
		}
	}
	if limiter.Allow("127.0.0.1:9999") {
		t.Fatal("client port bypassed rate limit")
	}
}
