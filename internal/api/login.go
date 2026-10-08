package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"tuba/product/internal/auth"
)

type LoginService interface {
	auth.Verifier
	Login(context.Context, string, string, string) (string, auth.Principal, time.Time, error)
	Logout(context.Context, string, string) error
	ChangePassword(context.Context, auth.Principal, string, string, string) error
}

// LoginLimiter bounds password-hashing work, including attempts at unknown
// accounts. The proxy IP is used deliberately; client-supplied forwarding
// headers cannot bypass the limit. Account lockouts are separately persisted.
type LoginLimiter struct {
	mu      sync.Mutex
	windows map[string]loginWindow
}
type loginWindow struct {
	until time.Time
	count int
}

func (l *LoginLimiter) Allow(remote string) bool {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.windows == nil {
		l.windows = map[string]loginWindow{}
	}
	now := time.Now()
	for key, v := range l.windows {
		if !v.until.After(now) {
			delete(l.windows, key)
		}
	}
	v := l.windows[host]
	if v.until.IsZero() {
		v.until = now.Add(time.Minute)
	}
	if v.count >= 30 || len(l.windows) >= 10000 && v.count == 0 {
		return false
	}
	v.count++
	l.windows[host] = v
	return true
}

func (s Server) allowedOrigin(r *http.Request, required bool) bool {
	if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
		return false
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return !required
	}
	if s.PublicOrigin != "" {
		return origin == s.PublicOrigin
	}
	u, err := url.Parse(origin)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host == r.Host && u.Path == "" && u.User == nil && u.RawQuery == "" && u.Fragment == ""
}
func sessionToken(r *http.Request) (string, bool, error) {
	if r.Header.Get("Authorization") != "" {
		v, e := auth.Bearer(r)
		return v, false, e
	}
	c, err := r.Cookie(auth.SessionCookie)
	if err != nil || c.Value == "" {
		return "", true, auth.ErrCredentials
	}
	return c.Value, true, nil
}
func (s Server) sessionCookie(w http.ResponseWriter, token string, expiry time.Time) {
	age := int(time.Until(expiry).Seconds())
	if token == "" {
		age = -1
	}
	http.SetCookie(w, &http.Cookie{Name: auth.SessionCookie, Value: token, Path: "/api/v1", HttpOnly: true, Secure: !s.InsecureSessionCookie, SameSite: http.SameSiteStrictMode, MaxAge: age, Expires: expiry})
}
func loginJSON(w http.ResponseWriter, r *http.Request, value any) bool {
	if strings.Split(r.Header.Get("Content-Type"), ";")[0] != "application/json" {
		http.Error(w, "application/json required", 415)
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		http.Error(w, "invalid login request", 400)
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); err == nil || !errors.Is(err, io.EOF) {
		http.Error(w, "invalid login request", 400)
		return false
	}
	return true
}
func (s Server) login(w http.ResponseWriter, r *http.Request) {
	if !s.allowedOrigin(r, false) {
		http.Error(w, "origin denied", 403)
		return
	}
	if s.Login == nil {
		http.Error(w, "login unavailable", 503)
		return
	}
	if s.LoginLimiter != nil && !s.LoginLimiter.Allow(r.RemoteAddr) {
		w.Header().Set("Retry-After", "60")
		http.Error(w, "too many login attempts", 429)
		return
	}
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if !loginJSON(w, r, &body) {
		return
	}
	token, p, expiry, err := s.Login.Login(r.Context(), body.Username, body.Password, requestID(r))
	if errors.Is(err, auth.ErrLocked) {
		w.Header().Set("Retry-After", "900")
		http.Error(w, "too many login attempts", 429)
		return
	}
	if errors.Is(err, auth.ErrCredentials) {
		http.Error(w, "invalid username or password", 401)
		return
	}
	if err != nil {
		http.Error(w, "login unavailable", 503)
		return
	}
	// Rotate any existing browser session after successful authentication.
	if previous, e := r.Cookie(auth.SessionCookie); e == nil && previous.Value != "" {
		if err = s.Login.Logout(r.Context(), previous.Value, requestID(r)); err != nil {
			_ = s.Login.Logout(r.Context(), token, requestID(r))
			http.Error(w, "login unavailable", 503)
			return
		}
	}
	s.sessionCookie(w, token, expiry)
	s.me(w, r, p)
}
func (s Server) logout(w http.ResponseWriter, r *http.Request) {
	if !s.allowedOrigin(r, false) {
		http.Error(w, "origin denied", 403)
		return
	}
	if s.Login == nil {
		http.Error(w, "login unavailable", 503)
		return
	}
	token, cookie, err := sessionToken(r)
	if err == nil && cookie && !s.allowedOrigin(r, true) {
		http.Error(w, "origin denied", 403)
		return
	}
	if err == nil {
		if err = s.Login.Logout(r.Context(), token, requestID(r)); err != nil {
			http.Error(w, "logout unavailable", 503)
			return
		}
	}
	s.sessionCookie(w, "", time.Unix(1, 0))
	w.WriteHeader(204)
}
func (s Server) changePassword(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if s.Login == nil {
		http.Error(w, "login unavailable", 503)
		return
	}
	if s.LoginLimiter != nil && !s.LoginLimiter.Allow(r.RemoteAddr) {
		http.Error(w, "too many password attempts", 429)
		return
	}
	var body struct {
		Current  string `json:"current_password"`
		Password string `json:"password"`
	}
	if !loginJSON(w, r, &body) {
		return
	}
	if err := auth.ValidatePassword(body.Password); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	err := s.Login.ChangePassword(r.Context(), p, body.Current, body.Password, requestID(r))
	if errors.Is(err, auth.ErrCredentials) {
		http.Error(w, "invalid current password", 401)
		return
	}
	if err != nil {
		http.Error(w, "password change unavailable", 503)
		return
	}
	s.sessionCookie(w, "", time.Unix(1, 0))
	w.WriteHeader(204)
}

func (s Server) createLocalAccount(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if s.Control == nil {
		http.Error(w, "account management unavailable", 503)
		return
	}
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Role     string `json:"role"`
	}
	if !loginJSON(w, r, &body) {
		return
	}
	if _, err := auth.NormalizeUsername(body.Username); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if err := auth.ValidatePassword(body.Password); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if !map[string]bool{"viewer": true, "analyst": true, "tenant_admin": true}[body.Role] {
		http.Error(w, "invalid role", 400)
		return
	}
	subject, err := s.Control.CreateLocalAccount(r.Context(), p, body.Username, body.Password, body.Role, requestID(r))
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		http.Error(w, "username already exists", 409)
		return
	}
	if err != nil {
		http.Error(w, "account creation unavailable", 503)
		return
	}
	writeJSON(w, 201, map[string]string{"subject": subject, "username": body.Username})
}

func (s Server) authDenied(w http.ResponseWriter, r *http.Request, p auth.Principal, reason string) {
	if s.Control != nil {
		if err := s.Control.AuditAuthDenial(r.Context(), p, reason, requestID(r)); err != nil {
			http.Error(w, "authorization unavailable", 503)
			return
		}
	}
	http.Error(w, reason, 403)
}
