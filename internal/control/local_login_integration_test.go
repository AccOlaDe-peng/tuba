package control

import (
	"context"
	"crypto/rand"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"tuba/product/internal/auth"
)

func TestLocalLoginPostgres(t *testing.T) {
	dsn := os.Getenv("TUBA_LOGIN_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TUBA_LOGIN_TEST_DATABASE_URL to test isolated schema against PostgreSQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	root, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	schema := "login_test_" + strings.ToLower(rand.Text()[:12])
	if _, err = root.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err = root.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	}()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, name := range []string{"00001_control_plane.sql", "00002_seed_local_tenant.sql", "00022_local_system_login.sql"} {
		// Resolve actual numbered filenames rather than duplicate the schema in tests.
		matches, err := filepath.Glob(filepath.Join("..", "..", "migrations", strings.Split(name, "_")[0]+"_*.sql"))
		if err != nil || len(matches) != 1 {
			t.Fatal("migration not found", name)
		}
		data, err := os.ReadFile(matches[0])
		if err != nil {
			t.Fatal(err)
		}
		up := strings.Split(strings.Split(string(data), "-- +goose Up")[1], "-- +goose Down")[0]
		if _, err = pool.Exec(ctx, up); err != nil {
			t.Fatal(name, err)
		}
	}
	if _, err = pool.Exec(ctx, `CREATE TABLE platform_release_publishers(identity_id uuid PRIMARY KEY REFERENCES identities(id),revoked_at timestamptz)`); err != nil {
		t.Fatal(err)
	}
	store := &Store{Pool: pool, Issuer: auth.LocalIssuer}
	login := &LocalLogin{Store: store, Lifetime: time.Hour}
	var identity, org, role string
	if err = pool.QueryRow(ctx, `INSERT INTO identities(issuer,subject) VALUES('external-old','old-admin') RETURNING id::text`).Scan(&identity); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT o.id::text,r.id::text FROM organizations o JOIN roles r ON r.organization_id=o.id WHERE o.slug='tenant_a' AND r.name='tenant_admin'`).Scan(&org, &role); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO memberships(organization_id,identity_id,role_id) VALUES($1,$2,$3)`, org, identity, role); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO platform_release_publishers(identity_id) VALUES($1)`, identity); err != nil {
		t.Fatal(err)
	}
	password := "Integration-test-only-123!"
	subject, err := store.BootstrapLocalAdmin(ctx, "admin", password, "tenant_a", "old-admin", "test-authorization")
	if err != nil || subject != "old-admin" {
		t.Fatal("adoption", subject, err)
	}
	if _, err = store.BootstrapLocalAdmin(ctx, "other-admin", password, "tenant_a", "", "test-authorization"); err == nil {
		t.Fatal("bootstrap repeated")
	}
	token, p, _, err := login.Login(ctx, "ADMIN", password, "test-login")
	if err != nil {
		t.Fatal(err)
	}
	if !p.Can("source:manage") || !p.Can("release:manage") {
		t.Fatal("adoption lost memberships or publisher grant")
	}
	var id string
	if err = pool.QueryRow(ctx, `SELECT identity_id::text FROM local_accounts WHERE username='admin'`).Scan(&id); err != nil || id != identity {
		t.Fatal("identity reference changed", err)
	}
	if _, err = login.Verify(ctx, token); err != nil {
		t.Fatal(err)
	}
	var storedTokenLength int
	if err = pool.QueryRow(ctx, `SELECT octet_length(token_hash) FROM auth_sessions LIMIT 1`).Scan(&storedTokenLength); err != nil || storedTokenLength != 32 {
		t.Fatal("session digest not stored", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE auth_sessions SET expires_at=now()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	if _, err = login.Verify(ctx, token); err == nil {
		t.Fatal("expired session accepted")
	}
	token, p, _, err = login.Login(ctx, "admin", password, "test-expiry-recovery")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE identities SET disabled_at=now() WHERE id=$1`, identity); err != nil {
		t.Fatal(err)
	}
	if _, err = login.Verify(ctx, token); err == nil {
		t.Fatal("disabled identity accepted")
	}
	if _, err = pool.Exec(ctx, `UPDATE identities SET disabled_at=NULL WHERE id=$1`, identity); err != nil {
		t.Fatal(err)
	}

	if _, err = login.Verify(ctx, "external.jwt.token"); err == nil {
		t.Fatal("external token accepted")
	}
	if err = login.Logout(ctx, token, "test-logout"); err != nil {
		t.Fatal(err)
	}
	if _, err = login.Verify(ctx, token); err == nil {
		t.Fatal("revoked session accepted")
	}
	token, p, _, err = login.Login(ctx, "admin", password, "test-login-2")
	if err != nil {
		t.Fatal(err)
	}
	if err = login.ChangePassword(ctx, p, password, "New-integration-password-123!", "test-password"); err != nil {
		t.Fatal(err)
	}
	if _, err = login.Verify(ctx, token); err == nil {
		t.Fatal("password change did not revoke sessions")
	}
	if _, _, _, err = login.Login(ctx, "admin", password, "test-old-password"); err == nil {
		t.Fatal("old password accepted")
	}
	password = "New-integration-password-123!"
	for i := 0; i < 4; i++ {
		if _, _, _, err = login.Login(ctx, "admin", "wrong-password", fmt.Sprint("failed-", i)); err != auth.ErrCredentials {
			t.Fatal(err)
		}
	}
	if _, _, _, err = login.Login(ctx, "admin", password, "test-lockout"); err != auth.ErrLocked {
		t.Fatal("persisted lockout missing", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE local_accounts SET locked_until=now()-interval '1 second' WHERE username='admin'`); err != nil {
		t.Fatal(err)
	}
	_, p, _, err = login.Login(ctx, "admin", password, "test-recovery")
	if err != nil {
		t.Fatal("lockout recovery", err)
	}
	analyst, err := store.CreateLocalAccount(ctx, p, "analyst", password, "analyst", "test-create")
	if err != nil {
		t.Fatal(err)
	}
	_, ap, _, err := login.Login(ctx, "analyst", password, "test-analyst")
	if err != nil || ap.Subject != analyst || ap.Can("user:manage") {
		t.Fatal("analyst permissions", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE memberships SET revoked_at=now() WHERE identity_id=$1`, identity); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Authorize(ctx, p); err == nil {
		t.Fatal("revoked membership accepted")
	}
	if _, _, _, err = login.Login(ctx, "admin", password, "test-membership-denied"); err == nil {
		t.Fatal("login without active membership accepted")
	}
	var count int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM audit_events WHERE action LIKE 'auth.%'`).Scan(&count); err != nil || count < 10 {
		t.Fatal("missing auth audit", count, err)
	}
}
