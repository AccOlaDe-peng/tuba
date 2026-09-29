// tuba-bootstrap-operator creates the first tenant administrator when a tenant
// has no active administrator who could otherwise use the audited membership
// API. It is intentionally one-time and writes an explicit bootstrap audit
// event with a null actor, because no authenticated TUBA operator exists yet.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	var subject, organization, email, displayName, approvedBy string
	flag.StringVar(&subject, "subject", "", "Keycloak subject (sub) of the already-created user")
	flag.StringVar(&organization, "organization", "", "TUBA organization slug")
	flag.StringVar(&email, "email", "", "operator email (optional)")
	flag.StringVar(&displayName, "display-name", "", "operator display name (optional)")
	flag.StringVar(&approvedBy, "approved-by", "", "operator or change reference authorizing this bootstrap")
	flag.Parse()

	issuer := strings.TrimRight(os.Getenv("OIDC_ISSUER"), "/")
	databaseURL := os.Getenv("DATABASE_URL")
	if issuer == "" || databaseURL == "" || subject == "" || organization == "" || approvedBy == "" {
		log.Fatal("OIDC_ISSUER, DATABASE_URL, --subject, --organization and --approved-by are required")
	}
	if len(subject) > 256 || len(organization) > 64 || len(approvedBy) > 256 {
		log.Fatal("argument exceeds allowed length")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("connect to control database: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("ping control database: %v", err)
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		log.Fatalf("begin bootstrap transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	var organizationID, roleID string
	if err := tx.QueryRow(ctx, `SELECT id::text FROM organizations WHERE slug=$1 FOR UPDATE`, organization).Scan(&organizationID); err != nil {
		log.Fatalf("organization %q not found: %v", organization, err)
	}
	if err := tx.QueryRow(ctx, `SELECT id::text FROM roles WHERE organization_id=$1 AND name='tenant_admin'`, organizationID).Scan(&roleID); err != nil {
		log.Fatalf("tenant_admin role not found for %q: %v", organization, err)
	}
	var hasAdmin bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM memberships m JOIN identities i ON i.id=m.identity_id WHERE m.organization_id=$1 AND m.role_id=$2 AND m.revoked_at IS NULL AND i.issuer=$3 AND i.disabled_at IS NULL)`, organizationID, roleID, issuer).Scan(&hasAdmin); err != nil {
		log.Fatalf("check existing tenant administrators: %v", err)
	}
	if hasAdmin {
		log.Fatal("bootstrap refused: an active tenant_admin already exists; use the normal member-management API")
	}

	var identityID string
	err = tx.QueryRow(ctx, `INSERT INTO identities(issuer,subject,email,display_name) VALUES($1,$2,NULLIF($3,''),NULLIF($4,'')) ON CONFLICT(issuer,subject) DO UPDATE SET email=COALESCE(NULLIF(excluded.email,''),identities.email),display_name=COALESCE(NULLIF(excluded.display_name,''),identities.display_name) WHERE identities.disabled_at IS NULL RETURNING id::text`, issuer, subject, email, displayName).Scan(&identityID)
	if err != nil {
		log.Fatalf("create or locate enabled identity: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO memberships(organization_id,identity_id,role_id,revoked_at) VALUES($1,$2,$3,NULL) ON CONFLICT(organization_id,identity_id,role_id) DO UPDATE SET revoked_at=NULL`, organizationID, identityID, roleID); err != nil {
		log.Fatalf("grant initial tenant_admin membership: %v", err)
	}
	requestID := fmt.Sprintf("bootstrap-%d", time.Now().UTC().UnixNano())
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,after_state,metadata) VALUES($1,NULL,'identity.bootstrap_membership','membership',$2,$3,jsonb_build_object('role','tenant_admin','issuer',$4::text,'subject',$2::text),jsonb_build_object('method','tuba-bootstrap-operator','approved_by',$5::text))`, organizationID, subject, requestID, issuer, approvedBy); err != nil {
		log.Fatalf("write bootstrap audit event: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		log.Fatalf("commit bootstrap transaction: %v", err)
	}
	fmt.Printf("bootstrapped tenant_admin membership: organization=%s subject=%s issuer=%s\n", organization, subject, issuer)
}
