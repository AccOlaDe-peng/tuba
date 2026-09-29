// tuba-bootstrap-release-publisher creates the initial global release publisher.
// Run once with the application DB identity after migration 00013. The operator
// approval string is written to the immutable global audit trail.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	var subject, issuer, approvedBy string
	flag.StringVar(&subject, "subject", "", "exact OIDC subject to grant")
	flag.StringVar(&issuer, "issuer", os.Getenv("OIDC_ISSUER"), "exact OIDC issuer")
	flag.StringVar(&approvedBy, "approved-by", "", "change/ticket or explicit operator authorization reference")
	flag.Parse()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" || subject == "" || issuer == "" || approvedBy == "" || len(subject) > 256 || len(approvedBy) > 256 || strings.ContainsAny(subject, "\r\n") || strings.ContainsAny(approvedBy, "\r\n") {
		log.Fatal("DATABASE_URL, --subject, OIDC_ISSUER and --approved-by are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var existing bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM platform_release_publishers)`).Scan(&existing); err != nil {
		log.Fatal(err)
	}
	if existing {
		log.Fatal("publisher bootstrap is one-time and a publisher record already exists")
	}
	var id string
	err = tx.QueryRow(ctx, `INSERT INTO identities(issuer,subject) VALUES($1,$2) ON CONFLICT(issuer,subject) DO UPDATE SET subject=excluded.subject RETURNING id::text`, strings.TrimRight(issuer, "/"), subject).Scan(&id)
	if err != nil {
		log.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO platform_release_publishers(identity_id) VALUES($1)`, id); err != nil {
		log.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events(actor_identity_id,action,resource_type,resource_id,request_id,metadata) VALUES(NULL,'release_publisher.bootstrap','platform_release_publisher',$1,$2,jsonb_build_object('approved_by',$3::text,'issuer',$4::text,'bootstrap_tool','tuba-bootstrap-release-publisher'))`, subject, "bootstrap-release-publisher:"+subject, approvedBy, strings.TrimRight(issuer, "/")); err != nil {
		log.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("bootstrapped platform release publisher for subject %s; approval recorded\n", subject)
}
