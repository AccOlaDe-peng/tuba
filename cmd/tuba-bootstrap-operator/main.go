// tuba-bootstrap-operator creates or adopts the first native system administrator.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"
	"tuba/product/internal/auth"
	"tuba/product/internal/control"
)

func main() {
	var username, organization, existing, approved string
	flag.StringVar(&username, "username", "", "system login username")
	flag.StringVar(&organization, "organization", "", "organization slug")
	flag.StringVar(&existing, "existing-subject", "", "optional existing tenant administrator subject to adopt, preserving all identity references")
	flag.StringVar(&approved, "approved-by", "", "authorization/change reference written to audit")
	flag.Parse()
	if username == "" || organization == "" || approved == "" {
		log.Fatal("DATABASE_URL, --username, --organization and --approved-by are required; supply password on stdin")
	}
	input, err := io.ReadAll(io.LimitReader(os.Stdin, 258))
	if err != nil {
		log.Fatal("read password from stdin")
	}
	password := strings.TrimSuffix(strings.TrimSuffix(string(input), "\n"), "\r")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := control.Open(ctx, os.Getenv("DATABASE_URL"), auth.LocalIssuer)
	if err != nil {
		log.Fatal("connect to database failed")
	}
	defer store.Close()
	subject, err := store.BootstrapLocalAdmin(ctx, username, password, organization, existing, approved)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("system administrator ready: username=%s organization=%s subject=%s\n", username, organization, subject)
}
