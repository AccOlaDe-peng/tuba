package pgutil

import (
	"testing"
	"time"
)

func TestParsePoolConfigAppliesBoundedStatementTimeout(t *testing.T) {
	t.Setenv("PG_STATEMENT_TIMEOUT", "25s")
	cfg, err := ParsePoolConfig("postgres://tuba:secret@127.0.0.1:5432/tuba?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.ConnConfig.RuntimeParams["statement_timeout"]; got != "25000ms" {
		t.Fatalf("statement_timeout=%q", got)
	}
	if cfg.PingTimeout != 5*time.Second {
		t.Fatalf("ping timeout=%s", cfg.PingTimeout)
	}
}

func TestParsePoolConfigRejectsInvalidStatementTimeout(t *testing.T) {
	t.Setenv("PG_STATEMENT_TIMEOUT", "0s")
	if _, err := ParsePoolConfig("postgres://tuba:secret@127.0.0.1:5432/tuba?sslmode=disable"); err == nil {
		t.Fatal("zero PG_STATEMENT_TIMEOUT accepted")
	}
}
