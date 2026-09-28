package pgutil

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	DefaultStatementTimeout = 15 * time.Second
	MinStatementTimeout     = time.Second
	MaxStatementTimeout     = 5 * time.Minute
	PoolPingTimeout         = 5 * time.Second
)

func NewPool(ctx context.Context, connectionString string) (*pgxpool.Pool, error) {
	cfg, err := ParsePoolConfig(connectionString)
	if err != nil {
		return nil, err
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}

func ParsePoolConfig(connectionString string) (*pgxpool.Config, error) {
	timeout, err := statementTimeoutFromEnv(os.Getenv("PG_STATEMENT_TIMEOUT"))
	if err != nil {
		return nil, err
	}
	cfg, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, err
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = make(map[string]string)
	}
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = strconv.FormatInt(timeout.Milliseconds(), 10) + "ms"
	cfg.PingTimeout = PoolPingTimeout
	return cfg, nil
}

func statementTimeoutFromEnv(value string) (time.Duration, error) {
	if value == "" {
		return DefaultStatementTimeout, nil
	}
	timeout, err := time.ParseDuration(value)
	if err != nil || timeout < MinStatementTimeout || timeout > MaxStatementTimeout {
		return 0, fmt.Errorf("PG_STATEMENT_TIMEOUT must be a duration between %s and %s", MinStatementTimeout, MaxStatementTimeout)
	}
	return timeout, nil
}
