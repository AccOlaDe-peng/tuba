package controlworker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"tuba/product/internal/pgutil"
)

// ProbeDependencies checks the control worker's required persistence and
// delivery dependencies without creating or modifying Kafka records.
func ProbeDependencies(ctx context.Context, pool *pgxpool.Pool, dialer *kafka.Dialer, brokers []string) error {
	if pool == nil {
		return errors.New("control worker PostgreSQL pool is not configured")
	}
	if dialer == nil || len(brokers) == 0 || brokers[0] == "" {
		return errors.New("control worker Kafka dialer and broker are required")
	}

	pingCtx, cancelPing := context.WithTimeout(ctx, pgutil.PoolPingTimeout)
	err := pool.Ping(pingCtx)
	cancelPing()
	if err != nil {
		return fmt.Errorf("PostgreSQL readiness probe: %w", err)
	}

	kafkaCtx, cancelKafka := context.WithTimeout(ctx, 5*time.Second)
	defer cancelKafka()
	conn, err := dialer.DialContext(kafkaCtx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("Kafka readiness probe: %w", err)
	}
	_ = conn.Close()
	return nil
}
