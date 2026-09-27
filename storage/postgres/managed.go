package postgres

import (
	"context"

	"github.com/alob-mtc/runnerq-go/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

// FreshStats bypasses the process-local display cache for service reads that
// must reflect the caller's preceding committed operation.
func (b *PostgresBackend) FreshStats(ctx context.Context) (*storage.QueueStats, error) {
	return b.readStats(ctx, false)
}

// OpenExisting opens a pre-provisioned backend without DDL. Managed services
// can run WithConfig during provisioning/migration and use this on API boot.
func OpenExisting(ctx context.Context, databaseURL, queueName string, leaseMS int64, poolSize int32) (*PostgresBackend, error) {
	if err := validateQueueName(queueName); err != nil {
		return nil, err
	}
	if poolSize < 2 || leaseMS <= 0 {
		return nil, storage.NewConfigurationError("pool size must be >=2 and lease duration positive")
	}
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, storage.NewConfigurationError("invalid PostgreSQL connection configuration")
	}
	cfg.MaxConns = poolSize
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		return nil, err
	}
	current, err := schemaCurrent(ctx, conn)
	conn.Release()
	if err != nil || !current {
		pool.Close()
		if err != nil {
			return nil, err
		}
		return nil, storage.NewConfigurationError("queue schema requires provisioning or migration")
	}
	b := &PostgresBackend{pool: pool, queueName: queueName}
	b.defaultLeaseMS.Store(leaseMS)
	b.sig = newSignaler(b)
	return b, nil
}
