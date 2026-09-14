package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/alob-mtc/runnerq-go/storage"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Result publication and durable parking take turns for one logical result.
// The lock is transaction-scoped and never accumulated across a workflow.
const dependencyResultLockClass = int32(1381913430)

func (b *PostgresBackend) lockResultTx(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
	// Hash collisions serialize unrelated results; they cannot merge identities.
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1::int4, hashtext($2))`, dependencyResultLockClass, b.queueName+":"+id.String())
	if err != nil {
		return databaseError(err, "failed to serialize result publication")
	}
	return nil
}

// lockProducerRootTx serializes dependency registration with retention for
// one workflow tree. The root row is the single coordination point for every
// result in that tree; it is held only until this transaction commits. This
// avoids a queue-wide advisory lock and avoids retaining a lock per result.
func (b *PostgresBackend) lockProducerRootTx(ctx context.Context, tx pgx.Tx, producer uuid.UUID) (bool, error) {
	var root uuid.UUID
	err := tx.QueryRow(ctx, `
		SELECT root.id
		FROM runnerq_activities producer
		JOIN runnerq_activities root
		  ON root.id = COALESCE(producer.root_activity_id, producer.id)
		 AND root.queue_name = producer.queue_name
		WHERE producer.id = $1 AND producer.queue_name = $2
		FOR KEY SHARE OF root`, producer, b.queueName).Scan(&root)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, databaseError(err, "failed to lock dependency producer root")
	}
	return true, nil
}
func (b *PostgresBackend) addDependencyTx(ctx context.Context, tx pgx.Tx, waiter, result uuid.UUID, producer *uuid.UUID) error {
	_, err := tx.Exec(ctx, `INSERT INTO runnerq_dependencies(queue_name,waiter_activity_id,result_id,producer_activity_id)
 VALUES($1,$2,$3,$4) ON CONFLICT(queue_name,waiter_activity_id,result_id) DO NOTHING`, b.queueName, waiter, result, producer)
	if err != nil {
		return databaseError(err, "failed to register result dependency")
	}
	return nil
}
func (b *PostgresBackend) linkChildTx(ctx context.Context, tx pgx.Tx, parent *uuid.UUID, child uuid.UUID) error {
	if parent == nil {
		return nil
	}
	// Existing APIs permit imported lineage with no local parent. Register only
	// live local parents; never manufacture an uncollectable reference.
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runnerq_activities WHERE id=$1 AND queue_name=$2)`, *parent, b.queueName).Scan(&exists); err != nil {
		return databaseError(err, "failed to find dependency parent")
	}
	if !exists {
		return nil
	}
	return b.addDependencyTx(ctx, tx, *parent, child, &child)
}
func (b *PostgresBackend) verifyClaimTx(ctx context.Context, tx pgx.Tx, id uuid.UUID, worker string) error {
	var n int
	err := tx.QueryRow(ctx, `SELECT 1 FROM runnerq_activities WHERE id=$1 AND queue_name=$2 AND status='processing' AND current_worker_id=$3 FOR UPDATE`, id, b.queueName, worker).Scan(&n)
	if err == pgx.ErrNoRows {
		return claimLost(id)
	}
	if err != nil {
		return databaseError(err, "failed to verify execution claim")
	}
	return nil
}
func (b *PostgresBackend) RegisterDependency(ctx context.Context, waiter, result uuid.UUID, worker string) error {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return databaseError(err, "failed to begin dependency transaction")
	}
	defer tx.Rollback(ctx)
	// A dependency and retention must take turns for the producer's workflow
	// tree, but unrelated queues and trees remain independent.
	if exists, err := b.lockProducerRootTx(ctx, tx, result); err != nil {
		return err
	} else if !exists {
		return storage.NewNotFoundError(fmt.Sprintf("activity result producer %s no longer exists in this queue", result))
	}
	if err := b.verifyClaimTx(ctx, tx, waiter, worker); err != nil {
		return err
	}
	if err := b.addDependencyTx(ctx, tx, waiter, result, &result); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return databaseError(err, "failed to commit dependency")
	}
	return nil
}
func (b *PostgresBackend) wakeResultWaitersTx(ctx context.Context, tx pgx.Tx, result uuid.UUID) error {
	_, err := tx.Exec(ctx, `UPDATE runnerq_activities a SET status='pending', scheduled_at=NULL
 WHERE a.queue_name=$1 AND a.status='waiting' AND EXISTS(
 SELECT 1 FROM runnerq_dependencies d WHERE d.queue_name=$1 AND d.result_id=$2 AND d.waiter_activity_id=a.id)`, b.queueName, result)
	if err != nil {
		return databaseError(err, "failed to wake result consumers")
	}
	return nil
}
func (b *PostgresBackend) YieldForResult(ctx context.Context, waiter, result uuid.UUID, producer *uuid.UUID, wakeAt time.Time, worker, kind, step string) error {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return databaseError(err, "failed to begin durable park")
	}
	defer tx.Rollback(ctx)
	if err := b.lockResultTx(ctx, tx, result); err != nil {
		return err
	}
	if err := b.verifyClaimTx(ctx, tx, waiter, worker); err != nil {
		// A lost reply can be reconciled even if an early wake or next claim has
		// occurred. The durable event identifies this exact execution's park.
		if se, ok := storage.IsStorageError(err); !ok || se.Kind != storage.ErrClaimLost {
			return err
		}
		var recorded bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runnerq_events WHERE queue_name=$1 AND activity_id=$2 AND worker_id=$3 AND event_type='Yielded' AND detail->>'result_id'=$4 AND detail->>'kind'=$5 AND detail->>'step'=$6)`, b.queueName, waiter, worker, result.String(), kind, step).Scan(&recorded); err != nil {
			return databaseError(err, "failed to reconcile durable park")
		}
		if recorded {
			return nil
		}
		return claimLost(waiter)
	}
	if producer != nil {
		exists, err := b.lockProducerRootTx(ctx, tx, *producer)
		if err != nil {
			return err
		}
		if !exists {
			return storage.NewNotFoundError("park result producer no longer exists")
		}
	}
	if err := b.addDependencyTx(ctx, tx, waiter, result, producer); err != nil {
		return err
	}
	var ready bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runnerq_results WHERE activity_id=$1 AND queue_name=$2)`, result, b.queueName).Scan(&ready); err != nil {
		return databaseError(err, "failed to check park readiness")
	}
	_, err = tx.Exec(ctx, `UPDATE runnerq_activities SET status=CASE WHEN $5 THEN 'pending' ELSE 'waiting' END,
 scheduled_at=CASE WHEN $5 THEN NULL ELSE $3::timestamptz END, last_worker_id=$4,current_worker_id=NULL,lease_deadline_ms=NULL,started_at=NULL
 WHERE id=$1 AND queue_name=$2`, waiter, b.queueName, wakeAt, worker, ready)
	if err != nil {
		return databaseError(err, "failed to park result consumer")
	}
	if err := b.recordEvent(ctx, tx, waiter, storage.EventYielded, &worker, toDetail(map[string]any{"result_id": result, "kind": kind, "step": step, "wake_at": wakeAt, "ready": ready})); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return databaseError(err, "failed to commit durable park")
	}
	b.signalEvent()
	if ready || !wakeAt.After(time.Now()) {
		b.signalWork()
	}
	return nil
}
