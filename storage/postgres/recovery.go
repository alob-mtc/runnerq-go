package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/alob-mtc/runnerq-go/storage"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func claimLost(id uuid.UUID) error {
	return &storage.StorageError{Kind: storage.ErrClaimLost, Message: fmt.Sprintf("activity %s is no longer owned by this execution", id)}
}

func (b *PostgresBackend) ExtendLeaseForWorker(ctx context.Context, id uuid.UUID, workerID string, extendBy time.Duration) (bool, error) {
	if extendBy <= 0 {
		return false, storage.NewConfigurationError("lease extension must be positive")
	}
	tag, err := b.pool.Exec(ctx, `UPDATE runnerq_activities
		SET lease_deadline_ms = GREATEST(lease_deadline_ms, (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint + $1)
		WHERE id = $2 AND queue_name = $3 AND status = 'processing' AND current_worker_id = $4`,
		extendBy.Milliseconds(), id, b.queueName, workerID)
	if err != nil {
		return false, databaseError(err, "failed to renew activity lease")
	}
	return tag.RowsAffected() > 0, nil
}

func (b *PostgresBackend) StoreCheckpoint(ctx context.Context, id, owner uuid.UUID, workerID string, result storage.ActivityResult, step string) error {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return databaseError(err, "failed to begin checkpoint transaction")
	}
	defer tx.Rollback(ctx)
	// Serialize with the reaper and acknowledgements. A stale execution cannot
	// publish checkpoints even though its user function may still be running.
	// This FOR UPDATE on the owner is also what orders the checkpoint against
	// a consumer parking on it (see dependencies.go), so it must stay ahead of
	// the insert and the wake below.
	var claimed int
	err = tx.QueryRow(ctx, `SELECT 1 FROM runnerq_activities
		WHERE id = $1 AND queue_name = $2 AND status = 'processing' AND current_worker_id = $3
		FOR UPDATE`, owner, b.queueName, workerID).Scan(&claimed)
	if err == pgx.ErrNoRows {
		return claimLost(owner)
	}
	if err != nil {
		return databaseError(err, "failed to verify checkpoint ownership")
	}
	state := "Ok"
	if result.State == storage.ResultErr {
		state = "Err"
	}
	tag, err := tx.Exec(ctx, `INSERT INTO runnerq_results
		(activity_id, queue_name, state, data, created_at, owner_activity_id, step)
		VALUES ($1, $2, $3, $4, NOW(), $5, NULLIF($6, ''))
		ON CONFLICT (activity_id) DO NOTHING`, id, b.queueName, state, result.Data, owner, step)
	if err != nil {
		return databaseError(err, "failed to store checkpoint")
	}
	if tag.RowsAffected() == 0 {
		var same bool
		err = tx.QueryRow(ctx, `SELECT queue_name = $2 AND state = $3
			AND data IS NOT DISTINCT FROM $4::jsonb AND owner_activity_id IS NOT DISTINCT FROM $5::uuid
			AND COALESCE(step, '') = $6 FROM runnerq_results WHERE activity_id = $1`,
			id, b.queueName, state, result.Data, owner, step).Scan(&same)
		if err != nil {
			return databaseError(err, "failed to reconcile checkpoint")
		}
		if !same {
			return &storage.StorageError{Kind: storage.ErrCheckpointConflict, Message: fmt.Sprintf("checkpoint %s already has a different outcome", id)}
		}
		return nil
	}
	if err := b.recordEvent(ctx, tx, id, storage.EventResultStored, &workerID, toDetail(map[string]any{"result_stored": true, "state": state})); err != nil {
		return err
	}
	if err := b.wakeResultWaitersTx(ctx, tx, id); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return databaseError(err, "failed to commit checkpoint")
	}
	b.signalEvent()
	b.signalResult(id)
	b.signalWork()
	return nil
}
