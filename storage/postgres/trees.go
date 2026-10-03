package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// lockTreeForDeleteTx locks the tree's idempotency keys and reports whether a
// live workflow in another tree depends on a result this tree produced (the
// tree must then be kept). Callers (retention, delete) hold the root FOR UPDATE.
//
// Key reuse (BehaviorReturnExisting) registers a dependency while holding the
// child's idempotency row FOR UPDATE and never takes the root, so the key rows
// are the ordering point: locking them before the dependency check means a
// reuse in flight either commits first (its dependency pins the tree) or waits,
// then finds the key gone and claims it fresh. Order is root → keys here and
// key only there, so no cycle.
func (b *PostgresBackend) lockTreeForDeleteTx(ctx context.Context, tx pgx.Tx, root uuid.UUID) (pinned bool, err error) {
	// A key's activity carries the key, so each key is a primary-key probe.
	if _, err := tx.Exec(ctx, `
		SELECT 1 FROM runnerq_idempotency k
		JOIN (`+treeSQL+`) t ON k.queue_name = $1 AND k.idempotency_key = t.idempotency_key AND k.activity_id = t.id
		FOR UPDATE OF k`, b.queueName, root); err != nil {
		return false, databaseError(err, "failed to lock tree idempotency keys")
	}
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM (`+treeSQL+`) t
			JOIN runnerq_dependencies d ON d.queue_name = $1 AND d.result_id = t.id AND d.producer_activity_id = t.id
			JOIN runnerq_activities waiter ON waiter.id = d.waiter_activity_id AND waiter.queue_name = $1
			WHERE waiter.root_activity_id <> $2
			  AND EXISTS (
				SELECT 1 FROM runnerq_activities live
				WHERE live.queue_name = $1
				  AND (live.id = waiter.root_activity_id
					OR (live.root_activity_id = waiter.root_activity_id AND live.parent_activity_id IS NOT NULL))
				  AND live.status NOT IN ('completed', 'failed', 'dead_letter', 'cancelled'))
		)`, b.queueName, root).Scan(&pinned); err != nil {
		return false, databaseError(err, "failed to check tree dependencies")
	}
	return pinned, nil
}

func (b *PostgresBackend) deleteTreeTx(ctx context.Context, tx pgx.Tx, root uuid.UUID) (int64, error) {
	var deleted int64
	err := tx.QueryRow(ctx, `
		WITH tree AS (`+treeSQL+`),
		del_dependencies AS (
			DELETE FROM runnerq_dependencies WHERE queue_name = $1
			  AND (waiter_activity_id IN (SELECT id FROM tree)
				OR (result_id IN (SELECT id FROM tree) AND producer_activity_id IS NOT NULL))
		),
		del_results AS (
			DELETE FROM runnerq_results
			WHERE queue_name = $1
			  AND (activity_id IN (SELECT id FROM tree) OR owner_activity_id IN (SELECT id FROM tree))
			RETURNING activity_id
		),
		del_events AS (
			DELETE FROM runnerq_events
			WHERE queue_name = $1
			  AND (activity_id IN (SELECT id FROM tree) OR activity_id IN (SELECT activity_id FROM del_results))
		),
		del_idem AS (
			DELETE FROM runnerq_idempotency k USING tree
			WHERE k.queue_name = $1 AND k.idempotency_key = tree.idempotency_key AND k.activity_id = tree.id
		),
		del_inputs AS (
			DELETE FROM runnerq_inputs WHERE activity_id IN (SELECT id FROM tree)
		),
		del_act AS (
			DELETE FROM runnerq_activities WHERE queue_name = $1 AND id IN (SELECT id FROM tree)
			RETURNING id
		)
		SELECT count(*) FROM del_act`, b.queueName, root).Scan(&deleted)
	return deleted, err
}

// treeSQL selects the workflow rooted at $2 in queue $1: the root by id, its
// descendants through idx_runnerq_root_children.
const treeSQL = `
	SELECT a.id, a.idempotency_key FROM runnerq_activities a
	WHERE a.queue_name = $1
	  AND (a.id = $2 OR (a.root_activity_id = $2 AND a.parent_activity_id IS NOT NULL))`
