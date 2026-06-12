package postgres

// Integration tests for the retention sweeper (CleanupExpired). Skipped
// unless RUNNERQ_TEST_DSN is set — see integration_test.go.

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
)

// retentionTree is the set of IDs a planted workflow tree owns, for asserting
// presence/absence across every table after a sweep.
type retentionTree struct {
	root, child  uuid.UUID
	checkpointID uuid.UUID // Run/Sleep-style result row owned by the root
	idemKey      string
}

// plantTree inserts a two-activity workflow tree directly via SQL: root +
// child with the given statuses, completed terminalAge ago, plus events for
// both, a result row for the child, a synthetic checkpoint result owned by
// the root (no matching activity row — like ctx.Run/Sleep writes), and an
// idempotency key pointing at the child.
func plantTree(t *testing.T, b *PostgresBackend, rootStatus, childStatus string, terminalAge time.Duration) retentionTree {
	t.Helper()
	ctx := context.Background()
	tree := retentionTree{
		root:         uuid.New(),
		child:        uuid.New(),
		checkpointID: uuid.New(),
	}
	tree.idemKey = "rq:step:" + tree.root.String() + ":" + tree.root.String() + ":reserve"
	completedAt := time.Now().UTC().Add(-terminalAge)

	insert := func(id uuid.UUID, status string, parent *uuid.UUID) {
		var cAt *time.Time
		if status == "completed" || status == "failed" || status == "dead_letter" {
			cAt = &completedAt
		}
		if _, err := b.pool.Exec(ctx, `
			INSERT INTO runnerq_activities (id, queue_name, activity_type, payload, priority, status,
				created_at, completed_at, parent_activity_id, root_activity_id, depth)
			VALUES ($1, $2, 'tree_member', '{}'::jsonb, 2, $3, $4, $5, $6, $7, $8)`,
			id, b.queueName, status, completedAt.Add(-time.Minute), cAt, parent, tree.root, 0); err != nil {
			t.Fatalf("insert activity: %v", err)
		}
		if _, err := b.pool.Exec(ctx, `
			INSERT INTO runnerq_events (activity_id, queue_name, event_type, created_at)
			VALUES ($1, $2, 'Enqueued', $3)`, id, b.queueName, completedAt.Add(-time.Minute)); err != nil {
			t.Fatalf("insert event: %v", err)
		}
	}
	insert(tree.root, rootStatus, nil)
	insert(tree.child, childStatus, &tree.root)

	if _, err := b.pool.Exec(ctx, `
		INSERT INTO runnerq_results (activity_id, queue_name, state, data, created_at, owner_activity_id)
		VALUES ($1, $2, 'Ok', '{}'::jsonb, $3, $1), ($4, $2, 'Ok', '{}'::jsonb, $3, $5)`,
		tree.child, b.queueName, completedAt, tree.checkpointID, tree.root); err != nil {
		t.Fatalf("insert results: %v", err)
	}
	// Event recorded under the checkpoint's synthetic ID, as StoreResult does.
	if _, err := b.pool.Exec(ctx, `
		INSERT INTO runnerq_events (activity_id, queue_name, event_type, created_at)
		VALUES ($1, $2, 'ResultStored', $3)`, tree.checkpointID, b.queueName, completedAt); err != nil {
		t.Fatalf("insert checkpoint event: %v", err)
	}
	if _, err := b.pool.Exec(ctx, `
		INSERT INTO runnerq_idempotency (queue_name, idempotency_key, activity_id, created_at, updated_at)
		VALUES ($1, $2, $3, NOW(), NOW())`, b.queueName, tree.idemKey, tree.child); err != nil {
		t.Fatalf("insert idempotency: %v", err)
	}
	return tree
}

// rowCounts returns how many rows each table still holds for the tree.
func rowCounts(t *testing.T, b *PostgresBackend, tree retentionTree) (activities, events, results, idem int) {
	t.Helper()
	ctx := context.Background()
	ids := []uuid.UUID{tree.root, tree.child, tree.checkpointID}
	queries := []struct {
		dst *int
		sql string
	}{
		{&activities, `SELECT COUNT(*) FROM runnerq_activities WHERE queue_name=$1 AND id = ANY($2)`},
		{&events, `SELECT COUNT(*) FROM runnerq_events WHERE queue_name=$1 AND activity_id = ANY($2)`},
		{&results, `SELECT COUNT(*) FROM runnerq_results WHERE queue_name=$1 AND activity_id = ANY($2)`},
	}
	for _, q := range queries {
		if err := b.pool.QueryRow(ctx, q.sql, b.queueName, ids).Scan(q.dst); err != nil {
			t.Fatalf("count: %v", err)
		}
	}
	if err := b.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM runnerq_idempotency WHERE queue_name=$1 AND idempotency_key=$2`,
		b.queueName, tree.idemKey).Scan(&idem); err != nil {
		t.Fatalf("count idem: %v", err)
	}
	return
}

func assertTreePresent(t *testing.T, b *PostgresBackend, tree retentionTree, label string) {
	t.Helper()
	if a, e, r, i := rowCounts(t, b, tree); a != 2 || e != 3 || r != 2 || i != 1 {
		t.Fatalf("%s: counts activities=%d events=%d results=%d idem=%d, want 2/3/2/1", label, a, e, r, i)
	}
}

func assertTreeGone(t *testing.T, b *PostgresBackend, tree retentionTree, label string) {
	t.Helper()
	if a, e, r, i := rowCounts(t, b, tree); a != 0 || e != 0 || r != 0 || i != 0 {
		t.Fatalf("%s: counts activities=%d events=%d results=%d idem=%d, want all 0", label, a, e, r, i)
	}
}

// The sweep deletes a whole expired terminal tree — activities, events,
// results (including checkpoint rows with synthetic IDs, by owner), the
// checkpoint's events, and idempotency keys — while leaving fresh trees and
// trees with non-terminal descendants untouched.
func TestCleanupExpiredSweepsTerminalTrees(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	oldDone := plantTree(t, b, "completed", "completed", 2*time.Hour)
	fresh := plantTree(t, b, "completed", "completed", time.Minute)
	guarded := plantTree(t, b, "completed", "processing", 2*time.Hour) // child still running

	n, err := b.CleanupExpired(ctx, storage.RetentionPolicy{Completed: time.Hour}, 100)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if n != 1 {
		t.Fatalf("swept %d trees, want exactly 1", n)
	}
	assertTreeGone(t, b, oldDone, "expired tree")
	assertTreePresent(t, b, fresh, "fresh tree")
	assertTreePresent(t, b, guarded, "tree with running child")
}

// Completed and failed trees age on separate clocks; zero TTL keeps forever.
func TestCleanupExpiredSeparateTTLs(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	oldCompleted := plantTree(t, b, "completed", "completed", 2*time.Hour)
	oldFailed := plantTree(t, b, "dead_letter", "completed", 2*time.Hour)

	// Completed: keep forever (0). Failed: 1h TTL.
	n, err := b.CleanupExpired(ctx, storage.RetentionPolicy{Failed: time.Hour}, 100)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if n != 1 {
		t.Fatalf("swept %d trees, want 1 (only the failed one)", n)
	}
	assertTreeGone(t, b, oldFailed, "expired failed tree")
	assertTreePresent(t, b, oldCompleted, "completed tree under zero TTL")
}

// Only one sweeper runs per queue: while another session holds the advisory
// lock, CleanupExpired no-ops instead of contending.
func TestCleanupExpiredLeadership(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	tree := plantTree(t, b, "completed", "completed", 2*time.Hour)

	holder, err := b.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin holder tx: %v", err)
	}
	var got bool
	if err := holder.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1::int4, hashtext($2))`,
		cleanupLockClass, b.queueName).Scan(&got); err != nil || !got {
		t.Fatalf("holder lock: got=%v err=%v", got, err)
	}

	n, err := b.CleanupExpired(ctx, storage.RetentionPolicy{Completed: time.Hour}, 100)
	if err != nil {
		t.Fatalf("cleanup under contention: %v", err)
	}
	if n != 0 {
		t.Fatalf("swept %d trees while another sweeper held the lock, want 0", n)
	}
	assertTreePresent(t, b, tree, "tree during contention")

	if err := holder.Rollback(ctx); err != nil {
		t.Fatalf("release holder: %v", err)
	}
	n, err = b.CleanupExpired(ctx, storage.RetentionPolicy{Completed: time.Hour}, 100)
	if err != nil || n != 1 {
		t.Fatalf("cleanup after release: n=%d err=%v, want 1", n, err)
	}
	assertTreeGone(t, b, tree, "tree after release")
}

// Batch size limits roots per call; repeated calls drain the backlog.
func TestCleanupExpiredBatches(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		plantTree(t, b, "completed", "completed", 2*time.Hour)
	}

	total := uint64(0)
	for i := 0; i < 5 && total < 3; i++ {
		n, err := b.CleanupExpired(ctx, storage.RetentionPolicy{Completed: time.Hour}, 2)
		if err != nil {
			t.Fatalf("cleanup pass %d: %v", i, err)
		}
		if n > 2 {
			t.Fatalf("batch returned %d roots, cap was 2", n)
		}
		if n == 0 {
			break
		}
		total += n
	}
	if total != 3 {
		t.Fatalf("drained %d trees, want 3", total)
	}
	var remaining int
	if err := b.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM runnerq_activities WHERE queue_name=$1`, b.queueName).Scan(&remaining); err != nil {
		t.Fatalf("count: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("%d activities remain after drain, want 0", remaining)
	}
}
