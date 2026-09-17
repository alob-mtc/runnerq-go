package postgres

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"
)

// The expectation is parsed from the DDL, so the parser must see every
// object schemaSql creates: any DDL form it does not recognise would be
// silently skipped by the fast path and never migrated.
func TestSchemaExpectationCoversAllDDL(t *testing.T) {
	e := expectedSchema()
	ddl := schemaCommentRe.ReplaceAllString(schemaSql, "")
	ifNotExists := strings.Count(strings.ToUpper(ddl), "IF NOT EXISTS")
	if got := len(e.tables) + len(e.columns) + len(schemaIndexRe.FindAllString(ddl, -1)); got != ifNotExists {
		t.Fatalf("parsed %d objects from %d IF NOT EXISTS clauses; schemaSql uses a DDL form the parser does not recognise", got, ifNotExists)
	}
	for _, want := range []string{"runnerq_activities", "runnerq_results", "runnerq_dependencies", "runnerq_worker_pools"} {
		if !slices.Contains(e.tables, want) {
			t.Fatalf("tables %v missing %s", e.tables, want)
		}
	}
	for _, want := range []string{"runnerq_activities.root_activity_id", "runnerq_results.owner_activity_id", "runnerq_results.step"} {
		if !slices.Contains(e.columns, want) {
			t.Fatalf("columns %v missing %s", e.columns, want)
		}
	}
	for _, want := range []string{"idx_runnerq_root_status", "idx_runnerq_dequeue_order_v2", "idx_runnerq_dequeue_effective_v2"} {
		if !slices.Contains(e.indexes, want) {
			t.Fatalf("indexes %v missing %s", e.indexes, want)
		}
	}
	if !slices.Contains(e.retired, "idx_runnerq_dequeue_order") {
		t.Fatalf("retired %v missing the superseded dequeue index", e.retired)
	}
}

// On an initialized database a new backend must take no table locks: with a
// row on runnerq_activities locked by an open transaction, the DDL's ACCESS
// EXCLUSIVE would block, so a fast connect proves the DDL did not run.
func TestSchemaInitSkipsDDLWhenCurrent(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if current, err := schemaCurrent(ctx, conn); err != nil || !current {
		t.Fatalf("schema current after init = %v, %v", current, err)
	}

	a := testActivity(1)
	if err := b.Enqueue(ctx, a); err != nil {
		t.Fatal(err)
	}
	tx, _ := pinnedTx(t, b)
	var one int
	if err := tx.QueryRow(ctx, `SELECT 1 FROM runnerq_activities WHERE id = $1 FOR UPDATE`, a.ID).Scan(&one); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	other := testBackendNamedCtx(t, connectCtx, "t_fastpath")
	other.Close()
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("connect took %v with a row lock held; schema init ran DDL on a current schema", took)
	}
}

// A missing object disables the fast path and the next init restores it.
func TestSchemaInitMigratesWhenObjectMissing(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	if _, err := b.pool.Exec(ctx, `DROP INDEX IF EXISTS idx_runnerq_root_status`); err != nil {
		t.Fatal(err)
	}
	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if current, err := schemaCurrent(ctx, conn); err != nil || current {
		t.Fatalf("schema reported current with an index missing: %v, %v", current, err)
	}
	conn.Release()

	testBackendNamed(t, "t_migrate").Close()

	conn, err = b.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if current, err := schemaCurrent(ctx, conn); err != nil || !current {
		t.Fatalf("schema not restored by a fresh init: %v, %v", current, err)
	}
}

// Index names are unique per schema only. A same-named valid index in another
// schema must not satisfy the dequeue-index inspection, or init would skip
// the build and the fast path would never report the schema current.
func TestSchemaInitIgnoresSameNamedIndexInOtherSchema(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	const decoy = "rq_decoy_schema"
	for _, stmt := range []string{
		`CREATE SCHEMA IF NOT EXISTS ` + decoy,
		`CREATE TABLE IF NOT EXISTS ` + decoy + `.t (x INT)`,
		`CREATE INDEX IF NOT EXISTS idx_runnerq_dequeue_order_v2 ON ` + decoy + `.t (x)`,
		`DROP INDEX IF EXISTS public.idx_runnerq_dequeue_order_v2`,
	} {
		if _, err := b.pool.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _, _ = b.pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+decoy+` CASCADE`) })

	testBackendNamed(t, "t_decoy").Close()

	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if current, err := schemaCurrent(ctx, conn); err != nil || !current {
		t.Fatalf("dequeue index not rebuilt in the active schema behind a same-named decoy: current=%v err=%v", current, err)
	}
	var decoyStillThere bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = $1 AND indexname = 'idx_runnerq_dequeue_order_v2')`, decoy).Scan(&decoyStillThere); err != nil {
		t.Fatal(err)
	}
	if !decoyStillThere {
		t.Fatal("init dropped an index that belongs to another schema")
	}
}

// A waiter for the schema lock must not sit inside a blocked
// pg_advisory_lock: that statement pins a snapshot, and the holder's CREATE
// INDEX CONCURRENTLY waits for all older snapshots — a cycle Postgres breaks
// with 40P01. With the lock held externally and the fast path disabled, a
// booting backend must wait without any session blocked on the advisory
// lock, then complete once the lock is released.
func TestSchemaInitWaitsForLockWithoutPinningSnapshot(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	if _, err := b.pool.Exec(ctx, `DROP INDEX IF EXISTS idx_runnerq_root_status`); err != nil {
		t.Fatal(err)
	}

	holder, err := b.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Release()
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_lock($1)`, schemaAdvisoryLockKey); err != nil {
		t.Fatal(err)
	}
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		if _, err := holder.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, schemaAdvisoryLockKey); err != nil {
			t.Errorf("unlock: %v", err)
		}
	}
	defer release()

	connectCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	done := make(chan *PostgresBackend, 1)
	go func() { done <- testBackendNamedCtx(t, connectCtx, "t_lockpoll") }()

	select {
	case other := <-done:
		other.Close()
		t.Fatal("backend initialized while another session held the schema lock")
	case <-time.After(500 * time.Millisecond):
	}
	var blocked int
	if err := holder.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND wait_event = 'advisory'`).Scan(&blocked); err != nil {
		t.Fatal(err)
	}
	if blocked != 0 {
		t.Fatalf("%d session(s) blocked inside pg_advisory_lock while waiting for the schema lock; a blocked waiter pins a snapshot and can deadlock the holder's CREATE INDEX CONCURRENTLY", blocked)
	}

	release()
	select {
	case other := <-done:
		other.Close()
	case <-time.After(15 * time.Second):
		t.Fatal("backend did not initialize after the schema lock was released")
	}

	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if current, err := schemaCurrent(ctx, conn); err != nil || !current {
		t.Fatalf("schema not restored after the waiter initialized: %v, %v", current, err)
	}
}
