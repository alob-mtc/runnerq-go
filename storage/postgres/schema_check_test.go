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
