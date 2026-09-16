package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Schema DDL is expensive even when it changes nothing: ALTER TABLE ... ADD
// COLUMN IF NOT EXISTS takes ACCESS EXCLUSIVE and CREATE INDEX IF NOT EXISTS
// takes SHARE on the table before either evaluates its IF NOT EXISTS, so
// every process start stalls live claims, acks and parks in every other
// process and can be picked as a deadlock victim by them. initSchema
// therefore first asks the catalog whether everything the DDL would create
// already exists and, if so, runs no DDL at all.
//
// The expectation is parsed from the DDL itself rather than maintained by
// hand, so adding a table, column or index to schemaSql (or an entry to
// dequeueIndexes) extends the check automatically; TestSchemaExpectation
// guards the parser against DDL forms it does not recognise.

type schemaExpectation struct {
	tables  []string // CREATE TABLE IF NOT EXISTS
	columns []string // "table.column" from ALTER TABLE ... ADD COLUMN IF NOT EXISTS
	indexes []string // CREATE INDEX IF NOT EXISTS, plus dequeueIndexes (must be valid)
	retired []string // dequeueIndexes predecessors (must be gone)
}

var (
	schemaCommentRe = regexp.MustCompile(`(?m)--[^\n]*`)
	schemaTableRe   = regexp.MustCompile(`(?i)CREATE TABLE IF NOT EXISTS (\w+)`)
	schemaIndexRe   = regexp.MustCompile(`(?i)CREATE (?:UNIQUE )?INDEX (?:CONCURRENTLY )?IF NOT EXISTS (\w+)`)
	schemaAlterRe   = regexp.MustCompile(`(?is)ALTER TABLE (\w+)(.*?);`)
	schemaColumnRe  = regexp.MustCompile(`(?i)ADD COLUMN IF NOT EXISTS (\w+)`)
)

var expectedSchema = sync.OnceValue(func() schemaExpectation {
	ddl := schemaCommentRe.ReplaceAllString(schemaSql, "")
	var e schemaExpectation
	for _, m := range schemaTableRe.FindAllStringSubmatch(ddl, -1) {
		e.tables = append(e.tables, strings.ToLower(m[1]))
	}
	for _, m := range schemaIndexRe.FindAllStringSubmatch(ddl, -1) {
		e.indexes = append(e.indexes, strings.ToLower(m[1]))
	}
	for _, alter := range schemaAlterRe.FindAllStringSubmatch(ddl, -1) {
		table := strings.ToLower(alter[1])
		for _, col := range schemaColumnRe.FindAllStringSubmatch(alter[2], -1) {
			e.columns = append(e.columns, table+"."+strings.ToLower(col[1]))
		}
	}
	for _, idx := range dequeueIndexes {
		e.indexes = append(e.indexes, strings.ToLower(idx.name))
		e.retired = append(e.retired, strings.ToLower(idx.dropAfter))
	}
	return e
})

// schemaCurrent reports whether every table, column and index the DDL would
// create already exists (indexes valid) and every retired index is gone, in
// the connection's current schema. One catalog round trip, no table locks.
func schemaCurrent(ctx context.Context, conn *pgxpool.Conn) (bool, error) {
	e := expectedSchema()
	var current bool
	err := conn.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM information_schema.tables
		        WHERE table_schema = current_schema() AND table_name = ANY($1)) = cardinality($1::text[])
		   AND (SELECT count(*) FROM information_schema.columns
		        WHERE table_schema = current_schema() AND table_name || '.' || column_name = ANY($2)) = cardinality($2::text[])
		   AND (SELECT count(*) FROM pg_index i
		        JOIN pg_class c ON c.oid = i.indexrelid
		        JOIN pg_namespace n ON n.oid = c.relnamespace
		        WHERE n.nspname = current_schema() AND c.relname = ANY($3) AND i.indisvalid) = cardinality($3::text[])
		   AND NOT EXISTS (SELECT 1 FROM pg_class c
		        JOIN pg_namespace n ON n.oid = c.relnamespace
		        WHERE n.nspname = current_schema() AND c.relname = ANY($4))`,
		e.tables, e.columns, e.indexes, e.retired).Scan(&current)
	if err != nil {
		return false, databaseError(err, fmt.Sprintf("Failed to inspect schema: %v", err))
	}
	return current, nil
}

// isDeadlock reports a Postgres "deadlock detected" error. Schema DDL run
// against live traffic (a rolling deploy, parallel test packages) can be
// chosen as the victim; the DDL is idempotent, so it is simply re-run.
func isDeadlock(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40P01"
}

// retryDeadlock runs op until it succeeds or fails with anything other than
// a deadlock, backing off briefly between attempts.
func retryDeadlock(ctx context.Context, what string, op func() error) error {
	const attempts = 5
	var err error
	for i := range attempts {
		if err = op(); err == nil || !isDeadlock(err) {
			return err
		}
		slog.Warn("runnerq: schema statement lost a deadlock; retrying", "step", what, "attempt", i+1)
		select {
		case <-time.After(time.Duration(50<<i) * time.Millisecond):
		case <-ctx.Done():
			return err
		}
	}
	return err
}
