package postgres

// Integration tests against a real PostgreSQL instance. They are skipped
// unless RUNNERQ_TEST_DSN is set, e.g.:
//
//	docker run -d --rm -e POSTGRES_PASSWORD=test -e POSTGRES_DB=runnerq_test -p 55432:5432 postgres:16-alpine
//	RUNNERQ_TEST_DSN='postgres://postgres:test@localhost:55432/runnerq_test' go test ./...
//
// Each test uses a fresh random queue name, so tests are isolated and can run
// in parallel against one database.

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
)

func testBackend(t *testing.T) *PostgresBackend {
	t.Helper()
	dsn := os.Getenv("RUNNERQ_TEST_DSN")
	if dsn == "" {
		t.Skip("RUNNERQ_TEST_DSN not set; skipping integration test")
	}
	queueName := "t_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
	b, err := WithConfig(context.Background(), dsn, queueName, 30_000, 5)
	if err != nil {
		t.Fatalf("connect backend: %v", err)
	}
	t.Cleanup(b.Close)
	return b
}

func testActivity(maxRetries uint32) storage.QueuedActivity {
	return storage.QueuedActivity{
		ID:             uuid.New(),
		ActivityType:   "test_activity",
		Payload:        json.RawMessage(`{"k":"v"}`),
		Priority:       storage.PriorityNormal,
		MaxRetries:     maxRetries,
		TimeoutSeconds: 30,
		CreatedAt:      time.Now().UTC(),
		Metadata:       map[string]string{},
	}
}

// expireLease force-expires an activity's lease so the reaper sees it.
func expireLease(t *testing.T, b *PostgresBackend, id uuid.UUID) {
	t.Helper()
	_, err := b.pool.Exec(context.Background(), `
		UPDATE runnerq_activities
		SET lease_deadline_ms = (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint - 10000
		WHERE id = $1`, id)
	if err != nil {
		t.Fatalf("expire lease: %v", err)
	}
}

func activityStatus(t *testing.T, b *PostgresBackend, id uuid.UUID) (status string, retryCount int32) {
	t.Helper()
	err := b.pool.QueryRow(context.Background(),
		`SELECT status, retry_count FROM runnerq_activities WHERE id = $1`, id).
		Scan(&status, &retryCount)
	if err != nil {
		t.Fatalf("read activity row: %v", err)
	}
	return status, retryCount
}

func hasEvent(t *testing.T, b *PostgresBackend, id uuid.UUID, eventType string) bool {
	t.Helper()
	events, err := b.GetActivityEvents(context.Background(), id, 100)
	if err != nil {
		t.Fatalf("get events: %v", err)
	}
	for _, e := range events {
		if e.EventType == eventType {
			return true
		}
	}
	return false
}

// Tier 0.1: completion and result storage are atomic, and a result row is
// written even for a nil handler result so awaiting parents always resolve.
func TestAckSuccessAlwaysStoresResult(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	a := testActivity(3)
	if err := b.Enqueue(ctx, a); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	claimed, err := b.Dequeue(ctx, "w1", time.Second, nil)
	if err != nil || claimed == nil {
		t.Fatalf("dequeue: claimed=%v err=%v", claimed, err)
	}

	if err := b.AckSuccess(ctx, a.ID, nil, "w1"); err != nil {
		t.Fatalf("ack success: %v", err)
	}

	res, err := b.GetResult(ctx, a.ID)
	if err != nil {
		t.Fatalf("get result: %v", err)
	}
	if res == nil {
		t.Fatal("completed activity has no result row — awaiting parents would hang forever")
	}
	if res.State != storage.ResultOk {
		t.Fatalf("result state = %v, want Ok", res.State)
	}
	if status, _ := activityStatus(t, b, a.ID); status != "completed" {
		t.Fatalf("status = %q, want completed", status)
	}
}

// Tier 0.2 (backend half): a worker whose lease expired and whose activity was
// reclaimed by another worker cannot ack it.
func TestStaleWorkerCannotAck(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	a := testActivity(5)
	if err := b.Enqueue(ctx, a); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if claimed, err := b.Dequeue(ctx, "stale-worker", time.Second, nil); err != nil || claimed == nil {
		t.Fatalf("first dequeue: claimed=%v err=%v", claimed, err)
	}

	expireLease(t, b, a.ID)
	if n, err := b.RequeueExpired(ctx, 10); err != nil || n != 1 {
		t.Fatalf("requeue expired: n=%d err=%v", n, err)
	}
	if claimed, err := b.Dequeue(ctx, "fresh-worker", time.Second, nil); err != nil || claimed == nil {
		t.Fatalf("second dequeue: claimed=%v err=%v", claimed, err)
	}

	if err := b.AckSuccess(ctx, a.ID, json.RawMessage(`"stale"`), "stale-worker"); err == nil {
		t.Fatal("stale worker ack succeeded; it must be fenced out")
	}
	if status, _ := activityStatus(t, b, a.ID); status != "processing" {
		t.Fatalf("status after stale ack = %q, want processing (still owned by fresh-worker)", status)
	}
	if err := b.AckSuccess(ctx, a.ID, json.RawMessage(`"fresh"`), "fresh-worker"); err != nil {
		t.Fatalf("owning worker ack failed: %v", err)
	}
}

// Tier 0.3: the idempotency-key claim and the enqueue commit atomically, and
// duplicate spawns get the existing activity back.
func TestEnqueueIdempotentClaimAndReuse(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	first := testActivity(3)
	first.IdempotencyKey = &storage.IdempotencyKeyConfig{Key: "job-42", Behavior: storage.BehaviorReturnExisting}
	existing, err := b.EnqueueIdempotent(ctx, &first)
	if err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	if existing != nil {
		t.Fatalf("first enqueue returned existing %v, want fresh claim", existing.ExistingID)
	}
	if status, _ := activityStatus(t, b, first.ID); status != "pending" {
		t.Fatalf("first activity status = %q, want pending", status)
	}

	second := testActivity(3)
	second.IdempotencyKey = first.IdempotencyKey
	existing, err = b.EnqueueIdempotent(ctx, &second)
	if err != nil {
		t.Fatalf("second enqueue: %v", err)
	}
	if existing == nil || existing.ExistingID != first.ID {
		t.Fatalf("second enqueue existing = %+v, want %s", existing, first.ID)
	}
	var count int
	if err := b.pool.QueryRow(ctx, `SELECT COUNT(*) FROM runnerq_activities WHERE id = $1`, second.ID).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Fatal("duplicate spawn created a second activity row")
	}
}

// Tier 0.3: a key orphaned by the old non-atomic claim/enqueue split (key row
// pointing at an activity that doesn't exist) is repaired instead of bricking
// every future spawn with that key.
func TestEnqueueIdempotentRepairsOrphanedKey(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	// Simulate the legacy crash window: claimed key, no activity row.
	if _, err := b.pool.Exec(ctx, `
		INSERT INTO runnerq_idempotency (queue_name, idempotency_key, activity_id, created_at, updated_at)
		VALUES ($1, $2, $3, NOW(), NOW())`,
		b.queueName, "orphan-key", uuid.New()); err != nil {
		t.Fatalf("plant orphan: %v", err)
	}

	a := testActivity(3)
	a.IdempotencyKey = &storage.IdempotencyKeyConfig{Key: "orphan-key", Behavior: storage.BehaviorReturnExisting}
	existing, err := b.EnqueueIdempotent(ctx, &a)
	if err != nil {
		t.Fatalf("enqueue over orphan: %v", err)
	}
	if existing != nil {
		t.Fatalf("orphaned key returned existing %v; it points at nothing and must be reclaimed", existing.ExistingID)
	}
	if status, _ := activityStatus(t, b, a.ID); status != "pending" {
		t.Fatalf("status = %q, want pending", status)
	}
	var pointsAt uuid.UUID
	if err := b.pool.QueryRow(ctx, `
		SELECT activity_id FROM runnerq_idempotency
		WHERE queue_name = $1 AND idempotency_key = $2`,
		b.queueName, "orphan-key").Scan(&pointsAt); err != nil {
		t.Fatalf("read key: %v", err)
	}
	if pointsAt != a.ID {
		t.Fatalf("key points at %s, want %s", pointsAt, a.ID)
	}
}

// Tier 0.4: lease expiry counts as a failed attempt; exhausted activities go
// to the dead-letter queue (with a result row so parents resolve) instead of
// looping forever, and both paths record events.
func TestReaperRetryAccountingAndDeadLetter(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	a := testActivity(2) // dead-letter on the second expired lease
	if err := b.Enqueue(ctx, a); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if claimed, err := b.Dequeue(ctx, "w1", time.Second, nil); err != nil || claimed == nil {
		t.Fatalf("dequeue 1: claimed=%v err=%v", claimed, err)
	}
	expireLease(t, b, a.ID)
	if n, err := b.RequeueExpired(ctx, 10); err != nil || n != 1 {
		t.Fatalf("requeue 1: n=%d err=%v", n, err)
	}
	status, retryCount := activityStatus(t, b, a.ID)
	if status != "pending" || retryCount != 1 {
		t.Fatalf("after reap 1: status=%q retry_count=%d, want pending/1", status, retryCount)
	}
	if !hasEvent(t, b, a.ID, storage.EventRequeued) {
		t.Fatal("no Requeued event recorded for reaped activity")
	}

	if claimed, err := b.Dequeue(ctx, "w2", time.Second, nil); err != nil || claimed == nil {
		t.Fatalf("dequeue 2: claimed=%v err=%v", claimed, err)
	}
	expireLease(t, b, a.ID)
	if n, err := b.RequeueExpired(ctx, 10); err != nil || n != 1 {
		t.Fatalf("requeue 2: n=%d err=%v", n, err)
	}
	status, retryCount = activityStatus(t, b, a.ID)
	if status != "dead_letter" || retryCount != 2 {
		t.Fatalf("after reap 2: status=%q retry_count=%d, want dead_letter/2", status, retryCount)
	}
	if !hasEvent(t, b, a.ID, storage.EventDeadLetter) {
		t.Fatal("no DeadLetter event recorded")
	}
	res, err := b.GetResult(ctx, a.ID)
	if err != nil {
		t.Fatalf("get result: %v", err)
	}
	if res == nil || res.State != storage.ResultErr {
		t.Fatalf("dead-lettered activity result = %+v, want Err result so parents resolve", res)
	}
}

// Tier 0.5: lease deadlines come from the database clock and ExtendLease
// pushes them forward; a non-processing activity cannot have its lease
// extended.
func TestLeaseUsesDBClockAndExtends(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	a := testActivity(3)
	if err := b.Enqueue(ctx, a); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if claimed, err := b.Dequeue(ctx, "w1", time.Second, nil); err != nil || claimed == nil {
		t.Fatalf("dequeue: claimed=%v err=%v", claimed, err)
	}

	var leaseMS int64
	var dbNowMS int64
	if err := b.pool.QueryRow(ctx, `
		SELECT lease_deadline_ms, (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint
		FROM runnerq_activities WHERE id = $1`, a.ID).Scan(&leaseMS, &dbNowMS); err != nil {
		t.Fatalf("read lease: %v", err)
	}
	if leaseMS <= dbNowMS {
		t.Fatalf("fresh lease %d is not in the database's future (db now %d)", leaseMS, dbNowMS)
	}

	ok, err := b.ExtendLease(ctx, a.ID, 10*time.Minute)
	if err != nil || !ok {
		t.Fatalf("extend lease: ok=%v err=%v", ok, err)
	}
	var extendedMS int64
	if err := b.pool.QueryRow(ctx,
		`SELECT lease_deadline_ms FROM runnerq_activities WHERE id = $1`, a.ID).Scan(&extendedMS); err != nil {
		t.Fatalf("read extended lease: %v", err)
	}
	if extendedMS <= leaseMS {
		t.Fatalf("extended lease %d not after original %d", extendedMS, leaseMS)
	}

	if err := b.AckSuccess(ctx, a.ID, nil, "w1"); err != nil {
		t.Fatalf("ack: %v", err)
	}
	ok, err = b.ExtendLease(ctx, a.ID, time.Minute)
	if err != nil {
		t.Fatalf("extend after complete: %v", err)
	}
	if ok {
		t.Fatal("extended the lease of a completed activity")
	}
}
