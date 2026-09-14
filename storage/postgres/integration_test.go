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
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
)

func testBackend(t *testing.T) *PostgresBackend {
	t.Helper()
	return testBackendNamed(t, "t_"+strings.ReplaceAll(uuid.New().String(), "-", "")[:16])
}

// testBackendNamed connects a backend to a specific queue. Two backends on
// the same queue name simulate separate processes sharing a database.
func testBackendNamed(t *testing.T, queueName string) *PostgresBackend {
	t.Helper()
	dsn := os.Getenv("RUNNERQ_TEST_DSN")
	if dsn == "" {
		t.Skip("RUNNERQ_TEST_DSN not set; skipping integration test")
	}
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

// storage.BatchQueueStorage: one round trip claims up to limit rows in the
// order Dequeue would have taken them, each with its own fenced token.
func TestDequeueBatchClaimsInDequeueOrderWithFencedTokens(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	for _, p := range []storage.ActivityPriority{storage.PriorityLow, storage.PriorityCritical, storage.PriorityNormal, storage.PriorityHigh, storage.PriorityLow} {
		a := testActivity(3)
		a.Priority = p
		if err := b.Enqueue(ctx, a); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	claims, err := b.DequeueBatch(ctx, "engine:batch:1", 3, 0, nil)
	if err != nil {
		t.Fatalf("batch dequeue: %v", err)
	}
	wantOrder := []storage.ActivityPriority{storage.PriorityCritical, storage.PriorityHigh, storage.PriorityNormal}
	if len(claims) != len(wantOrder) {
		t.Fatalf("claimed %d activities, want %d", len(claims), len(wantOrder))
	}
	for i, c := range claims {
		if c.Activity.Priority != wantOrder[i] {
			t.Fatalf("claim %d priority = %d, want %d (dequeue order)", i, c.Activity.Priority, wantOrder[i])
		}
		if want := "engine:batch:1:" + c.Activity.ID.String(); c.LeaseID != want {
			t.Fatalf("lease token = %q, want %q", c.LeaseID, want)
		}
		if c.Attempt != 1 || !c.LeaseDeadline.After(time.Now()) {
			t.Fatalf("claim %d attempt=%d deadline=%v, want attempt 1 and a future deadline", i, c.Attempt, c.LeaseDeadline)
		}
		if status, _ := activityStatus(t, b, c.Activity.ID); status != "processing" {
			t.Fatalf("status = %q, want processing", status)
		}
		if !hasEvent(t, b, c.Activity.ID, storage.EventDequeued) {
			t.Fatalf("activity %s has no dequeue event", c.Activity.ID)
		}
	}

	// Rows claimed together are still fenced apart: a sibling's token is
	// rejected, the row's own token is accepted.
	if err := b.AckSuccess(ctx, claims[0].Activity.ID, nil, claims[1].LeaseID); err == nil {
		t.Fatal("ack with a sibling claim's token succeeded; tokens must be per activity")
	}
	if err := b.AckSuccess(ctx, claims[0].Activity.ID, nil, claims[0].LeaseID); err != nil {
		t.Fatalf("ack with own token: %v", err)
	}

	rest, err := b.DequeueBatch(ctx, "engine:batch:2", 10, 0, nil)
	if err != nil || len(rest) != 2 {
		t.Fatalf("remaining claim: n=%d err=%v, want the 2 low-priority rows", len(rest), err)
	}
	for _, c := range rest {
		if c.Activity.Priority != storage.PriorityLow {
			t.Fatalf("remaining row priority = %d, want low", c.Activity.Priority)
		}
	}
	if none, err := b.DequeueBatch(ctx, "engine:batch:3", 10, 0, nil); err != nil || len(none) != 0 {
		t.Fatalf("empty queue claim: n=%d err=%v, want none", len(none), err)
	}
	if none, err := b.DequeueBatch(ctx, "engine:batch:4", 0, time.Second, nil); err != nil || len(none) != 0 {
		t.Fatalf("limit 0 claim: n=%d err=%v, want an immediate empty result", len(none), err)
	}
}

// The three static type-filter forms (none / one / many) and the limit.
func TestDequeueBatchTypeFilterForms(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	for _, typ := range []string{"a", "a", "b", "b", "c", "c"} {
		a := testActivity(3)
		a.ActivityType = typ
		if err := b.Enqueue(ctx, a); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}
	claimTypes := func(prefix string, limit int, filter []string) []string {
		t.Helper()
		claims, err := b.DequeueBatch(ctx, prefix, limit, 0, filter)
		if err != nil {
			t.Fatalf("batch dequeue %v: %v", filter, err)
		}
		types := make([]string, 0, len(claims))
		for _, c := range claims {
			types = append(types, c.Activity.ActivityType)
		}
		return types
	}

	if got := claimTypes("one", 10, []string{"a"}); len(got) != 2 || got[0] != "a" || got[1] != "a" {
		t.Fatalf("one-type filter claimed %v, want [a a]", got)
	}
	got := claimTypes("many", 3, []string{"b", "c"})
	if len(got) != 3 {
		t.Fatalf("many-type filter with limit 3 claimed %v", got)
	}
	for _, typ := range got {
		if typ == "a" {
			t.Fatalf("many-type filter [b c] claimed type a: %v", got)
		}
	}
	if got := claimTypes("all", 10, nil); len(got) != 1 || got[0] == "a" {
		t.Fatalf("unfiltered claim got %v, want the single remaining b/c row", got)
	}
}

// Two processes claiming in bulk from one queue never receive the same row.
func TestDequeueBatchConcurrentClaimersNeverOverlap(t *testing.T) {
	queueName := "t_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
	b1, b2 := testBackendNamed(t, queueName), testBackendNamed(t, queueName)
	ctx := context.Background()

	const total = 40
	for range total {
		if err := b1.Enqueue(ctx, testActivity(3)); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	var mu sync.Mutex
	seen := make(map[uuid.UUID]string, total)
	var wg sync.WaitGroup
	for i, b := range []*PostgresBackend{b1, b2} {
		wg.Go(func() {
			for round := 0; ; round++ {
				claims, err := b.DequeueBatch(ctx, fmt.Sprintf("claimer-%d-%d", i, round), 7, 0, nil)
				if err != nil {
					t.Errorf("claimer %d: %v", i, err)
					return
				}
				if len(claims) == 0 {
					return
				}
				mu.Lock()
				for _, c := range claims {
					if prev, dup := seen[c.Activity.ID]; dup {
						t.Errorf("activity %s claimed twice: %s and %s", c.Activity.ID, prev, c.LeaseID)
					}
					seen[c.Activity.ID] = c.LeaseID
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if len(seen) != total {
		t.Fatalf("claimed %d distinct activities, want %d", len(seen), total)
	}
}

// A blocking batch claim parks on the work signal and wakes when a row lands.
func TestDequeueBatchBlocksUntilWorkIsSignalled(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	type outcome struct {
		claims []storage.DequeuedActivity
		err    error
		took   time.Duration
	}
	done := make(chan outcome, 1)
	start := time.Now()
	go func() {
		claims, err := b.DequeueBatch(ctx, "blocker", 5, 10*time.Second, nil)
		done <- outcome{claims: claims, err: err, took: time.Since(start)}
	}()

	time.Sleep(300 * time.Millisecond)
	a := testActivity(3)
	if err := b.Enqueue(ctx, a); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	select {
	case o := <-done:
		if o.err != nil || len(o.claims) != 1 || o.claims[0].Activity.ID != a.ID {
			t.Fatalf("blocking claim returned n=%d err=%v", len(o.claims), o.err)
		}
		if o.took > 5*time.Second {
			t.Fatalf("blocking claim took %v; it should wake on the enqueue signal, not the deadline", o.took)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("blocking claim did not return after work was enqueued")
	}
}

// A batch token is fenced exactly like a single-claim token: once the lease
// expires and the row is reclaimed, the old token can no longer ack.
func TestStaleBatchTokenCannotAck(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	a := testActivity(5)
	if err := b.Enqueue(ctx, a); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	stale, err := b.DequeueBatch(ctx, "stale", 1, 0, nil)
	if err != nil || len(stale) != 1 {
		t.Fatalf("first claim: n=%d err=%v", len(stale), err)
	}

	expireLease(t, b, a.ID)
	if n, err := b.RequeueExpired(ctx, 10); err != nil || n != 1 {
		t.Fatalf("requeue expired: n=%d err=%v", n, err)
	}
	fresh, err := b.DequeueBatch(ctx, "fresh", 1, 0, nil)
	if err != nil || len(fresh) != 1 || fresh[0].Activity.ID != a.ID {
		t.Fatalf("second claim: n=%d err=%v", len(fresh), err)
	}
	if fresh[0].LeaseID == stale[0].LeaseID || fresh[0].Attempt != 2 {
		t.Fatalf("reclaim token=%q attempt=%d; want a new token and attempt 2", fresh[0].LeaseID, fresh[0].Attempt)
	}

	if err := b.AckSuccess(ctx, a.ID, json.RawMessage(`"stale"`), stale[0].LeaseID); err == nil {
		t.Fatal("stale batch token ack succeeded; it must be fenced out")
	}
	if status, _ := activityStatus(t, b, a.ID); status != "processing" {
		t.Fatalf("status after stale ack = %q, want processing", status)
	}
	if err := b.AckSuccess(ctx, a.ID, json.RawMessage(`"fresh"`), fresh[0].LeaseID); err != nil {
		t.Fatalf("owning token ack failed: %v", err)
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
