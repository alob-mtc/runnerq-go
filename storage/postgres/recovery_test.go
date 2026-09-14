package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/alob-mtc/runnerq-go/storage"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestDatabaseErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		err   error
		retry bool
	}{
		{io.EOF, true}, {context.DeadlineExceeded, true}, {&pgconn.PgError{Code: "40001"}, true}, {&pgconn.PgError{Code: "40P01"}, true}, {&pgconn.PgError{Code: "08006"}, true}, {&pgconn.PgError{Code: "57P01"}, true},
		{&pgconn.PgError{Code: "22P02"}, false}, {&pgconn.PgError{Code: "23505"}, false}, {&pgconn.PgError{Code: "42601"}, false},
	} {
		t.Run(fmt.Sprint(tc.err), func(t *testing.T) {
			err := databaseError(fmt.Errorf("wrapped: %w", tc.err), "operation")
			se, ok := storage.IsStorageError(err)
			if !ok || se.IsRetryable() != tc.retry || !errors.Is(err, tc.err) {
				t.Fatalf("classified %v as %v", tc.err, err)
			}
		})
	}
}
func TestRejectPoolWithoutQueryCapacity(t *testing.T) {
	for _, size := range []int32{0, 1} {
		if _, err := WithConfig(context.Background(), "", "test", 30000, size); err == nil {
			t.Fatalf("accepted pool size %d", size)
		}
	}
}
func claimTestActivity(t *testing.T, b *PostgresBackend, a storage.QueuedActivity, worker string) {
	t.Helper()
	if err := b.Enqueue(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	got, err := b.Dequeue(context.Background(), worker, 0, []string{a.ActivityType})
	if err != nil || got == nil || got.ID != a.ID {
		t.Fatalf("claim: %v %v", got, err)
	}
}
func TestCompletionReconcilesDuplicateAndRejectsConflict(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	a := testActivity(3)
	claimTestActivity(t, b, a, "attempt1")
	for range 2 {
		if err := b.AckSuccess(ctx, a.ID, json.RawMessage(`{"a":1,"b":2}`), "attempt1"); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.AckSuccess(ctx, a.ID, json.RawMessage(`{ "b":2, "a":1 }`), "attempt1"); err != nil {
		t.Fatalf("semantic JSON duplicate: %v", err)
	}
	for _, tc := range []struct{ w, r string }{{"attempt1", `false`}, {"stale", `{"a":1,"b":2}`}} {
		if err := b.AckSuccess(ctx, a.ID, json.RawMessage(tc.r), tc.w); err == nil {
			t.Fatal("conflicting completion accepted")
		}
	}
	var n int
	if err := b.pool.QueryRow(ctx, `SELECT count(*) FROM runnerq_events WHERE activity_id=$1 AND event_type='Completed'`, a.ID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("completion events=%d err=%v", n, err)
	}
}
func TestCheckpointImmutableAndFencedAfterReclaim(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	a := testActivity(3)
	claimTestActivity(t, b, a, "old")
	id := uuid.New()
	r := storage.ActivityResult{Data: json.RawMessage(`"original"`), State: storage.ResultOk}
	for range 2 {
		if err := b.StoreCheckpoint(ctx, id, a.ID, "old", r, "run:charge"); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.StoreCheckpoint(ctx, id, a.ID, "old", storage.ActivityResult{Data: json.RawMessage(`"changed"`)}, "run:charge"); err == nil {
		t.Fatal("checkpoint overwritten")
	}
	expireLease(t, b, a.ID)
	if n, err := b.RequeueExpired(ctx, 10); err != nil || n != 1 {
		t.Fatalf("reaper %d %v", n, err)
	}
	if _, err := b.Dequeue(ctx, "new", 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := b.StoreCheckpoint(ctx, uuid.New(), a.ID, "old", r, "run:late"); err == nil {
		t.Fatal("stale checkpoint accepted")
	}
	if err := b.AckSuccess(ctx, a.ID, r.Data, "old"); err == nil {
		t.Fatal("stale completion accepted")
	}
	if ok, err := b.ExtendLeaseForWorker(ctx, a.ID, "old", time.Hour); err != nil || ok {
		t.Fatalf("stale renewal %v %v", ok, err)
	}
	stored, err := b.GetResult(ctx, id)
	if err != nil || string(stored.Data) != `"original"` {
		t.Fatalf("checkpoint changed: %v %v", stored, err)
	}
}
func TestAttemptRenewalNeverShortensLease(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	a := testActivity(3)
	claimTestActivity(t, b, a, "owner")
	var before, after int64
	if err := b.pool.QueryRow(ctx, `SELECT lease_deadline_ms FROM runnerq_activities WHERE id=$1`, a.ID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if ok, err := b.ExtendLeaseForWorker(ctx, a.ID, "owner", time.Millisecond); err != nil || !ok {
		t.Fatalf("renew %v %v", ok, err)
	}
	if err := b.pool.QueryRow(ctx, `SELECT lease_deadline_ms FROM runnerq_activities WHERE id=$1`, a.ID).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after < before {
		t.Fatalf("shortened lease %d -> %d", before, after)
	}
}
func TestFailureAcknowledgementRetryDoesNotConsumeAnotherAttempt(t *testing.T) {
	for _, max := range []uint32{0, 1, 3} {
		t.Run(fmt.Sprint(max), func(t *testing.T) {
			b := testBackend(t)
			ctx := context.Background()
			a := testActivity(max)
			claimTestActivity(t, b, a, "claim")
			for range 2 {
				dead, err := b.AckFailure(ctx, a.ID, storage.NewRetryableFailure("failed"), "claim")
				if err != nil || dead != (max == 1) {
					t.Fatalf("failure ack dead=%v err=%v", dead, err)
				}
			}
			status, count := activityStatus(t, b, a.ID)
			if max != 1 && count != 1 {
				t.Fatalf("status=%s retry_count=%d", status, count)
			}
		})
	}
}

func TestSharedResultWakesAllConsumers(t *testing.T) {
	for _, outcome := range []string{"success", "failure", "reaper"} {
		t.Run(outcome, func(t *testing.T) {
			b := testBackend(t)
			ctx := context.Background()
			producer := testActivity(1)
			producer.ActivityType = "producer"
			claimTestActivity(t, b, producer, "producer")
			waiters := []storage.QueuedActivity{testActivity(3), testActivity(3)}
			for i, a := range waiters {
				a.ActivityType = fmt.Sprintf("waiter%d", i)
				waiters[i] = a
				w := a.ActivityType
				claimTestActivity(t, b, a, w)
				if err := b.RegisterDependency(ctx, a.ID, producer.ID, w); err != nil {
					t.Fatal(err)
				}
				if err := b.YieldForResult(ctx, a.ID, producer.ID, &producer.ID, time.Now().Add(time.Hour), w, "await", "child"); err != nil {
					t.Fatal(err)
				}
			}
			var err error
			switch outcome {
			case "success":
				err = b.AckSuccess(ctx, producer.ID, nil, "producer")
			case "failure":
				_, err = b.AckFailure(ctx, producer.ID, storage.NewNonRetryableFailure("failed"), "producer")
			case "reaper":
				expireLease(t, b, producer.ID)
				_, err = b.RequeueExpired(ctx, 10)
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, a := range waiters {
				if status, _ := activityStatus(t, b, a.ID); status != "pending" {
					t.Fatalf("consumer status=%s", status)
				}
			}
		})
	}
}
func TestParkPublicationRaceAndLostReply(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	for i := range 12 {
		producer := testActivity(3)
		producer.ActivityType = fmt.Sprintf("producer%d", i)
		claimTestActivity(t, b, producer, "p")
		a := testActivity(3)
		a.ActivityType = fmt.Sprintf("waiter%d", i)
		claimTestActivity(t, b, a, "w")
		// No prior dependency: race first registration+park against publication.
		start := make(chan struct{})
		errs := make(chan error, 2)
		var wg sync.WaitGroup
		wg.Go(func() {
			<-start
			errs <- b.YieldForResult(ctx, a.ID, producer.ID, &producer.ID, time.Now().Add(time.Hour), "w", "await", "child")
		})
		wg.Go(func() { <-start; errs <- b.AckSuccess(ctx, producer.ID, nil, "p") })
		close(start)
		wg.Wait()
		for range 2 {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}
		if status, _ := activityStatus(t, b, a.ID); status != "pending" {
			t.Fatalf("lost wake: %s", status)
		}
		// Retry the committed park after its early wake: must not repark it.
		if err := b.YieldForResult(ctx, a.ID, producer.ID, &producer.ID, time.Now().Add(time.Hour), "w", "await", "child"); err != nil {
			t.Fatal(err)
		}
	}
}
func TestRetentionPinsSharedProducerUntilConsumerTreeFinishes(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	producer := plantTree(t, b, "completed", "completed", 2*time.Hour)
	consumer := plantTree(t, b, "processing", "completed", 2*time.Hour)
	// The consumer activity itself is terminal; its live root can replay.
	if _, err := b.pool.Exec(ctx, `INSERT INTO runnerq_dependencies(queue_name,waiter_activity_id,result_id,producer_activity_id) VALUES($1,$2,$3,$3)`, b.queueName, consumer.child, producer.child); err != nil {
		t.Fatal(err)
	}
	policy := storage.RetentionPolicy{Completed: time.Hour}
	if n, err := b.CleanupExpired(ctx, policy, 100); err != nil || n != 0 {
		t.Fatalf("deleted pinned tree: %d %v", n, err)
	}
	assertTreePresent(t, b, producer, "shared producer")
	if _, err := b.pool.Exec(ctx, `UPDATE runnerq_activities SET status='completed',completed_at=NOW()-INTERVAL '2 hours' WHERE id=$1`, consumer.root); err != nil {
		t.Fatal(err)
	}
	if n, err := b.CleanupExpired(ctx, policy, 100); err != nil || n != 2 {
		t.Fatalf("cleanup terminal trees: %d %v", n, err)
	}
	var refs int
	if err := b.pool.QueryRow(ctx, `SELECT count(*) FROM runnerq_dependencies WHERE queue_name=$1`, b.queueName).Scan(&refs); err != nil || refs != 0 {
		t.Fatalf("orphan refs=%d err=%v", refs, err)
	}
}
func TestReusedChildReferenceIsCommittedBeforeReturn(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	parent := testActivity(3)
	claimTestActivity(t, b, parent, "parent")
	child := testActivity(3)
	child.IdempotencyKey = &storage.IdempotencyKeyConfig{Key: "shared", Behavior: storage.BehaviorReturnExisting}
	if _, err := b.EnqueueIdempotent(ctx, &child); err != nil {
		t.Fatal(err)
	}
	other := testActivity(3)
	other.ParentActivityID = &parent.ID
	other.IdempotencyKey = child.IdempotencyKey
	got, err := b.EnqueueIdempotent(ctx, &other)
	if err != nil || got == nil || got.ExistingID != child.ID {
		t.Fatalf("reuse %v %v", got, err)
	}
	var refs int
	if err := b.pool.QueryRow(ctx, `SELECT count(*) FROM runnerq_dependencies WHERE queue_name=$1 AND waiter_activity_id=$2 AND producer_activity_id=$3`, b.queueName, parent.ID, child.ID).Scan(&refs); err != nil || refs != 1 {
		t.Fatalf("reference was not committed: %d %v", refs, err)
	}
	if err := b.RegisterDependency(ctx, parent.ID, uuid.New(), "parent"); err == nil {
		t.Fatal("nonexistent future accepted")
	}
}

func TestBusinessKeyEncodingPreservesLegacyWithoutAliasing(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	old := testActivity(3)
	old.ActivityType = "c"
	old.IdempotencyKey = &storage.IdempotencyKeyConfig{Key: "a-b-c", Behavior: storage.BehaviorReturnExisting}
	if _, err := b.EnqueueIdempotent(ctx, &old); err != nil {
		t.Fatal(err)
	}
	replay := testActivity(3)
	replay.ActivityType = "c"
	replay.IdempotencyKey = &storage.IdempotencyKeyConfig{Key: storage.BusinessIdempotencyKey("a-b", "c"), Behavior: storage.BehaviorReturnExisting}
	existing, err := b.EnqueueIdempotent(ctx, &replay)
	if err != nil || existing == nil || existing.ExistingID != old.ID {
		t.Fatalf("legacy replay %v %v", existing, err)
	}
	distinct := testActivity(3)
	distinct.ActivityType = "b-c"
	distinct.IdempotencyKey = &storage.IdempotencyKeyConfig{Key: storage.BusinessIdempotencyKey("a", "b-c"), Behavior: storage.BehaviorReturnExisting}
	existing, err = b.EnqueueIdempotent(ctx, &distinct)
	if err != nil || existing != nil {
		t.Fatalf("aliased distinct pair %v %v", existing, err)
	}
	for _, a := range []storage.QueuedActivity{replay, distinct} {
		id, err := b.LookupIdempotencyActivityID(ctx, a.IdempotencyKey.Key)
		want := distinct.ID
		if a.ActivityType == "c" {
			want = old.ID
		}
		if err != nil || id != want {
			t.Fatalf("signal lookup id=%s want=%s err=%v", id, want, err)
		}
	}
}

func TestRetentionSkipsReferenceRegistrationLock(t *testing.T) {
	b := testBackend(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	producer := plantTree(t, b, "completed", "completed", 2*time.Hour)
	consumer := testActivity(3)
	claimTestActivity(t, b, consumer, "consumer")
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if locked, err := b.lockProducerRootTx(ctx, tx, producer.child); err != nil || !locked {
		t.Fatalf("lock producer root: locked=%v err=%v", locked, err)
	}
	if err := b.addDependencyTx(ctx, tx, consumer.ID, producer.child, &producer.child); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		n, err := b.CleanupExpired(ctx, storage.RetentionPolicy{Completed: time.Hour}, 100)
		if err == nil && n != 0 {
			err = fmt.Errorf("deleted %d pinned trees", n)
		}
		done <- err
	}()
	// SKIP LOCKED may let cleanup return immediately; either way it must not
	// delete the producer while the registration transaction owns its root row.
	finished := false
	select {
	case err := <-done:
		finished = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(40 * time.Millisecond):
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if !finished {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if n, err := b.CleanupExpired(ctx, storage.RetentionPolicy{Completed: time.Hour}, 100); err != nil || n != 0 {
		t.Fatalf("cleanup crossed committed reference: n=%d err=%v", n, err)
	}
	assertTreePresent(t, b, producer, "producer pinned by concurrent registration")
}

func TestAttemptBudgetBoundariesMatchFailureAndReaper(t *testing.T) {
	for _, reap := range []bool{false, true} {
		for _, max := range []uint32{0, 1, 3} {
			t.Run(fmt.Sprintf("reaper=%v/max=%d", reap, max), func(t *testing.T) {
				b := testBackend(t)
				ctx := context.Background()
				a := testActivity(max)
				if err := b.Enqueue(ctx, a); err != nil {
					t.Fatal(err)
				}
				attempts := 3
				if max == 1 {
					attempts = 1
				}
				for i := range attempts {
					worker := fmt.Sprintf("attempt-%d", i)
					claimed, err := b.Dequeue(ctx, worker, 0, nil)
					if err != nil || claimed == nil {
						t.Fatalf("claim %d: %v %v", i, claimed, err)
					}
					if reap {
						expireLease(t, b, a.ID)
						if n, err := b.RequeueExpired(ctx, 10); err != nil || n != 1 {
							t.Fatalf("reap %d %v", n, err)
						}
					} else {
						if _, err := b.AckFailure(ctx, a.ID, storage.NewRetryableFailure("failure"), worker); err != nil {
							t.Fatal(err)
						}
					}
					status, _ := activityStatus(t, b, a.ID)
					dead := max > 0 && i+1 >= int(max)
					if (status == "dead_letter") != dead {
						t.Fatalf("max=%d attempt=%d status=%s", max, i+1, status)
					}
				}
			})
		}
	}
}
