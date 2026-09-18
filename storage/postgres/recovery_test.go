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
	"github.com/jackc/pgx/v5"
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

// pinnedTx opens a transaction the test drives by hand to stand in for one
// side of the park/publish ordering. All of its statements are issued before
// the other side is started, and it then only commits, so it never requests a
// new lock while another session (a concurrent test package running schema
// DDL on the shared tables) may be queued behind it.
func pinnedTx(t *testing.T, b *PostgresBackend) (tx pgx.Tx, commit func()) {
	t.Helper()
	ctx := context.Background()
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(ctx) })
	return tx, func() {
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

// retryConflict runs op the way the engine's persistence retry does: a
// storage conflict (deadlock / serialization failure) is transient and is
// simply re-attempted. Concurrent test packages run schema DDL on the shared
// tables whenever they construct a backend, and a call queued behind that
// DDL can be picked as the deadlock victim.
func retryConflict(op func() error) error {
	var err error
	for range 5 {
		if err = op(); err == nil {
			return nil
		}
		if se, ok := storage.IsStorageError(err); !ok || se.Kind != storage.ErrConflict {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return err
}

// mustStayBlocked starts op and fails the test if it returns before the
// pinned side commits; it then commits and returns op's result.
func mustStayBlocked(t *testing.T, commit func(), op func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- op() }()
	select {
	case err := <-done:
		t.Fatalf("returned while the owner row was still held by the other side: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	commit()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("still blocked after the other side committed")
		return nil
	}
}

// The park/publish ordering rests on the owner's row lock, not on timing.
// Each subtest pins one side mid-transaction by hand, proves the other side
// waits on the row, and checks that once the pinned side commits the waiting
// side observes it: the waiter always ends up runnable.
func TestParkAndPublishOrderOnOwnerRowLock(t *testing.T) {
	ctx := context.Background()
	newPair := func(t *testing.T) (*PostgresBackend, storage.QueuedActivity, storage.QueuedActivity) {
		b := testBackend(t)
		producer := testActivity(1)
		producer.ActivityType = "producer"
		claimTestActivity(t, b, producer, "p")
		waiter := testActivity(3)
		waiter.ActivityType = "waiter"
		claimTestActivity(t, b, waiter, "w")
		return b, producer, waiter
	}

	t.Run("consumer commits first, publisher waits", func(t *testing.T) {
		b, producer, waiter := newPair(t)
		// Hand-driven park: owner FOR SHARE, dependency, status 'waiting'.
		tx, commit := pinnedTx(t, b)
		var one int
		if err := tx.QueryRow(ctx, `SELECT 1 FROM runnerq_activities WHERE id = $1 AND queue_name = $2 FOR SHARE`, producer.ID, b.queueName).Scan(&one); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO runnerq_dependencies (queue_name, waiter_activity_id, result_id, producer_activity_id) VALUES ($1, $2, $3, $3)`, b.queueName, waiter.ID, producer.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE runnerq_activities SET status = 'waiting', current_worker_id = NULL, lease_deadline_ms = NULL WHERE id = $1 AND queue_name = $2`, waiter.ID, b.queueName); err != nil {
			t.Fatal(err)
		}
		err := mustStayBlocked(t, commit, func() error {
			return retryConflict(func() error { return b.AckSuccess(ctx, producer.ID, nil, "p") })
		})
		if err != nil {
			t.Fatal(err)
		}
		if status, _ := activityStatus(t, b, waiter.ID); status != "pending" {
			t.Fatalf("waiter status=%s, want pending (publisher must see the committed dependency)", status)
		}
	})

	t.Run("publisher commits first, consumer waits", func(t *testing.T) {
		b, producer, waiter := newPair(t)
		// Hand-driven publication: owner exclusively locked, result stored.
		tx, commit := pinnedTx(t, b)
		var one int
		if err := tx.QueryRow(ctx, `SELECT 1 FROM runnerq_activities WHERE id = $1 AND queue_name = $2 FOR NO KEY UPDATE`, producer.ID, b.queueName).Scan(&one); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO runnerq_results (activity_id, queue_name, state, created_at, owner_activity_id) VALUES ($1, $2, 'Ok', NOW(), $1)`, producer.ID, b.queueName); err != nil {
			t.Fatal(err)
		}
		err := mustStayBlocked(t, commit, func() error {
			return retryConflict(func() error {
				return b.YieldForResult(ctx, waiter.ID, producer.ID, &producer.ID, time.Now().Add(time.Hour), "w", "await", "child")
			})
		})
		if err != nil {
			t.Fatal(err)
		}
		if status, _ := activityStatus(t, b, waiter.ID); status != "pending" {
			t.Fatalf("waiter status=%s, want pending (park must see the committed result)", status)
		}
	})
}

// A signal's owner is its target, so SignalActivity and a target parking for
// that signal order on the target's own row.
func TestSignalAndParkOrderOnTargetRow(t *testing.T) {
	ctx := context.Background()

	t.Run("park commits first, signal waits", func(t *testing.T) {
		b := testBackend(t)
		sigID := uuid.New()
		target := testActivity(3)
		claimTestActivity(t, b, target, "w")
		tx, commit := pinnedTx(t, b)
		var one int
		if err := tx.QueryRow(ctx, `SELECT 1 FROM runnerq_activities WHERE id = $1 AND queue_name = $2 FOR UPDATE`, target.ID, b.queueName).Scan(&one); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO runnerq_dependencies (queue_name, waiter_activity_id, result_id) VALUES ($1, $2, $3)`, b.queueName, target.ID, sigID); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE runnerq_activities SET status = 'waiting', current_worker_id = NULL, lease_deadline_ms = NULL WHERE id = $1 AND queue_name = $2`, target.ID, b.queueName); err != nil {
			t.Fatal(err)
		}
		err := mustStayBlocked(t, commit, func() error {
			return retryConflict(func() error { return b.SignalActivity(ctx, target.ID, sigID, "approve", json.RawMessage(`true`)) })
		})
		if err != nil {
			t.Fatal(err)
		}
		if status, _ := activityStatus(t, b, target.ID); status != "pending" {
			t.Fatalf("target status=%s, want pending (signal must see the committed park)", status)
		}
	})

	t.Run("signal commits first, park waits", func(t *testing.T) {
		b := testBackend(t)
		sigID := uuid.New()
		target := testActivity(3)
		claimTestActivity(t, b, target, "w")
		tx, commit := pinnedTx(t, b)
		var one int
		if err := tx.QueryRow(ctx, `SELECT 1 FROM runnerq_activities WHERE id = $1 AND queue_name = $2 FOR NO KEY UPDATE`, target.ID, b.queueName).Scan(&one); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO runnerq_results (activity_id, queue_name, state, created_at, owner_activity_id, step) VALUES ($1, $2, 'Ok', NOW(), $3, 'signal:approve')`, sigID, b.queueName, target.ID); err != nil {
			t.Fatal(err)
		}
		err := mustStayBlocked(t, commit, func() error {
			return retryConflict(func() error {
				return b.YieldForResult(ctx, target.ID, sigID, nil, time.Now().Add(time.Hour), "w", "signal", "approve")
			})
		})
		if err != nil {
			t.Fatal(err)
		}
		if status, _ := activityStatus(t, b, target.ID); status != "pending" {
			t.Fatalf("target status=%s, want pending (park must see the committed signal)", status)
		}
	})
}

// Key reuse (ReturnExisting) registers a dependency on an existing child that
// may be a retention candidate at that very moment. The child's idempotency
// row is the ordering point: cleanup locks it before its dependency check,
// reuse holds it while linking. Either order leaves no future pointing at a
// deleted activity.
func TestCleanupAndKeyReuseOrderOnIdempotencyRow(t *testing.T) {
	ctx := context.Background()
	policy := storage.RetentionPolicy{Completed: time.Hour}

	// An expired, completed child owning a ReturnExisting key, plus a live
	// parent that will reuse it. Returns the stored (encoded) key string.
	plant := func(t *testing.T) (b *PostgresBackend, parent, child storage.QueuedActivity, storedKey string) {
		b = testBackend(t)
		parent = testActivity(3)
		parent.ActivityType = "parent"
		claimTestActivity(t, b, parent, "parent")
		child = testActivity(3)
		child.ActivityType = "child"
		child.IdempotencyKey = &storage.IdempotencyKeyConfig{Key: "shared", Behavior: storage.BehaviorReturnExisting}
		if got, err := b.EnqueueIdempotent(ctx, &child); err != nil || got != nil {
			t.Fatalf("first claim: %v %v", got, err)
		}
		if c, err := b.Dequeue(ctx, "c", 0, []string{"child"}); err != nil || c == nil {
			t.Fatalf("claim child: %v %v", c, err)
		}
		if err := b.AckSuccess(ctx, child.ID, json.RawMessage(`42`), "c"); err != nil {
			t.Fatal(err)
		}
		if _, err := b.pool.Exec(ctx, `UPDATE runnerq_activities SET completed_at = NOW() - INTERVAL '2 hours' WHERE id = $1`, child.ID); err != nil {
			t.Fatal(err)
		}
		if err := b.pool.QueryRow(ctx, `SELECT idempotency_key FROM runnerq_idempotency WHERE queue_name = $1 AND activity_id = $2`, b.queueName, child.ID).Scan(&storedKey); err != nil {
			t.Fatal(err)
		}
		return b, parent, child, storedKey
	}
	childExists := func(t *testing.T, b *PostgresBackend, id uuid.UUID) bool {
		t.Helper()
		var n int
		if err := b.pool.QueryRow(ctx, `SELECT count(*) FROM runnerq_activities WHERE id = $1`, id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n > 0
	}

	t.Run("reuse commits first, cleanup waits and keeps the tree", func(t *testing.T) {
		b, parent, child, storedKey := plant(t)
		// Hand-driven reuse mid-flight: key row held, dependency written.
		tx, commit := pinnedTx(t, b)
		var one int
		if err := tx.QueryRow(ctx, `SELECT 1 FROM runnerq_idempotency WHERE queue_name = $1 AND idempotency_key = $2 FOR UPDATE`, b.queueName, storedKey).Scan(&one); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO runnerq_dependencies (queue_name, waiter_activity_id, result_id, producer_activity_id) VALUES ($1, $2, $3, $3)`, b.queueName, parent.ID, child.ID); err != nil {
			t.Fatal(err)
		}
		var swept uint64
		err := mustStayBlocked(t, commit, func() error {
			return retryConflict(func() error {
				var err error
				swept, err = b.CleanupExpired(ctx, policy, 100)
				return err
			})
		})
		if err != nil {
			t.Fatal(err)
		}
		if swept != 0 || !childExists(t, b, child.ID) {
			t.Fatalf("cleanup swept %d trees and child present=%v; a tree the reuse just pinned must survive", swept, childExists(t, b, child.ID))
		}
		if res, err := b.GetResult(ctx, child.ID); err != nil || res == nil {
			t.Fatalf("reused child's result must still resolve: %v %v", res, err)
		}
	})

	t.Run("cleanup commits first, reuse waits and claims fresh", func(t *testing.T) {
		b, parent, child, storedKey := plant(t)
		// Hand-driven cleanup mid-flight: root and key row held, tree deleted.
		tx, commit := pinnedTx(t, b)
		var one int
		if err := tx.QueryRow(ctx, `SELECT 1 FROM runnerq_activities WHERE id = $1 AND queue_name = $2 FOR UPDATE`, child.ID, b.queueName).Scan(&one); err != nil {
			t.Fatal(err)
		}
		if err := tx.QueryRow(ctx, `SELECT 1 FROM runnerq_idempotency WHERE queue_name = $1 AND idempotency_key = $2 FOR UPDATE`, b.queueName, storedKey).Scan(&one); err != nil {
			t.Fatal(err)
		}
		for _, stmt := range []string{
			`DELETE FROM runnerq_results WHERE activity_id = $1`,
			`DELETE FROM runnerq_idempotency WHERE activity_id = $1`,
			`DELETE FROM runnerq_activities WHERE id = $1`,
		} {
			if _, err := tx.Exec(ctx, stmt, child.ID); err != nil {
				t.Fatal(err)
			}
		}
		other := testActivity(3)
		other.ActivityType = "child"
		other.ParentActivityID = &parent.ID
		other.IdempotencyKey = child.IdempotencyKey
		var got *storage.IdempotencyResult
		err := mustStayBlocked(t, commit, func() error {
			return retryConflict(func() error {
				var err error
				got, err = b.EnqueueIdempotent(ctx, &other)
				return err
			})
		})
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Fatalf("reuse returned existing %s after cleanup deleted it; it must claim the key fresh", got.ExistingID)
		}
		var owner uuid.UUID
		if err := b.pool.QueryRow(ctx, `SELECT activity_id FROM runnerq_idempotency WHERE queue_name = $1 AND idempotency_key = $2`, b.queueName, storedKey).Scan(&owner); err != nil || owner != other.ID {
			t.Fatalf("key owner = %s (%v), want the fresh activity %s", owner, err, other.ID)
		}
		if !childExists(t, b, other.ID) {
			t.Fatal("fresh activity was not enqueued")
		}
	})
}

// A spawn issued by an execution that has since lost its claim must not add a
// child: the replacement execution issues the same spawns.
func TestHandlerSpawnsAreFencedAfterReclaim(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()
	parent := testActivity(3)
	claimTestActivity(t, b, parent, "old")
	child := func(key string) storage.QueuedActivity {
		c := testActivity(3)
		c.ParentActivityID, c.RootActivityID, c.Depth = &parent.ID, parent.ID, 1
		if key != "" {
			c.IdempotencyKey = &storage.IdempotencyKeyConfig{Key: key, Behavior: storage.BehaviorReturnExisting}
		}
		return c
	}
	live, keyed := child(""), child("rq:step:fence:"+parent.ID.String())
	if err := b.EnqueueForWorker(ctx, live, parent.ID, "old"); err != nil {
		t.Fatalf("owned spawn: %v", err)
	}
	if existing, err := b.EnqueueIdempotentForWorker(ctx, &keyed, parent.ID, "old"); err != nil || existing != nil {
		t.Fatalf("owned keyed spawn: %v %v", existing, err)
	}
	expireLease(t, b, parent.ID)
	if n, err := b.RequeueExpired(ctx, 10); err != nil || n != 1 {
		t.Fatalf("reaper %d %v", n, err)
	}
	if got, err := b.Dequeue(ctx, "new", 0, nil); err != nil || got == nil || got.ID != parent.ID {
		t.Fatalf("reclaim: %v %v", got, err)
	}
	stale, staleKeyed, staleReuse := child(""), child("rq:step:fence:late:"+parent.ID.String()), child(keyed.IdempotencyKey.Key)
	lost := func(err error) bool {
		se, ok := storage.IsStorageError(err)
		return ok && se.Kind == storage.ErrClaimLost
	}
	if err := b.EnqueueForWorker(ctx, stale, parent.ID, "old"); !lost(err) {
		t.Fatalf("stale spawn: %v", err)
	}
	if _, err := b.EnqueueIdempotentForWorker(ctx, &staleKeyed, parent.ID, "old"); !lost(err) {
		t.Fatalf("stale keyed spawn: %v", err)
	}
	if _, err := b.EnqueueIdempotentForWorker(ctx, &staleReuse, parent.ID, "old"); !lost(err) {
		t.Fatalf("stale reattach: %v", err)
	}
	if existing, err := b.EnqueueIdempotentForWorker(ctx, &staleReuse, parent.ID, "new"); err != nil || existing == nil || existing.ExistingID != keyed.ID {
		t.Fatalf("replacement reattach: %v %v", existing, err)
	}
	var children, keys int
	if err := b.pool.QueryRow(ctx, `SELECT count(*) FROM runnerq_activities WHERE parent_activity_id=$1`, parent.ID).Scan(&children); err != nil {
		t.Fatal(err)
	}
	if err := b.pool.QueryRow(ctx, `SELECT count(*) FROM runnerq_idempotency WHERE queue_name=$1 AND idempotency_key=$2`, b.queueName, staleKeyed.IdempotencyKey.Key).Scan(&keys); err != nil {
		t.Fatal(err)
	}
	if children != 2 || keys != 0 {
		t.Fatalf("children=%d stale keys=%d", children, keys)
	}
}
