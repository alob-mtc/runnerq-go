package postgres

// Integration tests for the wake-up signalling layer (signals.go): blocking
// dequeue, cross-process WaitForResult, and the event-stream tailer. Skipped
// unless RUNNERQ_TEST_DSN is set — see integration_test.go.
//
// "Cross-process" is simulated with two independent backend instances on the
// same queue: separate pools, separate LISTEN connections, sharing only the
// database — exactly the topology of a producing worker process and a
// separate server process awaiting a future.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
)

func sharedQueueName() string {
	return "t_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
}

// A worker parked in a blocking Dequeue on an empty queue must wake when a
// DIFFERENT process enqueues work — far faster than the fallback probe.
func TestBlockingDequeueWakesAcrossProcesses(t *testing.T) {
	queueName := sharedQueueName()
	consumer := testBackendNamed(t, queueName)
	producer := testBackendNamed(t, queueName)
	ctx := context.Background()

	a := testActivity(3)
	go func() {
		time.Sleep(300 * time.Millisecond)
		if err := producer.Enqueue(ctx, a); err != nil {
			t.Errorf("enqueue: %v", err)
		}
	}()

	start := time.Now()
	claimed, err := consumer.Dequeue(ctx, "w1", 10*time.Second, nil)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if claimed == nil {
		t.Fatal("blocking dequeue returned nothing despite enqueued work")
	}
	if claimed.ID != a.ID {
		t.Fatalf("claimed %s, want %s", claimed.ID, a.ID)
	}
	// Notification path: enqueue at ~300ms + signal flush (≤50ms) + delivery.
	// The fallback probe alone would take ~2s+, so anything well under that
	// proves the notify wake-up worked.
	if elapsed > 1500*time.Millisecond {
		t.Fatalf("dequeue took %v; expected a notification-driven wake well under the fallback probe interval", elapsed)
	}
}

// Blocking Dequeue must respect its block window on a queue that stays empty.
func TestBlockingDequeueTimesOut(t *testing.T) {
	b := testBackend(t)
	start := time.Now()
	claimed, err := b.Dequeue(context.Background(), "w1", 500*time.Millisecond, nil)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if claimed != nil {
		t.Fatalf("claimed %v from an empty queue", claimed.ID)
	}
	if elapsed := time.Since(start); elapsed < 400*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("empty blocking dequeue returned after %v, want ~500ms", elapsed)
	}
}

// A future awaited in one process must resolve when a different process
// completes the activity. This is the scenario the suspend-stress "poll
// storm" came from: previously each waiter hammered the results table at
// 10 queries/sec; now it parks on a notification with a slow fallback.
func TestWaitForResultAcrossProcesses(t *testing.T) {
	queueName := sharedQueueName()
	worker := testBackendNamed(t, queueName)
	awaiter := testBackendNamed(t, queueName)
	ctx := context.Background()

	a := testActivity(3)
	if err := worker.Enqueue(ctx, a); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	go func() {
		time.Sleep(400 * time.Millisecond)
		claimed, err := worker.Dequeue(ctx, "w1", time.Second, nil)
		if err != nil || claimed == nil {
			t.Errorf("worker dequeue: claimed=%v err=%v", claimed, err)
			return
		}
		if err := worker.AckSuccess(ctx, a.ID, []byte(`{"answer":42}`), "w1"); err != nil {
			t.Errorf("worker ack: %v", err)
		}
	}()

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	start := time.Now()
	res, err := awaiter.WaitForResult(waitCtx, a.ID)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("wait for result: %v", err)
	}
	if res.State != storage.ResultOk {
		t.Fatalf("result state = %v, want Ok", res.State)
	}
	var decoded map[string]int
	if err := json.Unmarshal(res.Data, &decoded); err != nil || decoded["answer"] != 42 {
		t.Fatalf("result data = %s (decode err %v), want answer=42", res.Data, err)
	}
	// Fallback re-check alone would take ~5s; the notify path should land
	// shortly after the 400ms ack.
	if elapsed > 3*time.Second {
		t.Fatalf("WaitForResult took %v; expected a notification-driven wake well under the %v fallback", elapsed, resultWaitFallback)
	}
}

// WaitForResult must return immediately when the result already exists —
// the common case for a parent retried after its children completed.
func TestWaitForResultAlreadyStored(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	a := testActivity(3)
	if err := b.Enqueue(ctx, a); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if claimed, err := b.Dequeue(ctx, "w1", time.Second, nil); err != nil || claimed == nil {
		t.Fatalf("dequeue: claimed=%v err=%v", claimed, err)
	}
	if err := b.AckSuccess(ctx, a.ID, []byte(`"v"`), "w1"); err != nil {
		t.Fatalf("ack: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	res, err := b.WaitForResult(waitCtx, a.ID)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if res == nil || res.State != storage.ResultOk {
		t.Fatalf("result = %+v, want Ok", res)
	}
}

// The event stream must deliver lifecycle events committed after the
// subscription, fed by the table tailer (writers no longer notify in-tx).
func TestEventStreamTailsCommittedEvents(t *testing.T) {
	queueName := sharedQueueName()
	consumer := testBackendNamed(t, queueName)
	producer := testBackendNamed(t, queueName)

	streamCtx := t.Context()
	events, err := consumer.EventStream(streamCtx)
	if err != nil {
		t.Fatalf("event stream: %v", err)
	}
	// Give the tailer a beat to anchor its cursor before producing.
	time.Sleep(300 * time.Millisecond)

	a := testActivity(3)
	if err := producer.Enqueue(context.Background(), a); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("event stream closed unexpectedly")
			}
			if ev.ActivityID == a.ID && ev.EventType == storage.EventEnqueued {
				return // success
			}
		case <-deadline:
			t.Fatal("Enqueued event never arrived on the stream")
		}
	}
}

// The three dequeue query forms must each claim the right rows.
func TestDequeueTypeFilters(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	mk := func(actType string) storage.QueuedActivity {
		a := testActivity(3)
		a.ActivityType = actType
		return a
	}
	aa, ab, ac := mk("type_a"), mk("type_b"), mk("type_c")
	for _, a := range []storage.QueuedActivity{aa, ab, ac} {
		if err := b.Enqueue(ctx, a); err != nil {
			t.Fatalf("enqueue %s: %v", a.ActivityType, err)
		}
	}

	// Single-type form.
	got, err := b.Dequeue(ctx, "w", 0, []string{"type_b"})
	if err != nil || got == nil || got.ID != ab.ID {
		t.Fatalf("single-type dequeue = %+v err=%v, want %s", got, err, ab.ID)
	}
	// Multi-type form: oldest of the listed types first (same priority).
	got, err = b.Dequeue(ctx, "w", 0, []string{"type_a", "type_c"})
	if err != nil || got == nil || got.ID != aa.ID {
		t.Fatalf("multi-type dequeue = %+v err=%v, want %s", got, err, aa.ID)
	}
	// Untyped form claims what's left.
	got, err = b.Dequeue(ctx, "w", 0, nil)
	if err != nil || got == nil || got.ID != ac.ID {
		t.Fatalf("untyped dequeue = %+v err=%v, want %s", got, err, ac.ID)
	}
	// Filtered dequeue must not claim other types.
	got, err = b.Dequeue(ctx, "w", 0, []string{"type_a"})
	if err != nil || got != nil {
		t.Fatalf("dequeue of exhausted type = %+v err=%v, want nil", got, err)
	}
}
