package runnerq

// Integration tests for signals: WaitForSignal (buffered, parked, timeout)
// and cross-process delivery. Skipped unless RUNNERQ_TEST_DSN is set.
// Cross-process is simulated with a second backend instance sharing only the
// database, as in storage/postgres/signals_test.go.

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
	"github.com/alob-mtc/runnerq-go/storage/postgres"
)

// A signal delivered before the handler reaches WaitForSignal — here, before
// the activity even runs — is buffered and returned instantly. Delivery also
// wakes the scheduled activity early.
func TestSignalBufferedBeforeWait(t *testing.T) {
	var invocations atomic.Int32
	h := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		invocations.Add(1)
		return ctx.WaitForSignal("approval", 0)
	}}
	rig := newStepsRig(t, func(e *WorkerEngine) { e.RegisterActivity("approve_me", h) })

	// Delay parks the activity as scheduled, guaranteeing the signal lands
	// before the handler runs.
	fut, err := rig.engine.GetActivityExecutor().
		Activity("approve_me").
		Delay(time.Minute).
		Payload(json.RawMessage(`{}`)).
		Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	if err := rig.engine.Signal(context.Background(), fut.activityID, "approval", json.RawMessage(`{"approved":true}`)); err != nil {
		t.Fatalf("signal: %v", err)
	}

	res := rig.await(t, fut.activityID, 20*time.Second)
	if res.State != storage.ResultOk || !strings.Contains(string(res.Data), "approved") {
		t.Fatalf("result = %v %s, want buffered signal payload", res.State, res.Data)
	}
	if got := invocations.Load(); got != 1 {
		t.Fatalf("handler invoked %d times, want 1 — buffered signal must resolve on first pass", got)
	}
}

// An unbounded WaitForSignal yields (no worker held, no retry consumed) and
// is woken by delivery from a DIFFERENT process.
func TestSignalWakesParkedWaiterAcrossProcesses(t *testing.T) {
	dsn := os.Getenv("RUNNERQ_TEST_DSN")
	if dsn == "" {
		t.Skip("RUNNERQ_TEST_DSN not set; skipping integration test")
	}
	var invocations atomic.Int32
	h := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		invocations.Add(1)
		return ctx.WaitForSignal("human-approval", 0)
	}}
	rig := newStepsRig(t, func(e *WorkerEngine) { e.RegisterActivity("gate", h) })

	fut, err := rig.engine.GetActivityExecutor().
		Activity("gate").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	// Wait until the activity has actually yielded (parked as Scheduled).
	deadline := time.After(15 * time.Second)
	for {
		snap, err := rig.backend.GetActivity(context.Background(), fut.activityID)
		if err == nil && snap != nil && snap.Status == "Scheduled" {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("activity never parked; last snapshot: %+v", snap)
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Deliver from a separate backend instance — a different "process".
	queueName := rig.engine.config.QueueName
	external, err := postgres.New(context.Background(), dsn, queueName)
	if err != nil {
		t.Fatalf("external backend: %v", err)
	}
	defer external.Close()
	if err := SignalActivity(context.Background(), external, fut.activityID, "human-approval", json.RawMessage(`{"by":"ops"}`)); err != nil {
		t.Fatalf("external signal: %v", err)
	}

	res := rig.await(t, fut.activityID, 20*time.Second)
	if res.State != storage.ResultOk || !strings.Contains(string(res.Data), "ops") {
		t.Fatalf("result = %v %s, want delivered payload", res.State, res.Data)
	}
	if got := invocations.Load(); got != 2 {
		t.Fatalf("handler invoked %d times, want 2 (park + resume)", got)
	}
	snap, err := rig.backend.GetActivity(context.Background(), fut.activityID)
	if err != nil || snap == nil {
		t.Fatalf("get activity: snap=%v err=%v", snap, err)
	}
	if snap.RetryCount != 0 {
		t.Fatalf("retry_count = %d, want 0 — a parked signal wait must not consume retries", snap.RetryCount)
	}
}

// A short timeout waits in-process — single invocation, no park — and
// surfaces a typed, non-retryable timeout error the handler can act on.
func TestSignalTimeoutInProcess(t *testing.T) {
	var invocations atomic.Int32
	h := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		invocations.Add(1)
		_, err := ctx.WaitForSignal("never-arrives", time.Second)
		if !IsSignalTimeout(err) {
			return nil, NewNonRetryError("expected a signal timeout, got: " + errString(err))
		}
		return json.RawMessage(`{"timed_out":true}`), nil
	}}
	rig := newStepsRig(t, func(e *WorkerEngine) { e.RegisterActivity("waiter", h) })

	fut, err := rig.engine.GetActivityExecutor().
		Activity("waiter").Payload(json.RawMessage(`{}`)).Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	res := rig.await(t, fut.activityID, 20*time.Second)
	if res.State != storage.ResultOk || !strings.Contains(string(res.Data), "timed_out") {
		t.Fatalf("result = %v %s, want timeout handled", res.State, res.Data)
	}
	// The contract under test is that a 1s wait inside the default 300s
	// budget stays in-process: exactly one invocation, no Yielded event.
	if got := invocations.Load(); got != 1 {
		t.Fatalf("handler invoked %d times, want 1 — a short wait must not park", got)
	}
	events, err := rig.backend.GetActivityEvents(context.Background(), fut.activityID, 100)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	for _, ev := range events {
		if ev.EventType == storage.EventYielded {
			t.Fatal("short in-budget signal wait recorded a Yielded event")
		}
	}
}

// A timeout longer than the handler budget parks; the wait deadline persists
// across the park, so the replay times out instead of restarting the clock.
func TestSignalTimeoutSurvivesPark(t *testing.T) {
	var invocations atomic.Int32
	h := &funcHandler{fn: func(ctx ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		invocations.Add(1)
		_, err := ctx.WaitForSignal("slow-approval", 4*time.Second)
		if err != nil {
			if IsSignalTimeout(err) {
				return json.RawMessage(`{"timed_out":true}`), nil
			}
			return nil, err // includes the yield sentinel on the first pass
		}
		return nil, NewNonRetryError("signal unexpectedly delivered")
	}}
	rig := newStepsRig(t, func(e *WorkerEngine) { e.RegisterActivity("slow_gate", h) })

	start := time.Now()
	fut, err := rig.engine.GetActivityExecutor().
		Activity("slow_gate").
		Timeout(2 * time.Second). // 4s wait in a 2s budget → must park
		Payload(json.RawMessage(`{}`)).
		Execute(context.Background())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	res := rig.await(t, fut.activityID, 30*time.Second)
	elapsed := time.Since(start)
	if res.State != storage.ResultOk || !strings.Contains(string(res.Data), "timed_out") {
		t.Fatalf("result = %v %s, want persisted-deadline timeout", res.State, res.Data)
	}
	if elapsed < 4*time.Second {
		t.Fatalf("completed in %v — the 4s wait deadline was not honored across the park", elapsed)
	}
	if got := invocations.Load(); got != 2 {
		t.Fatalf("handler invoked %d times, want 2 (park + timed-out replay)", got)
	}
	snap, _ := rig.backend.GetActivity(context.Background(), fut.activityID)
	if snap == nil || snap.RetryCount != 0 {
		t.Fatalf("snapshot %+v, want retry_count 0", snap)
	}
}

// Signalling a nonexistent activity is rejected with a not-found error
// specifically — not just any backend failure.
func TestSignalNonexistentActivity(t *testing.T) {
	rig := newStepsRig(t, func(e *WorkerEngine) {})
	err := rig.engine.Signal(context.Background(), uuid.New(), "ghost", nil)
	if err == nil {
		t.Fatal("signal to nonexistent activity succeeded; it must be rejected")
	}
	we, ok := IsWorkerError(err)
	if !ok {
		t.Fatalf("error type = %T (%v), want *WorkerError", err, err)
	}
	se, ok := storage.IsStorageError(we.Cause)
	if !ok || se.Kind != storage.ErrNotFound {
		t.Fatalf("error = %v (cause %v), want a storage not-found rejection", err, we.Cause)
	}
}

func errString(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}
