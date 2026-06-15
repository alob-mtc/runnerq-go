package runnerq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ActivityContext is provided to activity handlers during execution.
type ActivityContext struct {
	// ActivityID is the unique identifier for this activity instance.
	ActivityID uuid.UUID

	// ActivityType is the type of activity being executed.
	ActivityType string

	// RetryCount is the current retry attempt number (0 for first execution).
	RetryCount uint32

	// Metadata is custom metadata associated with the activity.
	Metadata map[string]string

	// Ctx is the Go context for cancellation and deadline propagation.
	Ctx context.Context

	// ActivityExecutor allows executing other activities from within a handler.
	// Spawns made through this executor are tagged as children of this activity.
	ActivityExecutor ActivityExecutor

	// ParentActivityID is the direct parent of this activity, if any.
	ParentActivityID *uuid.UUID

	// RootActivityID is the root of the lineage tree this activity belongs to.
	// For root activities, RootActivityID equals ActivityID.
	RootActivityID uuid.UUID

	// Depth is the lineage depth of this activity (0 for roots).
	Depth uint16

	// queue gives Run and Sleep access to checkpoint storage. Set by the
	// engine; nil when an ActivityContext is constructed by hand (e.g. in
	// handler unit tests), in which case Run executes fn without
	// checkpointing and Sleep waits in-process.
	queue activityQueue
}

// deriveCheckpointID derives the stable identity of a named checkpoint of an
// activity. UUIDv5 over (activity ID, kind:name) — any process deriving the
// same (activity, kind, name) gets the same ID, which is what lets a retried
// handler find a previous attempt's stored value, and an external process
// address a signal at a waiting activity.
func deriveCheckpointID(activityID uuid.UUID, kind, name string) uuid.UUID {
	return uuid.NewSHA1(activityID, []byte(kind+":"+name))
}

func (c ActivityContext) checkpointID(kind, name string) uuid.UUID {
	return deriveCheckpointID(c.ActivityID, kind, name)
}

// Run executes fn as a named, checkpointed step: its successful result (or
// permanent failure) is persisted, and when a retried handler reaches the
// same step it returns the stored outcome WITHOUT re-running fn. Use it for
// local side effects that must not repeat across retries — payments, emails,
// non-idempotent API calls.
//
// Semantics:
//   - success           → result stored; later attempts return it instantly.
//   - NonRetryError     → failure stored; later attempts return the same
//     error without re-running fn.
//   - retryable error   → nothing stored; fn runs again on the next attempt.
//   - crash AFTER fn but BEFORE the result commits → fn runs again. Run is
//     at-least-once with at-most-once-per-recorded-success; fn should be as
//     idempotent as the external system allows (e.g. pass an idempotency key
//     to the payment provider).
//
// Step names must be stable across retries and unique within the handler.
func (c ActivityContext) Run(name string, fn func() (json.RawMessage, error)) (json.RawMessage, error) {
	if name == "" {
		// An empty name would alias every unnamed Run onto one checkpoint
		// key, replaying the wrong stored result on retry.
		return nil, NewNonRetryError("Run requires a non-empty step name")
	}
	if c.queue == nil {
		// Hand-constructed context (unit tests): no checkpoint storage.
		return fn()
	}
	checkID := c.checkpointID("run", name)

	if stored, err := c.queue.GetResult(c.Ctx, checkID); err != nil {
		return nil, err
	} else if stored != nil {
		if stored.State == ResultOk {
			return stored.Data, nil
		}
		var failure struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(stored.Data, &failure)
		return nil, NewNonRetryError(failure.Error)
	}

	out, fnErr := fn()
	if fnErr != nil {
		if re, ok := fnErr.(RetryableError); ok && !re.IsRetryable() {
			// Permanent failure: checkpoint it so retries of the PARENT (for
			// unrelated reasons) don't re-run a step that failed for good.
			failureJSON, _ := json.Marshal(map[string]string{"error": fnErr.Error()})
			if err := c.queue.StoreResult(c.Ctx, checkID, c.ActivityID, activityResult{Data: failureJSON, State: ResultErr}, "run:"+name); err != nil {
				return nil, err
			}
		}
		return nil, fnErr
	}

	if err := c.queue.StoreResult(c.Ctx, checkID, c.ActivityID, activityResult{Data: out, State: ResultOk}, "run:"+name); err != nil {
		// The side effect happened but the checkpoint didn't commit; surface
		// a retryable error so the attempt retries — fn will run again, which
		// is the documented at-least-once edge.
		return nil, NewRetryError(fmt.Sprintf("step %q ran but checkpoint failed: %v", name, err))
	}
	return out, nil
}

// yieldMargin is how much handler-deadline headroom Sleep requires to wait
// in-process. A wake that wouldn't land at least this far before the
// activity's timeout yields instead, so the sleep never converts into a
// spurious timeout-retry.
const yieldMargin = 2 * time.Second

// yieldPark is the sentinel error a yielding durable wait (Sleep,
// WaitForSignal, or an in-handler GetResult) returns. The engine intercepts
// it and parks the activity row as 'waiting' until wakeAt — without counting
// a retry — instead of treating it as a failure. Parked waits are woken
// early by what they wait for: signal delivery or child completion.
//
// recheck closes the park race: a checkpoint/result that commits between the
// handler's final check and the park landing produces no wake of its own
// (the row wasn't 'waiting' yet when the producer looked). When recheck is
// set, the engine re-checks that result AFTER the park commits and self-
// wakes the activity if it exists.
type yieldPark struct {
	wakeAt  time.Time
	kind    string // "sleep" | "signal" | "await" — for the Yielded event / console
	step    string
	recheck uuid.UUID // result ID to re-check post-park; uuid.Nil = none
}

func (y *yieldPark) Error() string {
	return fmt.Sprintf("durable wait %q yields until %s", y.step, y.wakeAt.Format(time.RFC3339))
}

// Sleep is a durable timer: the wake deadline is persisted under the step
// name on first execution, so a handler that crashes or is redeployed
// mid-sleep resumes with only the REMAINDER of the wait — a 24h sleep does
// not restart from zero, and an already-elapsed sleep returns immediately on
// replay.
//
// When the remaining wait fits inside the activity's timeout budget, Sleep
// waits in-process. When it doesn't, Sleep YIELDS: it returns a sentinel
// error that the caller MUST propagate unchanged (`if err != nil { return
// nil, err }`); the engine intercepts it, parks the activity as scheduled
// until the wake time without consuming a retry, and re-invokes the handler
// afterwards — earlier Run/Step checkpoints fast-forward and this Sleep
// returns nil.
//
// Step names must be stable across retries and unique within the handler.
func (c ActivityContext) Sleep(name string, d time.Duration) error {
	if name == "" {
		// An empty name would alias every unnamed Sleep onto one checkpoint
		// key, replaying the wrong wake deadline on retry.
		return NewNonRetryError("Sleep requires a non-empty step name")
	}
	if c.queue == nil {
		// Hand-constructed context (unit tests): plain in-process wait.
		select {
		case <-time.After(d):
			return nil
		case <-c.Ctx.Done():
			return c.Ctx.Err()
		}
	}
	checkID := c.checkpointID("sleep", name)

	var wakeAt time.Time
	if stored, err := c.queue.GetResult(c.Ctx, checkID); err != nil {
		return err
	} else if stored != nil {
		var cp struct {
			WakeAt time.Time `json:"wake_at"`
		}
		if err := json.Unmarshal(stored.Data, &cp); err != nil {
			return NewNonRetryError(fmt.Sprintf("corrupt sleep checkpoint %q: %v", name, err))
		}
		wakeAt = cp.WakeAt
	} else {
		wakeAt = time.Now().UTC().Add(d)
		cpJSON, _ := json.Marshal(map[string]time.Time{"wake_at": wakeAt})
		// Persist BEFORE waiting, so a crash mid-sleep resumes the remainder
		// instead of restarting the full duration.
		if err := c.queue.StoreResult(c.Ctx, checkID, c.ActivityID, activityResult{Data: cpJSON, State: ResultOk}, "sleep:"+name); err != nil {
			return err
		}
	}

	remaining := time.Until(wakeAt)
	if remaining <= 0 {
		return nil
	}

	// Yield when the wake wouldn't comfortably precede the handler deadline:
	// burning the rest of this attempt on a wait that ends in a timeout-retry
	// would consume retry budget and hold a goroutine for nothing. The margin
	// is capped at half the remaining budget so short-timeout handlers (≤ 2×
	// yieldMargin) can still take short sleeps in-process instead of paying a
	// reschedule round-trip for every wait.
	if deadline, ok := c.Ctx.Deadline(); ok {
		margin := max(min(yieldMargin, time.Until(deadline)/2), 0)
		if wakeAt.After(deadline.Add(-margin)) {
			return &yieldPark{wakeAt: wakeAt, kind: "sleep", step: name}
		}
	}

	select {
	case <-time.After(remaining):
		return nil
	case <-c.Ctx.Done():
		return c.Ctx.Err()
	}
}

// signalParkHorizon is the park deadline for WaitForSignal calls with no
// timeout — effectively "until signalled". The parked row's scheduled_at is
// never reached; delivery flips it to pending early.
const signalParkHorizon = 100 * 365 * 24 * time.Hour

// WaitForSignal blocks until an external signal named name is delivered to
// THIS activity (via WorkerEngine.Signal or runnerq.SignalActivity from any
// process sharing the database) and returns its payload. timeout bounds the
// wait, measured from the FIRST attempt that reached this call — replays
// share the persisted deadline, they don't restart it; 0 means wait forever.
// On timeout it returns a non-retryable error (check with IsSignalTimeout).
//
// Signals are buffered: one delivered before the handler reaches this call —
// or before the activity even started — is returned immediately, including on
// replay. Repeated signals with the same name overwrite the payload
// (last write wins).
//
// Like Sleep, a wait that doesn't fit the handler's timeout budget YIELDS:
// the sentinel error MUST be propagated unchanged; the engine parks the
// activity (no retry consumed, no worker held) until delivery wakes it or the
// wait deadline passes. Signal names must be stable across retries and
// unique within the handler.
func (c ActivityContext) WaitForSignal(name string, timeout time.Duration) (json.RawMessage, error) {
	if name == "" {
		// An empty name would alias every unnamed wait onto one checkpoint
		// key, delivering the wrong signal on retry.
		return nil, NewNonRetryError("WaitForSignal requires a non-empty signal name")
	}
	if timeout < 0 {
		// A computed negative duration silently becoming "wait forever"
		// would be a surprising and hard-to-debug outcome — fail fast
		// before the wait checkpoint is written.
		return nil, NewNonRetryError("WaitForSignal timeout must be >= 0 (0 = wait forever)")
	}
	if c.queue == nil {
		return nil, NewNonRetryError("WaitForSignal requires engine checkpoint storage; it cannot run on a hand-constructed ActivityContext")
	}
	sigID := c.checkpointID("signal", name)

	// Persist the wait deadline on first arrival so timeout is measured from
	// the first wait, not restarted by every replay. nil deadline = forever.
	var deadline *time.Time
	waitID := c.checkpointID("signalwait", name)
	if stored, err := c.queue.GetResult(c.Ctx, waitID); err != nil {
		return nil, err
	} else if stored != nil {
		var cp struct {
			Deadline *time.Time `json:"deadline"`
		}
		if err := json.Unmarshal(stored.Data, &cp); err != nil {
			return nil, NewNonRetryError(fmt.Sprintf("corrupt signal-wait checkpoint %q: %v", name, err))
		}
		deadline = cp.Deadline
	} else {
		if timeout > 0 {
			d := time.Now().UTC().Add(timeout)
			deadline = &d
		}
		cpJSON, _ := json.Marshal(map[string]*time.Time{"deadline": deadline})
		// Step left "" this pass: signal step history is the follow-up (C).
		if err := c.queue.StoreResult(c.Ctx, waitID, c.ActivityID, activityResult{Data: cpJSON, State: ResultOk}, ""); err != nil {
			return nil, err
		}
	}

	for {
		stored, err := c.queue.GetResult(c.Ctx, sigID)
		if err != nil {
			return nil, err
		}
		if stored != nil {
			return stored.Data, nil
		}

		if deadline != nil && !time.Now().Before(*deadline) {
			// One final check before declaring a timeout: a signal that
			// committed between the lookup above and this deadline check
			// would otherwise be misreported as missing. Ties go to the
			// signal.
			if stored, err := c.queue.GetResult(c.Ctx, sigID); err != nil {
				return nil, err
			} else if stored != nil {
				return stored.Data, nil
			}
			return nil, &WorkerError{
				Kind:    ErrSignalTimeoutW,
				Message: fmt.Sprintf("signal %q was not delivered within the wait deadline", name),
			}
		}

		wake := time.Now().UTC().Add(signalParkHorizon)
		if deadline != nil {
			wake = *deadline
		}

		// Same yield policy as Sleep: don't burn this attempt on a wait that
		// would end in a timeout-retry. Parked waits are woken early by
		// delivery (SignalActivity flips the scheduled row to pending).
		if ctxDeadline, ok := c.Ctx.Deadline(); ok {
			margin := max(min(yieldMargin, time.Until(ctxDeadline)/2), 0)
			if wake.After(ctxDeadline.Add(-margin)) {
				return nil, &yieldPark{wakeAt: wake, kind: "signal", step: name, recheck: sigID}
			}
		}

		// In-process wait on the signal's result row — notification-driven
		// across processes, with the backend's fallback re-checks — bounded
		// by the wait deadline so the timeout error above can fire.
		stored, err = c.waitForCheckpoint(sigID, wake)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && c.Ctx.Err() == nil {
				continue // wait deadline reached → loop re-checks and times out
			}
			return nil, err
		}
		return stored.Data, nil
	}
}

// waitForCheckpoint blocks on a checkpoint row until it exists or wake
// passes.
func (c ActivityContext) waitForCheckpoint(id uuid.UUID, wake time.Time) (*activityResult, error) {
	waitCtx, cancel := context.WithDeadline(c.Ctx, wake)
	defer cancel()
	return c.queue.WaitForResult(waitCtx, id)
}

// ActivityHandler is the interface that all activity handlers must implement.
// Implementations should be safe for concurrent use.
type ActivityHandler interface {
	// ActivityType returns the activity type string this handler processes.
	ActivityType() string

	// Handle processes the activity with the given payload and context.
	// Returns:
	//   (result, nil)       - completed successfully, result may be nil
	//   (nil, RetryError)   - failed but should be retried
	//   (nil, NonRetryError)- failed permanently
	Handle(ctx ActivityContext, payload json.RawMessage) (json.RawMessage, error)

	// OnDeadLetter is called when an activity enters the dead letter state.
	// The default behavior (if not overridden by embedding DefaultDeadLetterHandler) is a no-op.
	OnDeadLetter(ctx ActivityContext, payload json.RawMessage, errorMsg string)
}

// DefaultDeadLetterHandler provides a no-op OnDeadLetter implementation.
// Embed this in your handler struct if you don't need dead letter handling.
type DefaultDeadLetterHandler struct{}

// OnDeadLetter is a no-op implementation.
func (DefaultDeadLetterHandler) OnDeadLetter(_ ActivityContext, _ json.RawMessage, _ string) {}
