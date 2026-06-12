package runnerq

import (
	"context"
	"encoding/json"
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

// checkpointID derives the stable identity of a named checkpoint of THIS
// activity. UUIDv5 over (activity ID, kind:name) — the same activity row
// retried on any worker derives the same ID, which is what lets a retry find
// the previous attempt's stored value.
func (c ActivityContext) checkpointID(kind, name string) uuid.UUID {
	return uuid.NewSHA1(c.ActivityID, []byte(kind+":"+name))
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
			if err := c.queue.StoreResult(c.Ctx, checkID, c.ActivityID, activityResult{Data: failureJSON, State: ResultErr}); err != nil {
				return nil, err
			}
		}
		return nil, fnErr
	}

	if err := c.queue.StoreResult(c.Ctx, checkID, c.ActivityID, activityResult{Data: out, State: ResultOk}); err != nil {
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

// yieldSleep is the sentinel error a yielding Sleep returns. The engine
// intercepts it and reschedules the activity row to wake at wakeAt — without
// counting a retry — instead of treating it as a failure.
type yieldSleep struct {
	wakeAt time.Time
	step   string
}

func (y *yieldSleep) Error() string {
	return fmt.Sprintf("durable sleep %q yields until %s", y.step, y.wakeAt.Format(time.RFC3339))
}

// Sleep is a durable timer: the wake deadline is persisted under the step
// name on first execution, so a handler that crashes or is redeployed
// mid-sleep resumes with only the REMAINDER of the wait — a 24h sleep does
// not restart from zero, and an already-elapsed sleep returns immediately on
// replay.
//
// When the remaining wait fits inside the activity's timeout budget, Sleep
// waits in-process (releasing the worker slot under SuspendOnAwait, like
// awaiting a child). When it doesn't, Sleep YIELDS: it returns a sentinel
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
		if err := c.queue.StoreResult(c.Ctx, checkID, c.ActivityID, activityResult{Data: cpJSON, State: ResultOk}); err != nil {
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
		margin := min(yieldMargin, time.Until(deadline)/2)
		if margin < 0 {
			margin = 0
		}
		if wakeAt.After(deadline.Add(-margin)) {
			return &yieldSleep{wakeAt: wakeAt, step: name}
		}
	}

	if h := suspendFromContext(c.Ctx); h != nil {
		h.release()
		defer func() { _ = h.reacquire(c.Ctx) }()
	}
	select {
	case <-time.After(remaining):
		return nil
	case <-c.Ctx.Done():
		return c.Ctx.Err()
	}
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
