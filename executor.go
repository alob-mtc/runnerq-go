package runnerq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
)

// ActivityExecutor allows executing activities, enabling orchestration.
type ActivityExecutor interface {
	Activity(activityType string) *ActivityBuilder
}

// ActivityFuture represents a pending activity result that can be awaited.
type ActivityFuture struct {
	queue      activityQueue
	activityID uuid.UUID
}

// ActivityID returns the awaited activity's ID — the externally-shareable
// handle for a future. Hand it to another process and reconstruct the future
// there with FutureFor.
func (f *ActivityFuture) ActivityID() uuid.UUID {
	return f.activityID
}

// FutureFor reconstructs an awaitable future from an activity ID — futures
// are rehydratable across processes and restarts: any process holding a
// backend for the same queue can await any activity's result.
func FutureFor(backend storage.Storage, activityID uuid.UUID) *ActivityFuture {
	return &ActivityFuture{
		queue:      newBackendQueueAdapter(backend, nil),
		activityID: activityID,
	}
}

// WaitAll awaits every future and returns their results in order. Inside a
// handler, sequential awaiting is already efficient under replay semantics:
// the first pending child parks the parent, and on each wake every completed
// child fast-forwards — so no parallel-wait machinery is needed. Propagate
// the error unchanged, as with GetResult.
func WaitAll(ctx context.Context, futures ...*ActivityFuture) ([]json.RawMessage, error) {
	results := make([]json.RawMessage, len(futures))
	for i, f := range futures {
		if f == nil {
			return nil, &WorkerError{Kind: ErrQueue, Message: fmt.Sprintf("WaitAll: future at index %d is nil", i)}
		}
		r, err := f.GetResult(ctx)
		if err != nil {
			return nil, err
		}
		results[i] = r
	}
	return results, nil
}

// awaitParkGrace is how long an in-handler GetResult waits in-process before
// yield-parking the parent. Fast children resolve on this fast path with no
// park round-trip; slower ones cost one replay when they complete. The grace
// also bounds worst-case fan-out starvation: even if every worker is a
// parent awaiting children, all of them park within the grace window and
// free their workers for the children.
const awaitParkGrace = 2 * time.Second

// GetResult waits for and returns the completed activity result. The wait is
// notification-driven when the backend supports it (the Postgres backend
// does), with a slow table re-check as fallback — including when the caller
// is a different process from the worker producing the result.
//
// Called from inside an activity handler, GetResult waits in-process for a
// short grace and then YIELDS, parking the parent activity — no goroutine
// held, no lease, no retry consumed — until the child's completion wakes it.
// The handler replays, earlier Step/Run checkpoints fast-forward, and this
// call returns the now-available result. The sentinel error MUST be
// propagated unchanged, same as Sleep and WaitForSignal. A parent workflow
// can therefore outlive any number of handler invocations and deploys.
//
// Called from outside a handler (a server process holding a future), it
// blocks until the result exists or ctx is done.
func (f *ActivityFuture) GetResult(ctx context.Context) (json.RawMessage, error) {
	if !inHandlerScope(ctx) {
		result, err := f.queue.WaitForResult(ctx, f.activityID)
		if err != nil {
			return nil, err
		}
		return translateResult(result)
	}

	// In-handler: wait in-process up to the grace (clamped to the handler's
	// remaining budget, same margin policy as Sleep), then park.
	bound := time.Now().Add(awaitParkGrace)
	if deadline, ok := ctx.Deadline(); ok {
		margin := max(min(yieldMargin, time.Until(deadline)/2), 0)
		if budgetBound := deadline.Add(-margin); budgetBound.Before(bound) {
			bound = budgetBound
		}
	}
	if bound.After(time.Now()) {
		wctx, cancel := context.WithDeadline(ctx, bound)
		result, err := f.queue.WaitForResult(wctx, f.activityID)
		cancel()
		if err == nil {
			return translateResult(result)
		}
		if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			return nil, err
		}
		// Grace elapsed without a result — park.
	}

	// Wake comes from the child's terminal ack (completion, failure, or
	// dead-letter — all store a result), so the horizon is nominal. recheck
	// closes the race with a child that completed while we were parking.
	return nil, &yieldPark{
		wakeAt:  time.Now().UTC().Add(signalParkHorizon),
		kind:    "await",
		step:    "await:" + f.activityID.String(),
		recheck: f.activityID,
	}
}

func translateResult(result *activityResult) (json.RawMessage, error) {
	switch result.State {
	case ResultOk:
		return result.Data, nil
	default:
		resultJSON, _ := json.Marshal(result.Data)
		return nil, &WorkerError{Kind: ErrCustom, Message: string(resultJSON)}
	}
}

// ActivityBuilder builds and executes activities with fluent configuration.
type ActivityBuilder struct {
	wrapper        *WorkerEngineWrapper
	activityType   string
	payload        json.RawMessage
	priority       *ActivityPriority
	maxRetries     *uint32
	timeout        *time.Duration
	maxRetryDelay  *time.Duration
	delay          *time.Duration
	idempotencyKey *IdempotencyConfig
	metadata       map[string]string
	asRoot         bool
	step           string
}

// Payload sets the JSON payload for the activity.
func (b *ActivityBuilder) Payload(payload json.RawMessage) *ActivityBuilder {
	b.payload = payload
	return b
}

// Priority sets the priority level.
func (b *ActivityBuilder) Priority(p ActivityPriority) *ActivityBuilder {
	b.priority = &p
	return b
}

// MaxRetries sets the maximum number of retry attempts (0 for unlimited).
func (b *ActivityBuilder) MaxRetries(retries uint32) *ActivityBuilder {
	b.maxRetries = &retries
	return b
}

// Timeout sets the maximum execution time.
func (b *ActivityBuilder) Timeout(d time.Duration) *ActivityBuilder {
	b.timeout = &d
	return b
}

// MaxRetryDelay caps the exponential backoff delay between retries.
// Defaults to 1 hour if not set. Non-positive durations are ignored.
// Values below 1 second are rounded up to 1 second.
func (b *ActivityBuilder) MaxRetryDelay(d time.Duration) *ActivityBuilder {
	if d <= 0 {
		return b
	}
	b.maxRetryDelay = &d
	return b
}

// Delay sets the delay before execution.
func (b *ActivityBuilder) Delay(d time.Duration) *ActivityBuilder {
	b.delay = &d
	return b
}

// idempotencyStorageKey is the actual key stored in the idempotency table for
// a user-supplied key on an activity of activityType. User keys are namespaced
// by activity type so the same business key can be reused across distinct
// activity types without colliding. SignalActivityByKey reconstructs the same
// composite to address a workflow by its business key — keep the two in lockstep
// by routing both through this single function.
func idempotencyStorageKey(userKey, activityType string) string {
	return fmt.Sprintf("%s-%s", userKey, activityType)
}

// IdempotencyKeyOption sets the idempotency key and behavior for duplicate detection.
func (b *ActivityBuilder) IdempotencyKeyOption(key string, behavior OnDuplicate) *ActivityBuilder {
	b.idempotencyKey = &IdempotencyConfig{Key: key, Behavior: behavior}
	return b
}

// Step makes this spawn a named, memoized step of the calling handler. The
// idempotency key is derived automatically from (root activity, parent
// activity, name) with ReturnExisting behavior, so when a retried parent
// re-issues the same spawn it gets the SAME child back instead of creating a
// duplicate — and GetResult returns the child's stored result instantly if it
// already completed. This is what lets a retried orchestrator fast-forward
// through work it finished on a previous attempt.
//
// Contract: step names must be stable across retries and unique among this
// parent's spawns (two spawns sharing a name resolve to one child). Only
// valid when called from inside an activity handler; incompatible with
// AsRoot and IdempotencyKeyOption.
func (b *ActivityBuilder) Step(name string) *ActivityBuilder {
	b.step = name
	return b
}

// Metadata sets a single metadata key/value pair on the activity.
func (b *ActivityBuilder) Metadata(key, value string) *ActivityBuilder {
	if b.metadata == nil {
		b.metadata = make(map[string]string)
	}
	b.metadata[key] = value
	return b
}

// AsRoot detaches this spawn from its parent's lineage. The resulting activity
// becomes a root (no parent, root is itself, depth 0) regardless of the
// surrounding handler context. Use for fire-and-forget side jobs whose lifecycle
// is logically independent of the parent (audit logs, async telemetry, etc.).
func (b *ActivityBuilder) AsRoot() *ActivityBuilder {
	b.asRoot = true
	return b
}

// MetadataMap sets multiple metadata fields on the activity.
// Values are merged with existing metadata and overwrite duplicate keys.
func (b *ActivityBuilder) MetadataMap(metadata map[string]string) *ActivityBuilder {
	if len(metadata) == 0 {
		return b
	}
	if b.metadata == nil {
		b.metadata = make(map[string]string, len(metadata))
	}
	maps.Copy(b.metadata, metadata)
	return b
}

// Execute enqueues the activity and returns an ActivityFuture.
func (b *ActivityBuilder) Execute(ctx context.Context) (*ActivityFuture, error) {
	if b.payload == nil {
		return nil, &WorkerError{Kind: ErrQueue, Message: "Activity payload is required"}
	}
	if b.step != "" {
		if b.idempotencyKey != nil {
			return nil, &WorkerError{Kind: ErrQueue, Message: "Step and IdempotencyKeyOption are mutually exclusive — Step derives its own key"}
		}
		if b.asRoot {
			return nil, &WorkerError{Kind: ErrQueue, Message: "Step requires parent lineage and cannot be combined with AsRoot"}
		}
	}

	hasOption := b.priority != nil || b.maxRetries != nil || b.timeout != nil || b.maxRetryDelay != nil || b.delay != nil || b.idempotencyKey != nil || len(b.metadata) > 0

	var option *ActivityOption
	if hasOption {
		maxRetries := uint32(3)
		if b.maxRetries != nil {
			maxRetries = *b.maxRetries
		}
		timeoutSec := uint64(300)
		if b.timeout != nil {
			timeoutSec = uint64(b.timeout.Seconds())
		}
		var maxRetryDelaySec *uint64
		if b.maxRetryDelay != nil {
			secs := uint64(b.maxRetryDelay.Seconds())
			if secs == 0 {
				secs = 1
			}
			maxRetryDelaySec = &secs
		}
		var delaySec *uint64
		if b.delay != nil {
			d := uint64(b.delay.Seconds())
			delaySec = &d
		}
		var idempKey *IdempotencyConfig
		if b.idempotencyKey != nil {
			idempKey = &IdempotencyConfig{Key: idempotencyStorageKey(b.idempotencyKey.Key, b.activityType), Behavior: b.idempotencyKey.Behavior}
		}
		metadata := make(map[string]string)
		maps.Copy(metadata, b.metadata)
		option = &ActivityOption{
			Priority:             b.priority,
			MaxRetries:           maxRetries,
			TimeoutSeconds:       timeoutSec,
			MaxRetryDelaySeconds: maxRetryDelaySec,
			DelaySeconds:         delaySec,
			IdempotencyKey:       idempKey,
			Metadata:             metadata,
		}
	}

	return b.wrapper.executeActivity(ctx, b.activityType, b.payload, option, b.asRoot, b.step)
}

// WorkerEngineWrapper provides activity execution capabilities.
// It implements ActivityExecutor.
type WorkerEngineWrapper struct {
	queue    activityQueue
	maxDepth uint16
	lineage  *lineageScope // nil at engine level; set when scoped to a running parent
}

// lineageScope captures the parent context that this wrapper applies to spawns.
type lineageScope struct {
	parentID   uuid.UUID
	rootID     uuid.UUID
	childDepth uint16 // depth of the CHILD being spawned (parent.Depth + 1)
}

func newWorkerEngineWrapperWithDepth(queue activityQueue, maxDepth uint16) *WorkerEngineWrapper {
	if maxDepth == 0 {
		maxDepth = DefaultMaxActivityDepth
	}
	return &WorkerEngineWrapper{queue: queue, maxDepth: maxDepth}
}

// scopedForChild returns a wrapper whose spawns will be tagged as children of parent.
func (w *WorkerEngineWrapper) scopedForChild(parent *activity) *WorkerEngineWrapper {
	rootID := parent.RootActivityID
	if rootID == (uuid.UUID{}) {
		rootID = parent.ID
	}
	// Clamp the increment so a maxed-out parent depth doesn't wrap around to
	// 0. childDepth=MaxUint16 sentinel will fail the > maxDepth check on the
	// next spawn and stop the chain cleanly.
	childDepth := uint16(math.MaxUint16)
	if parent.Depth < math.MaxUint16 {
		childDepth = parent.Depth + 1
	}
	return &WorkerEngineWrapper{
		queue:    w.queue,
		maxDepth: w.maxDepth,
		lineage: &lineageScope{
			parentID:   parent.ID,
			rootID:     rootID,
			childDepth: childDepth,
		},
	}
}

// Activity creates a fluent activity builder.
func (w *WorkerEngineWrapper) Activity(activityType string) *ActivityBuilder {
	return &ActivityBuilder{
		wrapper:      w,
		activityType: activityType,
	}
}

func (w *WorkerEngineWrapper) executeActivity(ctx context.Context, activityType string, payload json.RawMessage, option *ActivityOption, asRoot bool, step string) (*ActivityFuture, error) {
	a := newActivity(activityType, payload, option)

	if w.lineage != nil && !asRoot {
		if w.lineage.childDepth > w.maxDepth {
			return nil, &WorkerError{
				Kind:    ErrDepthExceeded,
				Message: fmt.Sprintf("activity depth %d exceeds max %d", w.lineage.childDepth, w.maxDepth),
			}
		}
		p := w.lineage.parentID
		a.ParentActivityID = &p
		a.RootActivityID = w.lineage.rootID
		a.Depth = w.lineage.childDepth
	}

	if step != "" {
		if w.lineage == nil || asRoot {
			return nil, &WorkerError{Kind: ErrQueue, Message: "Step is only valid when spawning from inside an activity handler"}
		}
		// Positional identity: a retried parent re-issuing this exact spawn
		// derives the same key and reattaches to the existing child instead
		// of duplicating it. The "rq:step:" prefix keeps the derived keyspace
		// disjoint from user-supplied idempotency keys (which are suffixed
		// with the activity type, never prefixed like this).
		a.IdempotencyKey = &IdempotencyConfig{
			Key:      fmt.Sprintf("rq:step:%s:%s:%s", a.RootActivityID, w.lineage.parentID, step),
			Behavior: ReturnExisting,
		}
	}

	activityID := a.ID

	if a.IdempotencyKey != nil {
		// Claim + enqueue happen atomically in the backend; a crash can't
		// leave the key pointing at an activity that was never enqueued.
		existing, err := w.queue.EnqueueIdempotent(ctx, a)
		if err != nil {
			return nil, WorkerErrorFromStorage(err)
		}
		if existing == nil {
			return &ActivityFuture{queue: w.queue, activityID: activityID}, nil
		}
		// Idempotency reuse: a different parent is logically spawning the same
		// child. The row's parent_activity_id stays as the original spawner;
		// we record this secondary link as an event for full audit attribution.
		// Skip the event when our parent IS the original recorded parent — that
		// would just be a same-parent retry of the same idempotency key, not a
		// new attribution.
		if w.lineage != nil && !asRoot {
			sameParent := existing.ExistingParentID != nil && *existing.ExistingParentID == w.lineage.parentID
			if !sameParent {
				if err := w.queue.RecordSpawnLinked(ctx, existing.ExistingID, w.lineage.parentID); err != nil {
					slog.Warn("Failed to record spawn link",
						"child_id", existing.ExistingID,
						"parent_id", w.lineage.parentID,
						"error", err)
				}
			}
		}
		return &ActivityFuture{queue: w.queue, activityID: existing.ExistingID}, nil
	}

	if a.ScheduledAt == nil {
		if err := w.queue.Enqueue(ctx, a); err != nil {
			return nil, WorkerErrorFromStorage(err)
		}
	} else {
		if err := w.queue.ScheduleActivity(ctx, a); err != nil {
			return nil, WorkerErrorFromStorage(err)
		}
	}

	return &ActivityFuture{queue: w.queue, activityID: activityID}, nil
}
