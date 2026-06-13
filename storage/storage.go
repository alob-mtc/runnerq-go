package storage

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// ActivityPriority determines execution ordering.
type ActivityPriority = int

const (
	PriorityLow      ActivityPriority = 1
	PriorityNormal   ActivityPriority = 2
	PriorityHigh     ActivityPriority = 3
	PriorityCritical ActivityPriority = 4
)

// IdempotencyBehavior defines how duplicates are handled.
type IdempotencyBehavior int

const (
	BehaviorAllowReuse IdempotencyBehavior = iota
	BehaviorReturnExisting
	BehaviorAllowReuseOnFailure
	BehaviorNoReuse
)

// ResultState indicates success or failure of an activity result.
type ResultState int

const (
	ResultOk ResultState = iota
	ResultErr
)

// QueuedActivity represents an activity ready to be enqueued.
type QueuedActivity struct {
	ID                   uuid.UUID
	ActivityType         string
	Payload              json.RawMessage
	Priority             ActivityPriority
	MaxRetries           uint32
	RetryCount           uint32
	TimeoutSeconds       uint64
	RetryDelaySeconds    uint64
	MaxRetryDelaySeconds uint64
	ScheduledAt          *time.Time
	Metadata             map[string]string
	IdempotencyKey       *IdempotencyKeyConfig
	CreatedAt            time.Time
	ParentActivityID     *uuid.UUID
	RootActivityID       uuid.UUID
	Depth                uint16
}

// IdempotencyKeyConfig holds a key and its behavior.
type IdempotencyKeyConfig struct {
	Key      string
	Behavior IdempotencyBehavior
}

// IdempotencyResult is returned by EnqueueIdempotent when an existing activity
// already owns the idempotency key. ExistingParentID is the parent recorded on
// that activity (may be nil for legacy or root activities).
type IdempotencyResult struct {
	ExistingID       uuid.UUID
	ExistingParentID *uuid.UUID
}

// DequeuedActivity is an activity claimed by a worker.
type DequeuedActivity struct {
	Activity      QueuedActivity
	LeaseID       string
	Attempt       uint32
	LeaseDeadline time.Time
}

// ActivityResult holds result data from a completed activity.
type ActivityResult struct {
	Data  json.RawMessage
	State ResultState
}

// RetentionPolicy controls how long terminal workflow trees are kept before
// the retention sweeper deletes them (activities, events, results including
// Run/Sleep checkpoints, and idempotency keys). A zero duration means "keep
// forever" for that class. The deletion unit is the whole tree rooted at a
// terminal root with no non-terminal descendants — never individual rows, so
// a retried parent can never find its children's results missing.
type RetentionPolicy struct {
	// Completed applies to trees whose root finished with status completed.
	Completed time.Duration
	// Failed applies to trees whose root is failed or dead_letter — kept on
	// a separate clock so operators can hold failures longer for inspection.
	Failed time.Duration
}

// FailureKind describes how an activity failed.
type FailureKind struct {
	Retryable bool
	Reason    string
	IsTimeout bool
}

// NewRetryableFailure creates a retryable failure.
func NewRetryableFailure(reason string) FailureKind {
	return FailureKind{Retryable: true, Reason: reason}
}

// NewNonRetryableFailure creates a non-retryable failure.
func NewNonRetryableFailure(reason string) FailureKind {
	return FailureKind{Retryable: false, Reason: reason}
}

// NewTimeoutFailure creates a timeout failure (treated as retryable).
func NewTimeoutFailure() FailureKind {
	return FailureKind{Retryable: true, Reason: "Activity execution timed out", IsTimeout: true}
}

// QueueStats holds queue-level statistics.
type QueueStats struct {
	Pending    uint64
	Processing uint64
	Scheduled  uint64
	Retrying   uint64
	// Waiting counts yield-parked activities: durable sleeps, signal waits,
	// and parents awaiting children.
	Waiting       uint64
	Failed        uint64
	DeadLetter    uint64
	ByPriority    PriorityBreakdown
	MaxWorkers    *int
	ActiveWorkers uint64 // distinct current_worker_id across processing rows
	// Roots is the same status breakdown but limited to root activities
	// (parent_activity_id IS NULL). Drives pill counts on the workflows list.
	Roots RootStatusBreakdown
}

// RootStatusBreakdown counts root activities per status.
type RootStatusBreakdown struct {
	Pending    uint64
	Processing uint64
	Scheduled  uint64
	Retrying   uint64
	Waiting    uint64
	Completed  uint64
	Failed     uint64
	DeadLetter uint64
}

// PriorityBreakdown counts activities by priority level.
type PriorityBreakdown struct {
	Critical uint64
	High     uint64
	Normal   uint64
	Low      uint64
}

// ActivitySnapshot is the canonical activity-state view returned by
// InspectionStorage. The observability package mirrors this struct in
// observability/models.go so it can ship the type to its consumers without
// importing storage (which would create a circular dependency).
type ActivitySnapshot struct {
	ID                uuid.UUID         `json:"id"`
	ActivityType      string            `json:"activity_type"`
	Payload           json.RawMessage   `json:"payload"`
	Priority          ActivityPriority  `json:"priority"`
	Status            string            `json:"status"`
	CreatedAt         time.Time         `json:"created_at"`
	ScheduledAt       *time.Time        `json:"scheduled_at,omitempty"`
	StartedAt         *time.Time        `json:"started_at,omitempty"`
	CompletedAt       *time.Time        `json:"completed_at,omitempty"`
	CurrentWorkerID   *string           `json:"current_worker_id,omitempty"`
	LastWorkerID      *string           `json:"last_worker_id,omitempty"`
	RetryCount        uint32            `json:"retry_count"`
	MaxRetries        uint32            `json:"max_retries"`
	TimeoutSeconds    uint64            `json:"timeout_seconds"`
	RetryDelaySeconds uint64            `json:"retry_delay_seconds"`
	Metadata          map[string]string `json:"metadata"`
	LastError         *string           `json:"last_error,omitempty"`
	LastErrorAt       *time.Time        `json:"last_error_at,omitempty"`
	StatusUpdatedAt   time.Time         `json:"status_updated_at"`
	Score             *float64          `json:"score,omitempty"`
	LeaseDeadlineMS   *int64            `json:"lease_deadline_ms,omitempty"`
	ProcessingMember  *string           `json:"processing_member,omitempty"`
	IdempotencyKey    *string           `json:"idempotency_key,omitempty"`
	ParentActivityID  *uuid.UUID        `json:"parent_activity_id,omitempty"`
	RootActivityID    *uuid.UUID        `json:"root_activity_id,omitempty"`
	Depth             uint16            `json:"depth"`
}

// ActivityEventType classifies lifecycle events.
type ActivityEventType = string

const (
	EventEnqueued   ActivityEventType = "Enqueued"
	EventScheduled  ActivityEventType = "Scheduled"
	EventDequeued   ActivityEventType = "Dequeued"
	EventCompleted  ActivityEventType = "Completed"
	EventFailed     ActivityEventType = "Failed"
	EventRetrying   ActivityEventType = "Retrying"
	EventDeadLetter ActivityEventType = "DeadLetter"
	// EventRequeued records the reaper returning an expired-lease activity to
	// the pending state for another attempt.
	EventRequeued ActivityEventType = "Requeued"
	// EventYielded records a durable Sleep or signal wait parking the
	// activity until its wake time without consuming a retry.
	EventYielded ActivityEventType = "Yielded"
	// EventSignaled records an external signal delivered to an activity.
	EventSignaled      ActivityEventType = "Signaled"
	EventLeaseExtended ActivityEventType = "LeaseExtended"
	EventResultStored  ActivityEventType = "ResultStored"
	// EventSpawnLinked records a secondary parent's link to an existing activity
	// when an idempotency reuse causes a different parent to claim ownership.
	EventSpawnLinked ActivityEventType = "SpawnLinked"
)

// ActivityEvent records a lifecycle event.
type ActivityEvent struct {
	ActivityID uuid.UUID         `json:"activity_id"`
	Timestamp  time.Time         `json:"timestamp"`
	EventType  ActivityEventType `json:"event_type"`
	WorkerID   *string           `json:"worker_id,omitempty"`
	Detail     json.RawMessage   `json:"detail,omitempty"`
}

// DeadLetterRecord is an activity in the dead letter queue.
type DeadLetterRecord struct {
	Activity ActivitySnapshot `json:"activity"`
	Error    string           `json:"error"`
	FailedAt time.Time        `json:"failed_at"`
}

// ResultStorage retrieves activity results.
type ResultStorage interface {
	GetResult(ctx context.Context, activityID uuid.UUID) (*ActivityResult, error)
}

// ResultWaiter is an optional capability: backends that can block efficiently
// until a result exists (e.g. via LISTEN/NOTIFY) implement it, and futures
// use it instead of polling GetResult. The wait must work across processes —
// the caller awaiting a result is routinely a different process from the
// worker that produces it, connected only through the shared database.
// Implementations must return once the result exists, or with the context's
// error on cancellation.
type ResultWaiter interface {
	WaitForResult(ctx context.Context, activityID uuid.UUID) (*ActivityResult, error)
}

// QueueStorage defines core queue operations.
type QueueStorage interface {
	ResultStorage

	Enqueue(ctx context.Context, activity QueuedActivity) error
	// Dequeue claims the next runnable activity. timeout is the maximum time
	// to block waiting for work when the queue is empty (0 = single
	// non-blocking attempt); (nil, nil) means nothing became claimable.
	Dequeue(ctx context.Context, workerID string, timeout time.Duration, activityTypes []string) (*QueuedActivity, error)
	// AckSuccess marks an activity as completed and persists its result.
	// Implementations MUST store the result atomically with the status change
	// and MUST write a result record even when result is nil, so callers
	// awaiting the result can always resolve once the activity completes.
	AckSuccess(ctx context.Context, activityID uuid.UUID, result json.RawMessage, workerID string) error
	// AckFailure marks an activity as failed. Returns true if moved to dead letter queue.
	AckFailure(ctx context.Context, activityID uuid.UUID, failure FailureKind, workerID string) (bool, error)
	ProcessScheduled(ctx context.Context) (uint64, error)
	RequeueExpired(ctx context.Context, batchSize int) (uint64, error)
	// Yield reschedules a processing activity to wake at wakeAt WITHOUT
	// counting a retry — the engine uses it when a durable Sleep's wake time
	// doesn't fit the current attempt's timeout budget. Must be fenced on
	// workerID like AckSuccess/AckFailure (only the claiming worker may
	// yield) and return a not-found error when the row is no longer claimed
	// by that worker.
	Yield(ctx context.Context, activityID uuid.UUID, wakeAt time.Time, workerID string) error
	ExtendLease(ctx context.Context, activityID uuid.UUID, extendBy time.Duration) (bool, error)
	// StoreResult persists a result row. ownerActivityID is the activity
	// whose lifetime governs the row: pass the activity's own ID for normal
	// results, or the handler's activity ID for Run/Sleep checkpoints whose
	// activityID is synthetic. The retention sweeper deletes result rows
	// together with their owner's workflow tree.
	StoreResult(ctx context.Context, activityID uuid.UUID, ownerActivityID uuid.UUID, result ActivityResult) error
	// WakeWaiting makes a yield-parked ('waiting') activity immediately
	// runnable; false when the activity is in any other state. The engine
	// uses it to close the park race (a result committing between a
	// handler's final check and its park landing produces no wake of its
	// own).
	WakeWaiting(ctx context.Context, activityID uuid.UUID) (bool, error)
	// SignalActivity delivers an external signal to an activity: it stores
	// payload as a result row under signalID (owned by activityID, so the
	// retention sweeper collects it with the tree) and, if the target is
	// parked as scheduled (yielded waiting for the signal), makes it
	// immediately runnable. Must return a not-found error when no activity
	// row matches activityID — silently storing signals for nonexistent
	// activities would leak uncollectable rows. Atomic: store + wake commit
	// together. Repeated signals with the same signalID overwrite the
	// payload (last write wins).
	SignalActivity(ctx context.Context, activityID uuid.UUID, signalID uuid.UUID, payload json.RawMessage) error
	// LookupIdempotencyActivityID returns the activity ID that currently owns
	// idempotencyKey in this queue, so a caller can address a signal by
	// business idempotency key instead of by internal activity ID (see
	// runnerq.SignalActivityByKey). The idempotency table's PRIMARY KEY makes
	// this at most one activity. Returns a not-found error when no activity
	// holds the key (never claimed, or completed and retention-swept).
	LookupIdempotencyActivityID(ctx context.Context, idempotencyKey string) (uuid.UUID, error)
	// CleanupExpired deletes terminal workflow trees older than the policy's
	// TTLs, up to batchSize roots per call, and returns the number of root
	// trees deleted. Implementations must (a) only delete trees whose root is
	// terminal AND has no non-terminal descendants, (b) delete the tree's
	// activities, events, results (by activity AND by owner), and idempotency
	// keys together, and (c) coordinate so concurrent sweepers don't duplicate
	// work (returning 0 when another sweeper holds the lease is correct).
	CleanupExpired(ctx context.Context, policy RetentionPolicy, batchSize int) (uint64, error)
	// EnqueueIdempotent claims the activity's idempotency key AND enqueues the
	// activity in a single atomic operation. A nil result with a nil error
	// means the activity was enqueued (the caller MUST NOT call Enqueue
	// separately — doing so would double-enqueue). If a row already owns the
	// key and the configured behavior keeps it, the result describes the
	// existing activity (its id and the parent_activity_id recorded on it)
	// and nothing was enqueued.
	//
	// The claim and the enqueue MUST be atomic: a crash can never leave a
	// claimed key pointing at an activity that was never enqueued, which
	// would permanently brick the key.
	EnqueueIdempotent(ctx context.Context, activity *QueuedActivity) (*IdempotencyResult, error)
	// RecordSpawnLinked records that parentID logically spawned childID, even
	// though no new activity row was created (idempotency reuse). Best-effort:
	// callers should log but not fail on errors.
	RecordSpawnLinked(ctx context.Context, childID, parentID uuid.UUID) error
	// SchedulesNatively returns true if dequeue handles scheduled activities natively.
	SchedulesNatively() bool
}

// InspectionStorage provides read-only access to queue state for monitoring.
type InspectionStorage interface {
	ResultStorage

	Stats(ctx context.Context) (*QueueStats, error)
	ListPending(ctx context.Context, offset, limit int) ([]ActivitySnapshot, error)
	ListProcessing(ctx context.Context, offset, limit int) ([]ActivitySnapshot, error)
	ListScheduled(ctx context.Context, offset, limit int) ([]ActivitySnapshot, error)
	ListCompletedNonCron(ctx context.Context, offset, limit int) ([]ActivitySnapshot, error)
	ListCompletedCron(ctx context.Context, offset, limit int) ([]ActivitySnapshot, error)
	ListCompleted(ctx context.Context, offset, limit int) ([]ActivitySnapshot, error)
	ListDeadLetter(ctx context.Context, offset, limit int) ([]DeadLetterRecord, error)
	GetActivity(ctx context.Context, activityID uuid.UUID) (*ActivitySnapshot, error)
	GetActivityEvents(ctx context.Context, activityID uuid.UUID, limit int) ([]ActivityEvent, error)
	// GetChildren returns direct children of a parent activity.
	GetChildren(ctx context.Context, parentID uuid.UUID, offset, limit int) ([]ActivitySnapshot, error)
	// GetSubtree returns all activities in the tree rooted at rootID, including the root itself.
	GetSubtree(ctx context.Context, rootID uuid.UUID) ([]ActivitySnapshot, error)
	// ListRecentRoots returns top-level activities (no parent), newest first. When
	// status is non-empty, results are filtered to that status (raw column value
	// — e.g. 'processing', 'completed', 'dead_letter').
	ListRecentRoots(ctx context.Context, status string, offset, limit int) ([]ActivitySnapshot, error)
	// ListRecentActivities returns all activities (regardless of lineage), newest
	// first. Status filter behaves the same way as ListRecentRoots.
	ListRecentActivities(ctx context.Context, status string, offset, limit int) ([]ActivitySnapshot, error)
	// ListCronActivities returns recent activities tagged with metadata.source='cron',
	// regardless of status. Used by the console Schedules page to group cron runs.
	ListCronActivities(ctx context.Context, offset, limit int) ([]ActivitySnapshot, error)
	// EventStream returns a channel that yields real-time activity events.
	EventStream(ctx context.Context) (<-chan ActivityEvent, error)
}

// WorkerPoolStorage tracks live engine instances for cluster-wide capacity
// reporting. Engines call Register on Start, Heartbeat periodically, and
// Deregister on graceful shutdown. Crashed pools age out of Stats() once
// their last_seen_at falls outside the backend's liveness window.
type WorkerPoolStorage interface {
	RegisterWorkerPool(ctx context.Context, pool WorkerPoolInfo) error
	HeartbeatWorkerPool(ctx context.Context, poolID uuid.UUID) error
	DeregisterWorkerPool(ctx context.Context, poolID uuid.UUID) error
}

// Storage combines the queue, inspection, and worker-pool surfaces a backend
// must provide.
type Storage interface {
	QueueStorage
	InspectionStorage
	WorkerPoolStorage
}

// LeaseConfigurer is an optional interface backends can implement to accept
// the lease duration from the engine configuration at startup.
type LeaseConfigurer interface {
	SetLeaseMS(leaseMS int64)
}

// WorkerPoolInfo describes one engine instance's worker pool for cluster-wide
// liveness tracking.
type WorkerPoolInfo struct {
	PoolID        uuid.UUID
	QueueName     string
	MaxWorkers    int
	ActivityTypes []string
}
