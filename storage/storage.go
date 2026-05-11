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

// IdempotencyResult is returned by CheckIdempotency when an existing activity
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
	Pending       uint64
	Processing    uint64
	Scheduled     uint64
	Retrying      uint64
	Failed        uint64
	DeadLetter    uint64
	ByPriority    PriorityBreakdown
	MaxWorkers    *int
	ActiveWorkers uint64 // distinct current_worker_id across processing rows
}

// PriorityBreakdown counts activities by priority level.
type PriorityBreakdown struct {
	Critical uint64
	High     uint64
	Normal   uint64
	Low      uint64
}

// ActivitySnapshot is imported from observability - re-declare here for the interface.
// The actual type lives in observability/models.go. We use the same structure.
// To avoid circular imports, storage defines its own snapshot type.
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
	EventEnqueued      ActivityEventType = "Enqueued"
	EventScheduled     ActivityEventType = "Scheduled"
	EventDequeued      ActivityEventType = "Dequeued"
	EventStarted       ActivityEventType = "Started"
	EventCompleted     ActivityEventType = "Completed"
	EventFailed        ActivityEventType = "Failed"
	EventRetrying      ActivityEventType = "Retrying"
	EventDeadLetter    ActivityEventType = "DeadLetter"
	EventRequeued      ActivityEventType = "Requeued"
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

// QueueStorage defines core queue operations.
type QueueStorage interface {
	ResultStorage

	Enqueue(ctx context.Context, activity QueuedActivity) error
	Dequeue(ctx context.Context, workerID string, timeout time.Duration, activityTypes []string) (*QueuedActivity, error)
	AckSuccess(ctx context.Context, activityID uuid.UUID, result json.RawMessage, workerID string) error
	// AckFailure marks an activity as failed. Returns true if moved to dead letter queue.
	AckFailure(ctx context.Context, activityID uuid.UUID, failure FailureKind, workerID string) (bool, error)
	ProcessScheduled(ctx context.Context) (uint64, error)
	RequeueExpired(ctx context.Context, batchSize int) (uint64, error)
	ExtendLease(ctx context.Context, activityID uuid.UUID, extendBy time.Duration) (bool, error)
	StoreResult(ctx context.Context, activityID uuid.UUID, result ActivityResult) error
	// CheckIdempotency claims the activity's idempotency key, or — if a row
	// already owns the key — returns a result describing the existing
	// activity (its id and the parent_activity_id recorded on it). A nil
	// result with a nil error means the caller's new activity has the claim.
	CheckIdempotency(ctx context.Context, activity *QueuedActivity) (*IdempotencyResult, error)
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
	// ListRecentRoots returns top-level activities (no parent), newest first. Used by the
	// console workflows list to show distinct runs rather than every activity in every tree.
	ListRecentRoots(ctx context.Context, offset, limit int) ([]ActivitySnapshot, error)
	// ListRecentActivities returns all activities (regardless of lineage), newest first.
	// Used by the console workflows list when "flatten" is enabled.
	ListRecentActivities(ctx context.Context, offset, limit int) ([]ActivitySnapshot, error)
	// ListCronActivities returns recent activities tagged with metadata.source='cron',
	// regardless of status. Used by the console Schedules page to group cron runs.
	ListCronActivities(ctx context.Context, offset, limit int) ([]ActivitySnapshot, error)
	// EventStream returns a channel that yields real-time activity events.
	EventStream(ctx context.Context) (<-chan ActivityEvent, error)
}

// Storage combines QueueStorage and InspectionStorage.
type Storage interface {
	QueueStorage
	InspectionStorage
}

// LeaseConfigurer is an optional interface backends can implement to accept
// the lease duration from the engine configuration at startup.
type LeaseConfigurer interface {
	SetLeaseMS(leaseMS int64)
}
