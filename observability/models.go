package observability

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// ActivityPriority mirrors the root package type for observability use.
type ActivityPriority = int

const (
	PriorityLow      ActivityPriority = 1
	PriorityNormal   ActivityPriority = 2
	PriorityHigh     ActivityPriority = 3
	PriorityCritical ActivityPriority = 4
)

// ActivityStatus mirrors the root package type for observability use.
type ActivityStatus = string

const (
	StatusPending    ActivityStatus = "Pending"
	StatusRunning    ActivityStatus = "Running"
	StatusCompleted  ActivityStatus = "Completed"
	StatusFailed     ActivityStatus = "Failed"
	StatusRetrying   ActivityStatus = "Retrying"
	StatusDeadLetter ActivityStatus = "DeadLetter"
)

// ActivitySnapshot is a rich view of an activity's current state.
type ActivitySnapshot struct {
	ID                uuid.UUID         `json:"id"`
	ActivityType      string            `json:"activity_type"`
	Payload           json.RawMessage   `json:"payload"`
	Priority          ActivityPriority  `json:"priority"`
	Status            ActivityStatus    `json:"status"`
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
}

// ActivityEventType classifies lifecycle events.
type ActivityEventType string

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
)

// ActivityEvent records a lifecycle event for an activity.
type ActivityEvent struct {
	ActivityID uuid.UUID         `json:"activity_id"`
	Timestamp  time.Time         `json:"timestamp"`
	EventType  ActivityEventType `json:"event_type"`
	WorkerID   *string           `json:"worker_id,omitempty"`
	Detail     json.RawMessage   `json:"detail,omitempty"`
}

// DeadLetterRecord is an activity that exhausted retries or permanently failed.
type DeadLetterRecord struct {
	Activity ActivitySnapshot `json:"activity"`
	Error    string           `json:"error"`
	FailedAt time.Time        `json:"failed_at"`
}

// QueueStats provides queue monitoring statistics.
type QueueStats struct {
	PendingActivities    uint64 `json:"pending_activities"`
	ProcessingActivities uint64 `json:"processing_activities"`
	CriticalPriority     uint64 `json:"critical_priority"`
	HighPriority         uint64 `json:"high_priority"`
	NormalPriority       uint64 `json:"normal_priority"`
	LowPriority          uint64 `json:"low_priority"`
	ScheduledActivities  uint64 `json:"scheduled_activities"`
	DeadLetterActivities uint64 `json:"dead_letter_activities"`
	MaxWorkers           *int   `json:"max_workers,omitempty"`
}
