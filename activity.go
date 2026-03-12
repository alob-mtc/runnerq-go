package runnerq

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// ActivityPriority determines the order of execution.
// Higher priority activities are processed first.
type ActivityPriority int

const (
	PriorityLow      ActivityPriority = 1
	PriorityNormal   ActivityPriority = 2
	PriorityHigh     ActivityPriority = 3
	PriorityCritical ActivityPriority = 4
)

func (p ActivityPriority) String() string {
	switch p {
	case PriorityLow:
		return "Low"
	case PriorityNormal:
		return "Normal"
	case PriorityHigh:
		return "High"
	case PriorityCritical:
		return "Critical"
	default:
		return "Normal"
	}
}

func (p ActivityPriority) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.String())
}

func (p *ActivityPriority) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		var n int
		if err2 := json.Unmarshal(data, &n); err2 != nil {
			return err
		}
		*p = ActivityPriority(n)
		return nil
	}
	switch s {
	case "Low":
		*p = PriorityLow
	case "Normal":
		*p = PriorityNormal
	case "High":
		*p = PriorityHigh
	case "Critical":
		*p = PriorityCritical
	default:
		*p = PriorityNormal
	}
	return nil
}

// OnDuplicate defines behavior when an activity with the same idempotency key exists.
type OnDuplicate int

const (
	// AllowReuse always creates a new activity, updating the idempotency record.
	AllowReuse OnDuplicate = iota
	// ReturnExisting returns the existing ActivityFuture if key exists.
	ReturnExisting
	// AllowReuseOnFailure allows reuse only if the previous activity failed.
	AllowReuseOnFailure
	// NoReuse returns an error if the key exists.
	NoReuse
)

// ActivityStatus tracks the activity lifecycle.
type ActivityStatus string

const (
	StatusPending    ActivityStatus = "Pending"
	StatusRunning    ActivityStatus = "Running"
	StatusCompleted  ActivityStatus = "Completed"
	StatusFailed     ActivityStatus = "Failed"
	StatusRetrying   ActivityStatus = "Retrying"
	StatusDeadLetter ActivityStatus = "DeadLetter"
)

// ActivityOption configures an activity's execution parameters.
type ActivityOption struct {
	Priority       *ActivityPriority
	MaxRetries     uint32
	TimeoutSeconds uint64
	DelaySeconds   *uint64
	IdempotencyKey *IdempotencyConfig
}

// IdempotencyConfig holds idempotency key and its behavior.
type IdempotencyConfig struct {
	Key      string
	Behavior OnDuplicate
}

// activity is an internal representation of an activity to be processed.
type activity struct {
	ID                uuid.UUID          `json:"id"`
	ActivityType      string             `json:"activity_type"`
	Payload           json.RawMessage    `json:"payload"`
	Priority          ActivityPriority   `json:"priority"`
	Status            ActivityStatus     `json:"status"`
	CreatedAt         time.Time          `json:"created_at"`
	ScheduledAt       *time.Time         `json:"scheduled_at,omitempty"`
	RetryCount        uint32             `json:"retry_count"`
	MaxRetries        uint32             `json:"max_retries"`
	TimeoutSeconds    uint64             `json:"timeout_seconds"`
	RetryDelaySeconds uint64             `json:"retry_delay_seconds"`
	Metadata          map[string]string  `json:"metadata"`
	IdempotencyKey    *IdempotencyConfig `json:"idempotency_key,omitempty"`
}

func newActivity(activityType string, payload json.RawMessage, option *ActivityOption) *activity {
	priority := PriorityNormal
	maxRetries := uint32(3)
	timeoutSeconds := uint64(300)
	var scheduledAt *time.Time
	var idempotencyKey *IdempotencyConfig

	if option != nil {
		if option.Priority != nil {
			priority = *option.Priority
		}
		maxRetries = option.MaxRetries
		timeoutSeconds = option.TimeoutSeconds
		if option.DelaySeconds != nil {
			t := time.Now().UTC().Add(time.Duration(*option.DelaySeconds) * time.Second)
			scheduledAt = &t
		}
		idempotencyKey = option.IdempotencyKey
	}

	return &activity{
		ID:                uuid.New(),
		ActivityType:      activityType,
		Payload:           payload,
		Priority:          priority,
		Status:            StatusPending,
		CreatedAt:         time.Now().UTC(),
		ScheduledAt:       scheduledAt,
		RetryCount:        0,
		MaxRetries:        maxRetries,
		TimeoutSeconds:    timeoutSeconds,
		RetryDelaySeconds: 1,
		Metadata:          make(map[string]string),
		IdempotencyKey:    idempotencyKey,
	}
}
