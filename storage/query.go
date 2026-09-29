package storage

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// QueryStorage is the read surface RunnerQ Cloud queries through (see the
// conductor package). It is a general query layer over activities, their
// events and steps: filter, sort, project, paginate and aggregate, using the
// Cloud's canonical, backend-neutral model.
//
// Scope: queries span every queue in the backend's database; filter on
// "queue" to narrow. Every executor of an app therefore answers the same way.
//
// Backends advertise what they can evaluate efficiently through
// QueryCapabilities and must reject anything else with ErrUnsupported (never
// silently ignore a filter, which would return wrong data).
type QueryStorage interface {
	QueryCapabilities() QueryCapabilities
	QueryActivities(ctx context.Context, q ActivityQuery) (*ActivityRecordPage, error)
	// CountActivities counts matches up to limit; exact is false when the
	// count stopped at limit.
	CountActivities(ctx context.Context, filter *QueryFilter, limit int64) (count int64, exact bool, err error)
	AggregateActivities(ctx context.Context, q AggregateQuery) (*AggregateRows, error)
	QueryEvents(ctx context.Context, q EventQuery) (*EventRecordPage, error)
	// ListStepEntries lists an activity's durable steps, oldest first.
	ListStepEntries(ctx context.Context, activityID uuid.UUID, includeData bool, limit int, cursor string) (*StepEntryPage, error)
	// GetActivityTree returns the tree the activity belongs to (any member
	// may be named), root first, up to maxNodes.
	GetActivityTree(ctx context.Context, activityID uuid.UUID, include RecordInclude, maxNodes int) (*ActivityTree, error)
}

// Canonical activity statuses.
const (
	RecordStatusPending    = "pending"
	RecordStatusScheduled  = "scheduled"
	RecordStatusRunning    = "running"
	RecordStatusWaiting    = "waiting"
	RecordStatusCompleted  = "completed"
	RecordStatusFailed     = "failed"
	RecordStatusDeadLetter = "dead_letter"
	RecordStatusCancelled  = "cancelled"
)

// Canonical event types. Backends map their internal event names onto these;
// events with no canonical equivalent keep a namespaced name of their own.
const (
	RecordEventCreated         = "activity.created"
	RecordEventScheduled       = "activity.scheduled"
	RecordEventCancelled       = "activity.cancelled"
	RecordEventAttemptStarted  = "attempt.started"
	RecordEventAttemptOK       = "attempt.succeeded"
	RecordEventAttemptFailed   = "attempt.failed"
	RecordEventAttemptTimedOut = "attempt.timed_out"
	RecordEventLeaseExpired    = "attempt.lease_expired"
	RecordEventLeaseExtended   = "attempt.lease_extended"
	RecordEventWaitParked      = "wait.parked"
	RecordEventSignalReceived  = "signal.received"
	RecordEventResultStored    = "result.stored"
	RecordEventChildLinked     = "child.linked"
	RecordEventDeadLetter      = "dead_letter.entered"
	RecordEventRedriven        = "dead_letter.redriven"
	RecordEventRetried         = "activity.retried"
	RecordEventRunNow          = "activity.run_now"
	RecordEventRescheduled     = "activity.rescheduled"
	RecordEventPriorityChanged = "activity.priority_changed"
)

// Filter operators.
const (
	OpEq       = "eq"
	OpNe       = "ne"
	OpIn       = "in"
	OpNin      = "nin"
	OpLt       = "lt"
	OpLte      = "lte"
	OpGt       = "gt"
	OpGte      = "gte"
	OpExists   = "exists"
	OpPrefix   = "prefix"
	OpContains = "contains"
)

// QueryFilter is a boolean expression. Exactly one of And, Or, Not or the
// Field/Op predicate is set. Value holds decoded JSON: string, float64, bool,
// []any or nil. Timestamps are RFC 3339 strings. Field "metadata.<key>"
// addresses a metadata tag.
type QueryFilter struct {
	And   []QueryFilter
	Or    []QueryFilter
	Not   *QueryFilter
	Field string
	Op    string
	Value any
}

// QuerySort orders by one field; the backend adds a unique tiebreaker.
type QuerySort struct {
	Field string
	Desc  bool
}

// RecordInclude selects heavy fields, which are omitted unless requested.
type RecordInclude struct {
	Payload   bool
	Result    bool
	LastError bool
}

// ActivityQuery is a filtered, sorted, paginated activity query.
type ActivityQuery struct {
	Filter  *QueryFilter
	Sort    *QuerySort // nil: created_at descending
	Include RecordInclude
	Limit   int    // 0: 50; capped at 1000
	Cursor  string // opaque, from a previous page
}

// RecordError describes a failure.
type RecordError struct {
	Message string
	Kind    string
	At      *time.Time
}

// RecordWait describes why a waiting activity is parked.
type RecordWait struct {
	Kind  string // sleep | signal | children | other
	Name  string
	Until *time.Time
}

// ActivityRecord is an activity in the canonical model.
type ActivityRecord struct {
	ID       uuid.UUID
	Type     string
	Queue    string
	Status   string // canonical
	Priority int
	RootID   uuid.UUID
	ParentID *uuid.UUID
	Depth    int
	// IdempotencyKey is the key the application set (see
	// ApplicationIdempotencyKey), never the stored encoding; empty when the
	// application set none. Filters on "idempotency_key" match this form.
	IdempotencyKey string
	// Attempt is the attempt running or next to run, from 1; for a terminal
	// activity, the number of attempts made.
	Attempt int
	// MaxAttempts is the total number of attempts allowed; 0 is unlimited.
	MaxAttempts    int
	CreatedAt      time.Time
	ScheduledFor   *time.Time
	StartedAt      *time.Time
	CompletedAt    *time.Time
	UpdatedAt      time.Time
	Timeout        time.Duration
	LeaseExpiresAt *time.Time
	// ExecutorID is the engine instance running it (running activities only).
	ExecutorID string
	Wait       *RecordWait
	Metadata   map[string]string
	LastError  *RecordError    // only with Include.LastError
	Payload    json.RawMessage // only with Include.Payload
	Result     *ActivityResult // only with Include.Result
}

// ActivityRecordPage is one page of activities.
type ActivityRecordPage struct {
	Items      []ActivityRecord
	NextCursor string
}

// AggregateBucket splits an aggregate into fixed time buckets.
type AggregateBucket struct {
	Field    string // created_at | completed_at
	Interval time.Duration
	From     *time.Time
	To       *time.Time
}

// DurationMetric asks for percentiles of a duration.
type DurationMetric struct {
	Field       string    // queue | run | total
	Percentiles []float64 // 0 < p < 100
}

// AggregateQuery groups and measures activities.
type AggregateQuery struct {
	Filter    *QueryFilter
	GroupBy   []string
	Bucket    *AggregateBucket
	Count     bool
	Durations []DurationMetric
	Limit     int // groups; 0: 200, capped at 1000
}

// AggregateRow is one group.
type AggregateRow struct {
	Key    map[string]string
	Bucket *time.Time
	Count  int64
	// Durations maps a duration field to percentile label ("p95") to
	// milliseconds.
	Durations map[string]map[string]float64
}

// AggregateRows is an aggregate's result.
type AggregateRows struct {
	Rows      []AggregateRow
	Truncated bool
}

// EventQuery lists lifecycle events in log order. Filterable fields are
// advertised in QueryCapabilities.EventFilters; "seq" is the event's position
// in the log (EventRecord.ID), increasing in insertion order, so
// seq > cursor tails it. A transaction that commits late can surface an
// event below an already-seen seq; tailers rescan a window below their
// cursor to catch those.
type EventQuery struct {
	Filter        *QueryFilter
	Desc          bool // newest first
	Limit         int
	Cursor        string
	IncludeDetail bool
}

// EventRecord is one lifecycle event.
type EventRecord struct {
	ID         int64
	Cursor     string
	ActivityID uuid.UUID
	Type       string // canonical
	At         time.Time
	ExecutorID string
	Detail     json.RawMessage // only with IncludeDetail
}

// EventRecordPage is one page of events.
type EventRecordPage struct {
	Items      []EventRecord
	NextCursor string
}

// StepEntry is one durable step of an activity.
type StepEntry struct {
	ID         uuid.UUID
	ActivityID uuid.UUID
	Kind       string // run | sleep | signal | other
	Name       string
	State      ResultState
	Data       json.RawMessage // only when requested
	CreatedAt  time.Time
}

// StepEntryPage is one page of steps.
type StepEntryPage struct {
	Items      []StepEntry
	NextCursor string
}

// ActivityTree is the tree an activity belongs to.
type ActivityTree struct {
	RootID    uuid.UUID
	Items     []ActivityRecord
	Truncated bool
}

// QueryCapabilities lists what a backend can evaluate.
type QueryCapabilities struct {
	ActivityFilters []string // field names; "metadata" covers metadata.<key>
	ActivitySorts   []string
	EventFilters    []string
	GroupBy         []string
	Buckets         []string
	Durations       []string
}

// NewInvalidQueryError reports malformed or out-of-range query input.
func NewInvalidQueryError(field, msg string) *StorageError {
	return &StorageError{Kind: ErrInvalidArgument, Message: msg, Field: field}
}

// NewUnsupportedQueryError reports a query feature the backend cannot evaluate.
func NewUnsupportedQueryError(field, msg string) *StorageError {
	return &StorageError{Kind: ErrUnsupported, Message: msg, Field: field}
}
