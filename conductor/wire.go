package conductor

import (
	"encoding/json"
	"fmt"
)

// Wire types for Conductor protocol v1. The contract is the spec in
// runnerq-cloud's docs/protocol.md.

// protocolVersion is the highest Conductor protocol version this agent speaks.
const protocolVersion = 1

type kind string

const (
	kindRequest  kind = "req"
	kindResponse kind = "res"
	kindEvent    kind = "evt"
)

// envelope is one frame on the wire.
type envelope struct {
	V     int                        `json:"v"`
	Kind  kind                       `json:"kind"`
	ID    string                     `json:"id,omitempty"`
	Type  string                     `json:"type"`
	Data  json.RawMessage            `json:"data,omitempty"`
	Error *wireError                 `json:"error,omitempty"`
	Meta  map[string]json.RawMessage `json:"meta,omitempty"`
}

const metaDeadline = "deadline"

type errorCode string

const (
	codeInvalidArgument    errorCode = "invalid_argument"
	codeNotFound           errorCode = "not_found"
	codeFailedPrecondition errorCode = "failed_precondition"
	codeConflict           errorCode = "conflict"
	codeForbidden          errorCode = "forbidden"
	codeUnsupported        errorCode = "unsupported"
	codeResourceExhausted  errorCode = "resource_exhausted"
	codeDeadlineExceeded   errorCode = "deadline_exceeded"
	codeUnavailable        errorCode = "unavailable"
	codeInternal           errorCode = "internal"
)

type wireError struct {
	Code    errorCode      `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func (e *wireError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

func errorf(code errorCode, format string, args ...any) *wireError {
	return &wireError{Code: code, Message: fmt.Sprintf(format, args...)}
}

func fieldError(code errorCode, field, format string, args ...any) *wireError {
	e := errorf(code, format, args...)
	e.Details = map[string]any{"field": field}
	return e
}

const (
	typeHello   = "hello"
	typeGoodbye = "goodbye"

	typeActivitiesList      = "activities.list"
	typeActivitiesGet       = "activities.get"
	typeActivitiesCount     = "activities.count"
	typeActivitiesAggregate = "activities.aggregate"
	typeStepsList           = "steps.list"
	typeEventsList          = "events.list"
	typeResultsGet          = "results.get"
	typeTreesGet            = "trees.get"
	typeExecutorDescribe    = "executor.describe"

	typeActivitiesCancel      = "activities.cancel"
	typeActivitiesRetry       = "activities.retry"
	typeActivitiesRunNow      = "activities.run_now"
	typeActivitiesReschedule  = "activities.reschedule"
	typeActivitiesSetPriority = "activities.set_priority"
	typeActivitiesDelete      = "activities.delete"
	typeActivitiesSignal      = "activities.signal"

	typeExecutorReport = "executor.report"
	typeConfigUpdate   = "config.update"
)

type dataMode string

const dataModeMetadataOnly dataMode = "metadata_only"

// --- handshake ---

type sdkInfo struct {
	Name     string `json:"name"`
	Version  string `json:"version"`
	Language string `json:"language"`
}

type executorInfo struct {
	ID             string            `json:"id"`
	Hostname       string            `json:"hostname,omitempty"`
	Queues         []string          `json:"queues,omitempty"`
	ActivityTypes  []string          `json:"activity_types,omitempty"`
	MaxConcurrency int               `json:"max_concurrency,omitempty"`
	StartedAt      string            `json:"started_at,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
}

type capability struct {
	V       int      `json:"v"`
	Filters []string `json:"filters,omitempty"`
	Sorts   []string `json:"sorts,omitempty"`
	Include []string `json:"include,omitempty"`
	GroupBy []string `json:"group_by,omitempty"`
	Buckets []string `json:"buckets,omitempty"`
	Metrics []string `json:"metrics,omitempty"`
	Targets []string `json:"targets,omitempty"`
}

type limits struct {
	MaxFrameBytes         int `json:"max_frame_bytes,omitempty"`
	MaxConcurrentRequests int `json:"max_concurrent_requests,omitempty"`
}

type hello struct {
	ProtocolVersions []int                 `json:"protocol_versions"`
	SDK              sdkInfo               `json:"sdk"`
	Executor         executorInfo          `json:"executor"`
	Capabilities     map[string]capability `json:"capabilities"`
	Limits           limits                `json:"limits"`
}

type appRef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type sessionConfig struct {
	DataMode         dataMode `json:"data_mode,omitempty"`
	ReportIntervalMS int64    `json:"report_interval_ms,omitempty"`
}

type welcome struct {
	Version   int           `json:"version"`
	SessionID string        `json:"session_id"`
	App       appRef        `json:"app"`
	Config    sessionConfig `json:"config"`
	Limits    limits        `json:"limits"`
}

type goodbye struct {
	Reason string `json:"reason"`
}

// --- queries ---

type wireFilter struct {
	And   []wireFilter    `json:"and,omitempty"`
	Or    []wireFilter    `json:"or,omitempty"`
	Not   *wireFilter     `json:"not,omitempty"`
	Field string          `json:"field,omitempty"`
	Op    string          `json:"op,omitempty"`
	Value json.RawMessage `json:"value,omitempty"`
}

type wireSort struct {
	Field string `json:"field"`
	Order string `json:"order,omitempty"`
}

type query struct {
	Filter  *wireFilter `json:"filter,omitempty"`
	Sort    []wireSort  `json:"sort,omitempty"`
	Include []string    `json:"include,omitempty"`
	Limit   int         `json:"limit,omitempty"`
	Cursor  string      `json:"cursor,omitempty"`
}

type page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}

type getRequest struct {
	ID      string   `json:"id"`
	Include []string `json:"include,omitempty"`
}

type countRequest struct {
	Filter *wireFilter `json:"filter,omitempty"`
}

type countResult struct {
	Count int64 `json:"count"`
	Exact bool  `json:"exact"`
}

type stepsRequest struct {
	ActivityID string   `json:"activity_id"`
	Include    []string `json:"include,omitempty"`
	Limit      int      `json:"limit,omitempty"`
	Cursor     string   `json:"cursor,omitempty"`
}

type resultRequest struct {
	ActivityID string `json:"activity_id"`
}

type treeRequest struct {
	ID       string   `json:"id"`
	Include  []string `json:"include,omitempty"`
	MaxNodes int      `json:"max_nodes,omitempty"`
}

type bucket struct {
	Field      string `json:"field"`
	IntervalMS int64  `json:"interval_ms"`
	From       string `json:"from,omitempty"`
	To         string `json:"to,omitempty"`
}

type metric struct {
	Name        string    `json:"name"`
	Field       string    `json:"field,omitempty"`
	Percentiles []float64 `json:"percentiles,omitempty"`
}

type aggregateRequest struct {
	Filter  *wireFilter `json:"filter,omitempty"`
	GroupBy []string    `json:"group_by,omitempty"`
	Bucket  *bucket     `json:"bucket,omitempty"`
	Metrics []metric    `json:"metrics"`
	Limit   int         `json:"limit,omitempty"`
}

type aggregateGroup struct {
	Key       map[string]string             `json:"key,omitempty"`
	Bucket    string                        `json:"bucket,omitempty"`
	Count     *int64                        `json:"count,omitempty"`
	Durations map[string]map[string]float64 `json:"durations,omitempty"`
}

type aggregateResult struct {
	Groups    []aggregateGroup `json:"groups"`
	Truncated bool             `json:"truncated"`
}

// --- resources ---

type errorInfo struct {
	Message string `json:"message,omitempty"`
	Kind    string `json:"kind,omitempty"`
	At      string `json:"at,omitempty"`
}

type result struct {
	State string          `json:"state"` // ok | error
	Data  json.RawMessage `json:"data,omitempty"`
	Error *errorInfo      `json:"error,omitempty"`
}

type wait struct {
	Kind  string `json:"kind"`
	Name  string `json:"name,omitempty"`
	Until string `json:"until,omitempty"`
}

type activityView struct {
	ID             string            `json:"id"`
	Type           string            `json:"type"`
	Queue          string            `json:"queue,omitempty"`
	Status         string            `json:"status"`
	Priority       int               `json:"priority"`
	RootID         string            `json:"root_id,omitempty"`
	ParentID       string            `json:"parent_id,omitempty"`
	Depth          int               `json:"depth"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	Attempt        int               `json:"attempt"`
	MaxAttempts    int               `json:"max_attempts,omitempty"` // absent: unlimited
	CreatedAt      string            `json:"created_at"`
	ScheduledFor   string            `json:"scheduled_for,omitempty"`
	StartedAt      string            `json:"started_at,omitempty"`
	CompletedAt    string            `json:"completed_at,omitempty"`
	UpdatedAt      string            `json:"updated_at,omitempty"`
	TimeoutMS      int64             `json:"timeout_ms,omitempty"`
	LeaseExpiresAt string            `json:"lease_expires_at,omitempty"`
	ExecutorID     string            `json:"executor_id,omitempty"`
	Wait           *wait             `json:"wait,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	LastError      *errorInfo        `json:"last_error,omitempty"`
	Payload        json.RawMessage   `json:"payload,omitempty"`
	Result         *result           `json:"result,omitempty"`
	// Steps and Events are pointers so an included-but-empty list encodes as
	// [] and a list that was not asked for is omitted.
	Steps  *[]stepView  `json:"steps,omitempty"`
	Events *[]eventView `json:"events,omitempty"`
}

type stepView struct {
	ID         string  `json:"id"`
	ActivityID string  `json:"activity_id"`
	Name       string  `json:"name"`
	Kind       string  `json:"kind"`
	Status     string  `json:"status"`
	CreatedAt  string  `json:"created_at"`
	Result     *result `json:"result,omitempty"`
}

type eventView struct {
	ID         string          `json:"id"`
	Cursor     string          `json:"cursor"`
	ActivityID string          `json:"activity_id"`
	Type       string          `json:"type"`
	At         string          `json:"at"`
	ExecutorID string          `json:"executor_id,omitempty"`
	Detail     json.RawMessage `json:"detail,omitempty"`
}

type treeView struct {
	RootID    string         `json:"root_id"`
	Items     []activityView `json:"items"`
	Truncated bool           `json:"truncated"`
}

type runningActivity struct {
	ActivityID string `json:"activity_id"`
	Type       string `json:"type"`
	Attempt    int    `json:"attempt"`
	StartedAt  string `json:"started_at"`
}

type executorState struct {
	ID             string            `json:"id"`
	UptimeMS       int64             `json:"uptime_ms"`
	MaxConcurrency int               `json:"max_concurrency"`
	InFlight       int               `json:"in_flight"`
	Running        []runningActivity `json:"running,omitempty"`
	// ClaimLagMS is how long the latest activity waited, from when it was
	// due, to start here.
	ClaimLagMS int64 `json:"claim_lag_ms"`
	// HeartbeatFailures counts claim renewals that failed.
	HeartbeatFailures uint64            `json:"heartbeat_failures"`
	Draining          bool              `json:"draining"`
	Counters          *executorCounters `json:"counters,omitempty"`
}

type executorCounters struct {
	Claimed      uint64 `json:"claimed"`
	Succeeded    uint64 `json:"succeeded"`
	Retried      uint64 `json:"retried"`
	Failed       uint64 `json:"failed"`
	TimedOut     uint64 `json:"timed_out"`
	DeadLettered uint64 `json:"dead_lettered"`
	ClaimsLost   uint64 `json:"claims_lost"`
}

// --- commands ---

type commandTarget struct {
	IDs            []string    `json:"ids,omitempty"`
	Filter         *wireFilter `json:"filter,omitempty"`
	Max            int         `json:"max,omitempty"`
	IdempotencyKey string      `json:"idempotency_key,omitempty"`
	Type           string      `json:"type,omitempty"`
	Queue          string      `json:"queue,omitempty"`
}

// commandRequest is every command's request; each command uses the fields
// that apply to it and rejects the rest.
type commandRequest struct {
	CommandID     string          `json:"command_id"`
	Target        commandTarget   `json:"target"`
	DryRun        bool            `json:"dry_run,omitempty"`
	Reason        string          `json:"reason,omitempty"`
	Cascade       string          `json:"cascade,omitempty"`
	ResetAttempts bool            `json:"reset_attempts,omitempty"`
	At            string          `json:"at,omitempty"`
	Priority      int             `json:"priority,omitempty"`
	Name          string          `json:"name,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
}

type commandItem struct {
	ID      string     `json:"id"`
	Outcome string     `json:"outcome"`
	Status  string     `json:"status,omitempty"`
	Error   *wireError `json:"error,omitempty"`
}

type commandResult struct {
	Matched  int           `json:"matched"`
	Applied  int           `json:"applied"`
	Cascaded int           `json:"cascaded,omitempty"`
	More     bool          `json:"more"`
	Replayed bool          `json:"replayed,omitempty"`
	Results  []commandItem `json:"results"`
}

// --- streams ---

const (
	typeEventsSubscribe   = "events.subscribe"
	typeEventsUnsubscribe = "events.unsubscribe"
	typeStreamEvents      = "stream.events"
	typeStreamGap         = "stream.gap"
)

type subscribeRequest struct {
	Filter      *wireFilter `json:"filter,omitempty"`
	AfterCursor string      `json:"after_cursor,omitempty"`
	MaxBatch    int         `json:"max_batch,omitempty"`
	MaxDelayMS  int         `json:"max_delay_ms,omitempty"`
}

type subscription struct {
	SubscriptionID string `json:"subscription_id"`
	// Cursor is where the stream starts: the request's after_cursor, or the
	// log's current end.
	Cursor string `json:"cursor,omitempty"`
}

type streamEvents struct {
	SubscriptionID string      `json:"subscription_id"`
	Items          []eventView `json:"items"`
	Cursor         string      `json:"cursor"`
}

type streamGap struct {
	SubscriptionID string `json:"subscription_id"`
	SinceCursor    string `json:"since_cursor"`
}
