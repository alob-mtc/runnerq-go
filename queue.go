package runnerq

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
)

// ---------------------------------------------------------------------------
// Internal queue interface
// ---------------------------------------------------------------------------

// ResultState indicates whether an activity result is success or failure.
type ResultState int

const (
	ResultOk ResultState = iota
	ResultErr
)

// activityResult holds the result data from a completed activity.
type activityResult struct {
	Data  json.RawMessage `json:"data,omitempty"`
	State ResultState     `json:"state"`
}

// activityQueue is the internal interface used by the worker engine for queue operations.
// Custom backends should implement the storage.Storage interface instead.
type activityQueue interface {
	Enqueue(ctx context.Context, a *activity) error
	Dequeue(ctx context.Context, timeout time.Duration, workerID string) (*activity, error)
	MarkCompleted(ctx context.Context, a *activity, workerID string) error
	// MarkFailed marks an activity as failed. Returns true if moved to dead letter queue.
	MarkFailed(ctx context.Context, a *activity, errorMessage string, retryable bool, workerID string) (bool, error)
	ScheduleActivity(ctx context.Context, a *activity) error
	ProcessScheduledActivities(ctx context.Context) ([]*activity, error)
	RequeueExpired(ctx context.Context, maxToProcess int) (uint64, error)
	EvaluateIdempotencyRule(ctx context.Context, a *activity) (*uuid.UUID, error)
	ExtendLease(ctx context.Context, activityID uuid.UUID, extendBy time.Duration) (bool, error)
	StoreResult(ctx context.Context, activityID uuid.UUID, result activityResult) error
	GetResult(ctx context.Context, activityID uuid.UUID) (*activityResult, error)
	SchedulesNatively() bool
}

// ---------------------------------------------------------------------------
// Backend adapter (bridges storage.Storage → activityQueue)
// ---------------------------------------------------------------------------

// backendQueueAdapter wraps a storage.Storage to provide the activityQueue interface.
type backendQueueAdapter struct {
	backend       storage.Storage
	activityTypes []string
}

func newBackendQueueAdapter(backend storage.Storage, activityTypes []string) *backendQueueAdapter {
	return &backendQueueAdapter{
		backend:       backend,
		activityTypes: activityTypes,
	}
}

func activityToQueued(a *activity) storage.QueuedActivity {
	var idempKey *storage.IdempotencyKeyConfig
	if a.IdempotencyKey != nil {
		idempKey = &storage.IdempotencyKeyConfig{
			Key:      a.IdempotencyKey.Key,
			Behavior: onDuplicateToStorageBehavior(a.IdempotencyKey.Behavior),
		}
	}
	return storage.QueuedActivity{
		ID:                a.ID,
		ActivityType:      a.ActivityType,
		Payload:           a.Payload,
		Priority:          storage.ActivityPriority(a.Priority),
		MaxRetries:        a.MaxRetries,
		RetryCount:        a.RetryCount,
		TimeoutSeconds:    a.TimeoutSeconds,
		RetryDelaySeconds: a.RetryDelaySeconds,
		ScheduledAt:       a.ScheduledAt,
		Metadata:          a.Metadata,
		IdempotencyKey:    idempKey,
		CreatedAt:         a.CreatedAt,
	}
}

func queuedToActivity(q *storage.QueuedActivity) *activity {
	var idempKey *IdempotencyConfig
	if q.IdempotencyKey != nil {
		idempKey = &IdempotencyConfig{
			Key:      q.IdempotencyKey.Key,
			Behavior: storageBehaviorToOnDuplicate(q.IdempotencyKey.Behavior),
		}
	}
	return &activity{
		ID:                q.ID,
		ActivityType:      q.ActivityType,
		Payload:           q.Payload,
		Priority:          ActivityPriority(q.Priority),
		Status:            StatusPending,
		CreatedAt:         q.CreatedAt,
		ScheduledAt:       q.ScheduledAt,
		RetryCount:        q.RetryCount,
		MaxRetries:        q.MaxRetries,
		TimeoutSeconds:    q.TimeoutSeconds,
		RetryDelaySeconds: q.RetryDelaySeconds,
		Metadata:          q.Metadata,
		IdempotencyKey:    idempKey,
	}
}

func onDuplicateToStorageBehavior(od OnDuplicate) storage.IdempotencyBehavior {
	switch od {
	case AllowReuse:
		return storage.BehaviorAllowReuse
	case ReturnExisting:
		return storage.BehaviorReturnExisting
	case AllowReuseOnFailure:
		return storage.BehaviorAllowReuseOnFailure
	case NoReuse:
		return storage.BehaviorNoReuse
	default:
		return storage.BehaviorAllowReuse
	}
}

func storageBehaviorToOnDuplicate(b storage.IdempotencyBehavior) OnDuplicate {
	switch b {
	case storage.BehaviorAllowReuse:
		return AllowReuse
	case storage.BehaviorReturnExisting:
		return ReturnExisting
	case storage.BehaviorAllowReuseOnFailure:
		return AllowReuseOnFailure
	case storage.BehaviorNoReuse:
		return NoReuse
	default:
		return AllowReuse
	}
}

func (a *backendQueueAdapter) Enqueue(ctx context.Context, act *activity) error {
	queued := activityToQueued(act)
	return a.backend.Enqueue(ctx, queued)
}

func (a *backendQueueAdapter) Dequeue(ctx context.Context, timeout time.Duration, workerID string) (*activity, error) {
	var types []string
	if len(a.activityTypes) > 0 {
		types = a.activityTypes
	}
	q, err := a.backend.Dequeue(ctx, workerID, timeout, types)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, nil
	}
	return queuedToActivity(q), nil
}

func (a *backendQueueAdapter) MarkCompleted(ctx context.Context, act *activity, workerID string) error {
	return a.backend.AckSuccess(ctx, act.ID, nil, workerID)
}

func (a *backendQueueAdapter) MarkFailed(ctx context.Context, act *activity, errorMessage string, retryable bool, workerID string) (bool, error) {
	var failure storage.FailureKind
	if retryable {
		failure = storage.NewRetryableFailure(errorMessage)
	} else {
		failure = storage.NewNonRetryableFailure(errorMessage)
	}
	return a.backend.AckFailure(ctx, act.ID, failure, workerID)
}

func (a *backendQueueAdapter) ScheduleActivity(ctx context.Context, act *activity) error {
	queued := activityToQueued(act)
	if queued.ScheduledAt == nil {
		now := time.Now().UTC()
		queued.ScheduledAt = &now
	}
	return a.backend.Enqueue(ctx, queued)
}

func (a *backendQueueAdapter) ProcessScheduledActivities(ctx context.Context) ([]*activity, error) {
	_, err := a.backend.ProcessScheduled(ctx)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (a *backendQueueAdapter) RequeueExpired(ctx context.Context, maxToProcess int) (uint64, error) {
	return a.backend.RequeueExpired(ctx, maxToProcess)
}

func (a *backendQueueAdapter) EvaluateIdempotencyRule(ctx context.Context, act *activity) (*uuid.UUID, error) {
	queued := activityToQueued(act)
	return a.backend.CheckIdempotency(ctx, &queued)
}

func (a *backendQueueAdapter) ExtendLease(ctx context.Context, activityID uuid.UUID, extendBy time.Duration) (bool, error) {
	return a.backend.ExtendLease(ctx, activityID, extendBy)
}

func (a *backendQueueAdapter) StoreResult(ctx context.Context, activityID uuid.UUID, result activityResult) error {
	backendResult := storage.ActivityResult{
		Data:  result.Data,
		State: storage.ResultState(result.State),
	}
	return a.backend.StoreResult(ctx, activityID, backendResult)
}

func (a *backendQueueAdapter) GetResult(ctx context.Context, activityID uuid.UUID) (*activityResult, error) {
	backendResult, err := a.backend.GetResult(ctx, activityID)
	if err != nil {
		return nil, err
	}
	if backendResult == nil {
		return nil, nil
	}

	var data json.RawMessage
	if backendResult.Data != nil {
		data = backendResult.Data
	}

	return &activityResult{
		Data:  data,
		State: ResultState(backendResult.State),
	}, nil
}

func (a *backendQueueAdapter) SchedulesNatively() bool {
	return a.backend.SchedulesNatively()
}
