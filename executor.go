package runnerq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
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

// GetResult waits for and returns the completed activity result.
// It polls every 100ms. The caller should use context for timeout control.
func (f *ActivityFuture) GetResult(ctx context.Context) (json.RawMessage, error) {
	for {
		result, err := f.queue.GetResult(ctx, f.activityID)
		if err != nil {
			return nil, err
		}
		if result != nil {
			switch result.State {
			case ResultOk:
				return result.Data, nil
			case ResultErr:
				resultJSON, _ := json.Marshal(result.Data)
				return nil, &WorkerError{Kind: ErrCustom, Message: string(resultJSON)}
			}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
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
	delay          *time.Duration
	idempotencyKey *IdempotencyConfig
	metadata       map[string]string
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

// Delay sets the delay before execution.
func (b *ActivityBuilder) Delay(d time.Duration) *ActivityBuilder {
	b.delay = &d
	return b
}

// IdempotencyKeyOption sets the idempotency key and behavior for duplicate detection.
func (b *ActivityBuilder) IdempotencyKeyOption(key string, behavior OnDuplicate) *ActivityBuilder {
	b.idempotencyKey = &IdempotencyConfig{Key: key, Behavior: behavior}
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

// MetadataMap sets multiple metadata fields on the activity.
// Values are merged with existing metadata and overwrite duplicate keys.
func (b *ActivityBuilder) MetadataMap(metadata map[string]string) *ActivityBuilder {
	if len(metadata) == 0 {
		return b
	}
	if b.metadata == nil {
		b.metadata = make(map[string]string, len(metadata))
	}
	for k, v := range metadata {
		b.metadata[k] = v
	}
	return b
}

// Execute enqueues the activity and returns an ActivityFuture.
func (b *ActivityBuilder) Execute(ctx context.Context) (*ActivityFuture, error) {
	if b.payload == nil {
		return nil, &WorkerError{Kind: ErrQueue, Message: "Activity payload is required"}
	}

	hasOption := b.priority != nil || b.maxRetries != nil || b.timeout != nil || b.delay != nil || b.idempotencyKey != nil || len(b.metadata) > 0

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
		var delaySec *uint64
		if b.delay != nil {
			d := uint64(b.delay.Seconds())
			delaySec = &d
		}
		var idempKey *IdempotencyConfig
		if b.idempotencyKey != nil {
			prefixedKey := fmt.Sprintf("%s-%s", b.idempotencyKey.Key, b.activityType)
			idempKey = &IdempotencyConfig{Key: prefixedKey, Behavior: b.idempotencyKey.Behavior}
		}
		metadata := make(map[string]string)
		for k, v := range b.metadata {
			metadata[k] = v
		}
		option = &ActivityOption{
			Priority:       b.priority,
			MaxRetries:     maxRetries,
			TimeoutSeconds: timeoutSec,
			DelaySeconds:   delaySec,
			IdempotencyKey: idempKey,
			Metadata:       metadata,
		}
	}

	return b.wrapper.executeActivity(ctx, b.activityType, b.payload, option)
}

// WorkerEngineWrapper provides activity execution capabilities.
// It implements ActivityExecutor.
type WorkerEngineWrapper struct {
	queue activityQueue
}

func newWorkerEngineWrapper(queue activityQueue) *WorkerEngineWrapper {
	return &WorkerEngineWrapper{queue: queue}
}

// Activity creates a fluent activity builder.
func (w *WorkerEngineWrapper) Activity(activityType string) *ActivityBuilder {
	return &ActivityBuilder{
		wrapper:      w,
		activityType: activityType,
	}
}

func (w *WorkerEngineWrapper) executeActivity(ctx context.Context, activityType string, payload json.RawMessage, option *ActivityOption) (*ActivityFuture, error) {
	a := newActivity(activityType, payload, option)
	activityID := a.ID

	existingID, err := w.queue.EvaluateIdempotencyRule(ctx, a)
	if err != nil {
		return nil, WorkerErrorFromStorage(err)
	}
	if existingID != nil {
		return &ActivityFuture{queue: w.queue, activityID: *existingID}, nil
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
