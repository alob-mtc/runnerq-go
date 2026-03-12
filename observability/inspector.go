package observability

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
)

// QueueInspector provides read-only access to queue state for monitoring.
type QueueInspector struct {
	maxWorkers *int
	backend    storage.InspectionStorage
}

// NewQueueInspector creates a new inspector from an InspectionStorage backend.
func NewQueueInspector(backend storage.InspectionStorage) *QueueInspector {
	return &QueueInspector{backend: backend}
}

// WithMaxWorkers sets the max workers count for stats reporting.
func (q *QueueInspector) WithMaxWorkers(max int) *QueueInspector {
	q.maxWorkers = &max
	return q
}

// EventStream returns a channel of real-time activity events.
func (q *QueueInspector) EventStream(ctx context.Context) (<-chan ActivityEvent, error) {
	backendCh, err := q.backend.EventStream(ctx)
	if err != nil {
		return nil, err
	}

	ch := make(chan ActivityEvent, 100)
	go func() {
		defer close(ch)
		for evt := range backendCh {
			ch <- convertBackendEvent(evt)
		}
	}()

	return ch, nil
}

// ListPending returns pending activities.
func (q *QueueInspector) ListPending(ctx context.Context, offset, limit int) ([]ActivitySnapshot, error) {
	snapshots, err := q.backend.ListPending(ctx, offset, limit)
	if err != nil {
		return nil, err
	}
	return convertSnapshots(snapshots), nil
}

// ListProcessing returns activities currently being processed.
func (q *QueueInspector) ListProcessing(ctx context.Context, offset, limit int) ([]ActivitySnapshot, error) {
	snapshots, err := q.backend.ListProcessing(ctx, offset, limit)
	if err != nil {
		return nil, err
	}
	return convertSnapshots(snapshots), nil
}

// ListScheduled returns scheduled activities.
func (q *QueueInspector) ListScheduled(ctx context.Context, offset, limit int) ([]ActivitySnapshot, error) {
	snapshots, err := q.backend.ListScheduled(ctx, offset, limit)
	if err != nil {
		return nil, err
	}
	return convertSnapshots(snapshots), nil
}

// ListDeadLetter returns dead letter records.
func (q *QueueInspector) ListDeadLetter(ctx context.Context, offset, limit int) ([]DeadLetterRecord, error) {
	records, err := q.backend.ListDeadLetter(ctx, offset, limit)
	if err != nil {
		return nil, err
	}
	var result []DeadLetterRecord
	for _, r := range records {
		result = append(result, DeadLetterRecord{
			Activity: convertSnapshot(r.Activity),
			Error:    r.Error,
			FailedAt: r.FailedAt,
		})
	}
	return result, nil
}

// ListCompleted returns completed activities.
func (q *QueueInspector) ListCompleted(ctx context.Context, offset, limit int) ([]ActivitySnapshot, error) {
	snapshots, err := q.backend.ListCompleted(ctx, offset, limit)
	if err != nil {
		return nil, err
	}
	return convertSnapshots(snapshots), nil
}

// GetActivity returns a specific activity by ID.
func (q *QueueInspector) GetActivity(ctx context.Context, activityID uuid.UUID) (*ActivitySnapshot, error) {
	s, err := q.backend.GetActivity(ctx, activityID)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, nil
	}
	converted := convertSnapshot(*s)
	return &converted, nil
}

// GetResult returns the result of a specific activity.
func (q *QueueInspector) GetResult(ctx context.Context, activityID uuid.UUID) (json.RawMessage, error) {
	r, err := q.backend.GetResult(ctx, activityID)
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, nil
	}
	return r.Data, nil
}

// RecentEvents returns recent events for a specific activity.
func (q *QueueInspector) RecentEvents(ctx context.Context, activityID uuid.UUID, limit int) ([]ActivityEvent, error) {
	events, err := q.backend.GetActivityEvents(ctx, activityID, limit)
	if err != nil {
		return nil, err
	}
	var result []ActivityEvent
	for _, e := range events {
		result = append(result, convertBackendEvent(e))
	}
	return result, nil
}

// Stats returns queue statistics.
func (q *QueueInspector) Stats(ctx context.Context) (*QueueStats, error) {
	backendStats, err := q.backend.Stats(ctx)
	if err != nil {
		return nil, err
	}

	maxWorkers := q.maxWorkers
	if maxWorkers == nil {
		maxWorkers = backendStats.MaxWorkers
	}

	return &QueueStats{
		PendingActivities:    backendStats.Pending,
		ProcessingActivities: backendStats.Processing,
		CriticalPriority:     backendStats.ByPriority.Critical,
		HighPriority:         backendStats.ByPriority.High,
		NormalPriority:       backendStats.ByPriority.Normal,
		LowPriority:          backendStats.ByPriority.Low,
		ScheduledActivities:  backendStats.Scheduled,
		DeadLetterActivities: backendStats.DeadLetter,
		MaxWorkers:           maxWorkers,
	}, nil
}

func convertSnapshot(s storage.ActivitySnapshot) ActivitySnapshot {
	return ActivitySnapshot{
		ID:                s.ID,
		ActivityType:      s.ActivityType,
		Payload:           s.Payload,
		Priority:          s.Priority,
		Status:            s.Status,
		CreatedAt:         s.CreatedAt,
		ScheduledAt:       s.ScheduledAt,
		StartedAt:         s.StartedAt,
		CompletedAt:       s.CompletedAt,
		CurrentWorkerID:   s.CurrentWorkerID,
		LastWorkerID:      s.LastWorkerID,
		RetryCount:        s.RetryCount,
		MaxRetries:        s.MaxRetries,
		TimeoutSeconds:    s.TimeoutSeconds,
		RetryDelaySeconds: s.RetryDelaySeconds,
		Metadata:          s.Metadata,
		LastError:         s.LastError,
		LastErrorAt:       s.LastErrorAt,
		StatusUpdatedAt:   s.StatusUpdatedAt,
		IdempotencyKey:    s.IdempotencyKey,
	}
}

func convertSnapshots(ss []storage.ActivitySnapshot) []ActivitySnapshot {
	result := make([]ActivitySnapshot, 0, len(ss))
	for _, s := range ss {
		result = append(result, convertSnapshot(s))
	}
	return result
}

func convertBackendEvent(e storage.ActivityEvent) ActivityEvent {
	return ActivityEvent{
		ActivityID: e.ActivityID,
		Timestamp:  e.Timestamp,
		EventType:  ActivityEventType(e.EventType),
		WorkerID:   e.WorkerID,
		Detail:     e.Detail,
	}
}
