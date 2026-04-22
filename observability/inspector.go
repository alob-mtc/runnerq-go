package observability

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
)

// QueueInspector provides read-only access to queue state for monitoring.
type QueueInspector struct {
	maxWorkers *int
	backend    storage.InspectionStorage
	hub        notifyHub
}

// NewQueueInspector creates a new inspector from an InspectionStorage backend.
func NewQueueInspector(backend storage.InspectionStorage) *QueueInspector {
	qi := &QueueInspector{backend: backend}
	qi.hub.backend = backend
	return qi
}

// WithMaxWorkers sets the max workers count for stats reporting.
func (q *QueueInspector) WithMaxWorkers(max int) *QueueInspector {
	q.maxWorkers = &max
	return q
}

// EventStream returns a channel of real-time activity events.
// Deprecated: Use SubscribeEvents for fan-out multiplexed access.
func (q *QueueInspector) EventStream(ctx context.Context) (<-chan ActivityEvent, error) {
	return q.SubscribeEvents(ctx)
}

// SubscribeEvents returns a channel of real-time activity events backed by a
// shared fan-out hub. Only one backend connection is used regardless of how
// many subscribers are active. The returned channel is closed when ctx is
// cancelled or the hub shuts down.
func (q *QueueInspector) SubscribeEvents(ctx context.Context) (<-chan ActivityEvent, error) {
	return q.hub.subscribe(ctx)
}

// ---------------------------------------------------------------------------
// notifyHub — single-connection fan-out broadcaster
// ---------------------------------------------------------------------------

type subscriber struct {
	ch  chan ActivityEvent
	ctx context.Context
}

type notifyHub struct {
	backend storage.InspectionStorage

	mu          sync.Mutex
	subscribers map[uint64]subscriber
	nextID      uint64
	running     atomic.Bool
	cancel      context.CancelFunc
}

func (h *notifyHub) subscribe(ctx context.Context) (<-chan ActivityEvent, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.subscribers == nil {
		h.subscribers = make(map[uint64]subscriber)
	}

	id := h.nextID
	h.nextID++
	ch := make(chan ActivityEvent, 100)
	h.subscribers[id] = subscriber{ch: ch, ctx: ctx}

	// Auto-unsubscribe when context cancels.
	go func() {
		<-ctx.Done()
		h.unsubscribe(id)
	}()

	// Start the shared listener if not already running.
	if !h.running.Load() {
		if err := h.startLocked(); err != nil {
			delete(h.subscribers, id)
			close(ch)
			return nil, err
		}
	}

	return ch, nil
}

func (h *notifyHub) unsubscribe(id uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	sub, ok := h.subscribers[id]
	if !ok {
		return
	}
	close(sub.ch)
	delete(h.subscribers, id)

	// Stop the listener when no subscribers remain.
	if len(h.subscribers) == 0 && h.cancel != nil {
		h.cancel()
		h.cancel = nil
	}
}

// startLocked starts the backend listener goroutine. Caller must hold h.mu.
func (h *notifyHub) startLocked() error {
	ctx, cancel := context.WithCancel(context.Background())
	backendCh, err := h.backend.EventStream(ctx)
	if err != nil {
		cancel()
		return err
	}

	h.cancel = cancel
	h.running.Store(true)

	go h.broadcast(ctx, backendCh)
	return nil
}

func (h *notifyHub) broadcast(ctx context.Context, backendCh <-chan storage.ActivityEvent) {
	defer h.running.Store(false)
	defer h.closeAll()

	for {
		select {
		case evt, ok := <-backendCh:
			if !ok {
				return
			}
			converted := convertBackendEvent(evt)
			h.mu.Lock()
			for id, sub := range h.subscribers {
				// Skip subscribers whose context has already cancelled.
				if sub.ctx.Err() != nil {
					continue
				}
				select {
				case sub.ch <- converted:
				default:
					slog.Debug("Dropping event for slow subscriber", "subscriber_id", id)
				}
			}
			h.mu.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

// closeAll closes every subscriber channel — used when the backend stream ends.
func (h *notifyHub) closeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, sub := range h.subscribers {
		close(sub.ch)
		delete(h.subscribers, id)
	}
	if h.cancel != nil {
		h.cancel()
		h.cancel = nil
	}
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
	snapshots, err := q.backend.ListCompletedNonCron(ctx, offset, limit)
	if err != nil {
		return nil, err
	}
	return convertSnapshots(snapshots), nil
}

// ListCronCompleted returns cron-tagged completed activities.
func (q *QueueInspector) ListCronCompleted(ctx context.Context, offset, limit int) ([]ActivitySnapshot, error) {
	snapshots, err := q.backend.ListCompletedCron(ctx, offset, limit)
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
