package runnerq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/alob-mtc/runnerq-go/storage"
)

// backoff implements simple exponential backoff for idle polls.
type backoff struct {
	current time.Duration
	base    time.Duration
	max     time.Duration
}

func newBackoff(base, max time.Duration) *backoff {
	return &backoff{current: base, base: base, max: max}
}

func (b *backoff) reset() {
	b.current = b.base
}

func (b *backoff) next() time.Duration {
	n := b.current
	b.current = min(b.current*2, b.max)
	return n
}

// WorkerEngine is the main activity processing engine.
type WorkerEngine struct {
	queue      activityQueue
	backend    storage.Storage
	handlers   map[string]ActivityHandler
	config     WorkerConfig
	running    atomic.Bool
	cancelFunc context.CancelFunc
	shutdownCh chan struct{}
	metrics    MetricsSink
	resultWg   sync.WaitGroup // tracks in-flight result storage goroutines
}

// NewWorkerEngineWithBackend creates a WorkerEngine from a custom backend.
func NewWorkerEngineWithBackend(backend storage.Storage, config WorkerConfig) *WorkerEngine {
	// Propagate lease config to backends that support it.
	if lc, ok := backend.(storage.LeaseConfigurer); ok && config.LeaseMS != nil {
		lc.SetLeaseMS(int64(*config.LeaseMS))
	}

	adapter := newBackendQueueAdapter(backend, config.ActivityTypes)
	shutdownCh := make(chan struct{})
	return &WorkerEngine{
		queue:      adapter,
		backend:    backend,
		handlers:   make(map[string]ActivityHandler),
		config:     config,
		shutdownCh: shutdownCh,
		metrics:    NoopMetrics{},
	}
}

// SetMetrics sets the metrics sink.
func (e *WorkerEngine) SetMetrics(sink MetricsSink) {
	e.metrics = sink
}

// Inspector returns a QueueInspector for observability operations.
// Import the observability package and use NewQueueInspector(backend) instead
// for decoupled usage.
func (e *WorkerEngine) Backend() storage.Storage {
	return e.backend
}

// MaxConcurrentActivities returns the max workers config for inspector use.
func (e *WorkerEngine) MaxConcurrentActivities() int {
	return e.config.MaxConcurrentActivities
}

// RegisterActivity registers an activity handler for a given activity type.
func (e *WorkerEngine) RegisterActivity(activityType string, handler ActivityHandler) {
	e.handlers[activityType] = handler
}

// GetActivityExecutor returns an ActivityExecutor for orchestrating activities.
func (e *WorkerEngine) GetActivityExecutor() ActivityExecutor {
	return newWorkerEngineWrapper(e.queue)
}

// Start starts the worker engine and blocks until shutdown or error.
func (e *WorkerEngine) Start(ctx context.Context) error {
	if !e.running.CompareAndSwap(false, true) {
		return &WorkerError{Kind: ErrAlreadyRunning}
	}

	if len(e.config.ActivityTypes) > 0 {
		var missing []string
		for _, t := range e.config.ActivityTypes {
			if _, ok := e.handlers[t]; !ok {
				missing = append(missing, t)
			}
		}
		if len(missing) > 0 {
			panic(fmt.Sprintf("activity_types filter contains types with no registered handler: %v", missing))
		}
	}

	slog.Info("Starting worker engine", "max_concurrent_activities", e.config.MaxConcurrentActivities)

	engineCtx, cancel := context.WithCancel(ctx)
	e.cancelFunc = cancel
	e.shutdownCh = make(chan struct{})

	var wg sync.WaitGroup

	// Scheduled activities processor (skipped if backend handles it natively)
	if !e.queue.SchedulesNatively() {
		wg.Go(func() {
			e.runScheduledProcessor(engineCtx)
		})
	}

	// Reaper processor
	wg.Go(func() {
		e.runReaperProcessor(engineCtx)
	})

	// Worker loops
	for i := 0; i < e.config.MaxConcurrentActivities; i++ {
		workerID := i
		wg.Go(func() {
			e.runWorkerLoop(engineCtx, workerID)
		})
	}

	// Wait for shutdown signal or context cancellation
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		slog.Info("Received shutdown signal", "signal", sig)
	case <-ctx.Done():
		slog.Info("Context cancelled")
	case <-e.shutdownCh:
		slog.Info("Shutdown requested")
	}

	e.stop()
	cancel()
	wg.Wait()

	// Wait for any in-flight result storage goroutines to finish
	e.resultWg.Wait()

	signal.Stop(sigCh)
	slog.Info("Worker engine stopped")
	return nil
}

// Stop initiates a graceful shutdown of the worker engine.
func (e *WorkerEngine) Stop() {
	e.stop()
}

func (e *WorkerEngine) stop() {
	slog.Info("Stopping worker engine")
	e.running.Store(false)
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
	select {
	case <-e.shutdownCh:
	default:
		close(e.shutdownCh)
	}
}

func (e *WorkerEngine) runWorkerLoop(ctx context.Context, workerID int) {
	slog.Debug("Starting worker loop", "worker_id", workerID)
	workerLabel := fmt.Sprintf("worker-%d", workerID)
	bo := newBackoff(100*time.Millisecond, 5*time.Second)

	for e.running.Load() {
		select {
		case <-ctx.Done():
			slog.Debug("Worker loop stopped (context)", "worker_id", workerID)
			return
		default:
		}

		act, err := e.queue.Dequeue(ctx, time.Second, workerLabel)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("Failed to dequeue activity", "worker_id", workerID, "error", err)
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return
			}
			continue
		}

		if act == nil {
			sleepFor := bo.next()
			select {
			case <-time.After(sleepFor):
			case <-ctx.Done():
				return
			}
			continue
		}

		bo.reset()
		e.processActivity(ctx, act, workerLabel, workerID)
	}

	slog.Debug("Worker loop stopped", "worker_id", workerID)
}

func (e *WorkerEngine) processActivity(ctx context.Context, act *activity, workerLabel string, workerID int) {
	activityID := act.ID
	activityType := act.ActivityType

	slog.Debug("Worker processing activity", "worker_id", workerID, "activity_id", activityID, "activity_type", activityType)

	handler, ok := e.handlers[activityType]
	if !ok {
		slog.Error("No handler found for activity type", "worker_id", workerID, "activity_id", activityID, "activity_type", activityType)
		if _, err := e.queue.MarkFailed(ctx, act, "handler_not_found", false, workerLabel); err != nil {
			slog.Error("Failed to mark activity as failed", "worker_id", workerID, "activity_id", activityID, "error", err)
		}
		return
	}

	activityTimeout := time.Duration(act.TimeoutSeconds) * time.Second
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, activityTimeout)
	defer timeoutCancel()

	actCtx := ActivityContext{
		ActivityID:       activityID,
		ActivityType:     activityType,
		RetryCount:       act.RetryCount,
		Metadata:         act.Metadata,
		Ctx:              timeoutCtx,
		ActivityExecutor: newWorkerEngineWrapper(e.queue),
	}

	payloadForDL := make(json.RawMessage, len(act.Payload))
	copy(payloadForDL, act.Payload)

	result, handlerErr := e.safeHandle(handler, actCtx, act.Payload)

	// Check if we timed out
	if timeoutCtx.Err() == context.DeadlineExceeded {
		e.handleTimeout(ctx, act, handler, actCtx, payloadForDL, workerLabel, workerID, activityID, activityType, activityTimeout)
		return
	}

	if handlerErr == nil {
		e.handleSuccess(ctx, act, result, workerLabel, workerID, activityID, activityType)
		return
	}

	retryable := true // default: unknown errors are retryable
	if re, ok := handlerErr.(RetryableError); ok {
		retryable = re.IsRetryable()
	}

	if retryable {
		e.handleRetryableFailure(ctx, act, handler, actCtx, payloadForDL, handlerErr.Error(), workerLabel, workerID, activityID, activityType)
	} else {
		e.handleNonRetryableFailure(ctx, act, handlerErr.Error(), workerLabel, workerID, activityID, activityType)
	}
}

// safeHandle calls the handler with panic recovery.
func (e *WorkerEngine) safeHandle(handler ActivityHandler, ctx ActivityContext, payload json.RawMessage) (result json.RawMessage, err error) {
	defer func() {
		if r := recover(); r != nil {
			var errMsg string
			switch v := r.(type) {
			case string:
				errMsg = fmt.Sprintf("panic: %s", v)
			case error:
				errMsg = fmt.Sprintf("panic: %s", v.Error())
			default:
				errMsg = "panic (unknown)"
			}
			err = NewRetryError(errMsg)
		}
	}()
	return handler.Handle(ctx, payload)
}

func (e *WorkerEngine) handleSuccess(ctx context.Context, act *activity, result json.RawMessage, workerLabel string, workerID int, activityID any, activityType string) {
	e.metrics.IncCounter("activity_completed", 1)
	if err := e.queue.MarkCompleted(ctx, act, workerLabel); err != nil {
		slog.Error("Failed to mark activity as completed", "worker_id", workerID, "activity_id", activityID, "error", err)
	}
	slog.Info("Activity completed successfully", "worker_id", workerID, "activity_id", activityID, "activity_type", activityType)

	// Async result storage — tracked for graceful shutdown
	e.resultWg.Add(1)
	go func() {
		defer e.resultWg.Done()
		storeCtx, storeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer storeCancel()
		res := activityResult{Data: result, State: ResultOk}
		if err := e.queue.StoreResult(storeCtx, act.ID, res); err != nil {
			slog.Error("Failed to store activity result", "activity_id", activityID, "error", err)
		}
	}()
}

func (e *WorkerEngine) handleRetryableFailure(ctx context.Context, act *activity, handler ActivityHandler, actCtx ActivityContext, payloadForDL json.RawMessage, reason string, workerLabel string, workerID int, activityID any, activityType string) {
	e.metrics.IncCounter("activity_retry", 1)
	slog.Warn("Activity requesting retry", "worker_id", workerID, "activity_id", activityID, "activity_type", activityType, "reason", reason)

	deadLettered, err := e.queue.MarkFailed(ctx, act, reason, true, workerLabel)
	if err != nil {
		slog.Error("Failed to mark activity for retry", "worker_id", workerID, "activity_id", activityID, "error", err)
		return
	}
	if deadLettered {
		dlCtx := ActivityContext{
			ActivityID:       act.ID,
			ActivityType:     activityType,
			RetryCount:       0,
			Metadata:         make(map[string]string),
			Ctx:              ctx,
			ActivityExecutor: newWorkerEngineWrapper(e.queue),
		}
		handler.OnDeadLetter(dlCtx, payloadForDL, reason)
	}
}

func (e *WorkerEngine) handleNonRetryableFailure(ctx context.Context, act *activity, reason string, workerLabel string, workerID int, activityID any, activityType string) {
	e.metrics.IncCounter("activity_failed_non_retry", 1)
	slog.Error("Activity failed", "worker_id", workerID, "activity_id", activityID, "activity_type", activityType, "reason", reason)

	if _, err := e.queue.MarkFailed(ctx, act, reason, false, workerLabel); err != nil {
		slog.Error("Failed to mark activity as failed", "worker_id", workerID, "activity_id", activityID, "error", err)
	}

	// Async result storage — tracked for graceful shutdown
	e.resultWg.Add(1)
	go func() {
		defer e.resultWg.Done()
		storeCtx, storeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer storeCancel()
		errorResult, _ := json.Marshal(map[string]any{
			"error":     reason,
			"type":      "non_retryable",
			"failed_at": time.Now().UTC().Format(time.RFC3339),
		})
		res := activityResult{Data: errorResult, State: ResultErr}
		if err := e.queue.StoreResult(storeCtx, act.ID, res); err != nil {
			slog.Error("Failed to store activity result", "activity_id", activityID, "error", err)
		}
	}()
}

func (e *WorkerEngine) handleTimeout(ctx context.Context, act *activity, handler ActivityHandler, actCtx ActivityContext, payloadForDL json.RawMessage, workerLabel string, workerID int, activityID any, activityType string, timeout time.Duration) {
	e.metrics.IncCounter("activity_timeout", 1)
	errorMsg := "Activity execution timed out"
	slog.Error("Activity timed out", "worker_id", workerID, "activity_id", activityID, "activity_type", activityType, "timeout", timeout)

	deadLettered, err := e.queue.MarkFailed(ctx, act, errorMsg, true, workerLabel)
	if err != nil {
		slog.Error("Failed to mark activity as failed", "worker_id", workerID, "activity_id", activityID, "error", err)
		return
	}
	if deadLettered {
		dlCtx := ActivityContext{
			ActivityID:       act.ID,
			ActivityType:     activityType,
			RetryCount:       0,
			Metadata:         make(map[string]string),
			Ctx:              ctx,
			ActivityExecutor: newWorkerEngineWrapper(e.queue),
		}
		handler.OnDeadLetter(dlCtx, payloadForDL, errorMsg)
	}
}

func (e *WorkerEngine) runScheduledProcessor(ctx context.Context) {
	pollInterval := uint64(5)
	if e.config.SchedulePollIntervalSeconds != nil {
		pollInterval = max(*e.config.SchedulePollIntervalSeconds, 1)
	}
	ticker := time.NewTicker(time.Duration(pollInterval) * time.Second)
	defer ticker.Stop()

	slog.Debug("Starting scheduled activities processor")
	for e.running.Load() {
		select {
		case <-ctx.Done():
			slog.Debug("Scheduled activities processor stopped")
			return
		case <-ticker.C:
			if _, err := e.queue.ProcessScheduledActivities(ctx); err != nil {
				slog.Error("Failed to process scheduled activities", "error", err)
			}
		}
	}
	slog.Debug("Scheduled activities processor stopped")
}

func (e *WorkerEngine) runReaperProcessor(ctx context.Context) {
	intervalSec := uint64(5)
	if e.config.ReaperIntervalSeconds != nil {
		intervalSec = max(*e.config.ReaperIntervalSeconds, 1)
	}
	batchSize := 100
	if e.config.ReaperBatchSize != nil {
		batchSize = *e.config.ReaperBatchSize
	}

	ticker := time.NewTicker(time.Duration(intervalSec) * time.Second)
	defer ticker.Stop()

	slog.Debug("Starting reaper processor")
	for e.running.Load() {
		select {
		case <-ctx.Done():
			slog.Debug("Reaper processor stopped")
			return
		case <-ticker.C:
			if _, err := e.queue.RequeueExpired(ctx, batchSize); err != nil {
				slog.Error("Reaper failed to requeue expired items", "error", err)
			}
		}
	}
	slog.Debug("Reaper processor stopped")
}

// WorkerEngineBuilder provides fluent configuration for WorkerEngine.
type WorkerEngineBuilder struct {
	queueName     *string
	maxWorkers    *int
	pollInterval  *time.Duration
	metrics       MetricsSink
	backend       storage.Storage
	activityTypes []string
}

// Builder creates a new WorkerEngineBuilder.
func Builder() *WorkerEngineBuilder {
	return &WorkerEngineBuilder{}
}

// QueueName sets the queue name.
func (b *WorkerEngineBuilder) QueueName(name string) *WorkerEngineBuilder {
	b.queueName = &name
	return b
}

// MaxWorkers sets the maximum concurrent workers.
func (b *WorkerEngineBuilder) MaxWorkers(max int) *WorkerEngineBuilder {
	b.maxWorkers = &max
	return b
}

// SchedulePollInterval sets the interval for polling scheduled activities.
func (b *WorkerEngineBuilder) SchedulePollInterval(interval time.Duration) *WorkerEngineBuilder {
	b.pollInterval = &interval
	return b
}

// ActivityTypes restricts this engine to only dequeue the specified types.
func (b *WorkerEngineBuilder) ActivityTypes(types []string) *WorkerEngineBuilder {
	b.activityTypes = types
	return b
}

// Metrics sets the metrics sink.
func (b *WorkerEngineBuilder) Metrics(sink MetricsSink) *WorkerEngineBuilder {
	b.metrics = sink
	return b
}

// Backend sets the storage backend. Required.
func (b *WorkerEngineBuilder) Backend(backend storage.Storage) *WorkerEngineBuilder {
	b.backend = backend
	return b
}

// Build creates the WorkerEngine with configured settings.
func (b *WorkerEngineBuilder) Build() (*WorkerEngine, error) {
	maxConcurrent := 10
	if b.maxWorkers != nil {
		maxConcurrent = *b.maxWorkers
	}
	pollIntervalSec := uint64(5)
	if b.pollInterval != nil {
		pollIntervalSec = uint64(b.pollInterval.Seconds())
	}

	if b.backend == nil {
		return nil, &WorkerError{
			Kind:    ErrConfiguration,
			Message: "No backend configured. Call .Backend(yourBackend) before .Build(). Use PostgresBackend for PostgreSQL.",
		}
	}

	queueName := "default"
	if b.queueName != nil {
		queueName = *b.queueName
	}

	leaseMS := uint64(60_000)
	reaperInterval := uint64(5)
	reaperBatch := 100

	config := WorkerConfig{
		QueueName:                   queueName,
		MaxConcurrentActivities:     maxConcurrent,
		SchedulePollIntervalSeconds: &pollIntervalSec,
		LeaseMS:                     &leaseMS,
		ReaperIntervalSeconds:       &reaperInterval,
		ReaperBatchSize:             &reaperBatch,
		ActivityTypes:               b.activityTypes,
	}

	engine := NewWorkerEngineWithBackend(b.backend, config)
	if b.metrics != nil {
		engine.SetMetrics(b.metrics)
	}

	return engine, nil
}
