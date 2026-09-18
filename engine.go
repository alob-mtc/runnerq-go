package runnerq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
)

// workerPoolHeartbeatInterval is how often the engine refreshes its
// runnerq_worker_pools row when the backend supports registration. Should be
// roughly 1/6 of the backend's liveness window so a single missed beat
// doesn't drop the pool out of cluster-wide capacity reporting.
const workerPoolHeartbeatInterval = 10 * time.Second

// WorkerEngine is the main activity processing engine.
type WorkerEngine struct {
	queue    activityQueue
	backend  storage.Storage
	handlers map[string]ActivityHandler
	config   WorkerConfig
	running  atomic.Bool
	metrics  MetricsSink

	// mu guards the shutdown machinery below. Start writes these fields and
	// Stop reads them, typically from different goroutines (`go engine.Start`
	// then `engine.Stop` from main is the documented usage), so unsynchronized
	// access is a data race; the mutex also makes closing shutdownCh
	// idempotent under concurrent Stop calls.
	mu           sync.Mutex
	active       bool               // true through startup and until the last drain finishes
	cancelFunc   context.CancelFunc // cancels the engine context (handlers, acks) — phase 3
	intakeCancel context.CancelFunc // cancels only the dequeue/poll loops — phase 1
	shutdownCh   chan struct{}

	// instanceID makes worker labels unique per engine instance. It feeds the
	// current_worker_id ack fence: without it, every process claims work as
	// "worker-N", so a stale worker whose lease expired could ack (and mark
	// completed/failed) a row that a different process had since reclaimed
	// and was still running.
	instanceID string

	// heartbeatInterval overrides attemptHeartbeatInterval; zero uses it.
	heartbeatInterval time.Duration

	poolID uuid.UUID // identity used for worker_pools registration; zero if backend doesn't support it
}

// NewWorkerEngineWithBackend creates a WorkerEngine from a custom backend.
func NewWorkerEngineWithBackend(backend storage.Storage, config WorkerConfig) *WorkerEngine {
	config = cloneWorkerConfig(config)
	// Propagate lease config to backends that support it.
	if lc, ok := backend.(storage.LeaseConfigurer); ok && config.LeaseMS != nil {
		leaseMS := min(*config.LeaseMS, math.MaxInt64)
		lc.SetLeaseMS(int64(leaseMS))
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
		instanceID: uuid.New().String(),
	}
}

// SetMetrics sets the metrics sink.
func (e *WorkerEngine) SetMetrics(sink MetricsSink) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.active {
		panic("cannot change metrics while engine is active")
	}
	if sink == nil {
		sink = NoopMetrics{}
	}
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

// RegisterActivity registers a handler under an activity type derived from
// its Go type name: &ResizeImage{} serves "ResizeImage". NameOf derives the
// same string for spawns. Panics for handlers whose type has no name (use
// RegisterActivityWithName), for a type already registered, or once the
// engine is running.
//
// The activity type is persisted with every enqueued activity. Renaming a
// handler struct therefore strands rows already in the store; pin the name
// with RegisterActivityWithName where that matters.
func (e *WorkerEngine) RegisterActivity(handler ActivityHandler) {
	if isNilHandler(handler) {
		panic("handler must be non-nil")
	}
	activityType, err := activityTypeOf(reflect.TypeOf(handler))
	if err != nil {
		panic(err)
	}
	e.RegisterActivityWithName(activityType, handler)
}

// RegisterActivityWithName registers a handler under an explicit activity
// type. Use it to decouple the persisted type from the handler's Go name, or
// to serve several types from one handler type. Panics on an empty type or
// nil handler, a type already registered, or once the engine is running.
func (e *WorkerEngine) RegisterActivityWithName(activityType string, handler ActivityHandler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.active {
		panic("cannot register activities while engine is active")
	}
	if activityType == "" || isNilHandler(handler) {
		panic("activity type and handler must be non-empty")
	}
	if _, dup := e.handlers[activityType]; dup {
		panic(fmt.Sprintf("activity type %q is already registered", activityType))
	}
	e.handlers[activityType] = handler
}

// isNilHandler reports whether handler is nil, including a typed nil such as
// (*ChargeCard)(nil) wrapped in the interface — which compares unequal to nil
// yet panics (or silently misbehaves) on the first Handle call.
func isNilHandler(handler ActivityHandler) bool {
	if handler == nil {
		return true
	}
	v := reflect.ValueOf(handler)
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface:
		return v.IsNil()
	}
	return false
}

// GetActivityExecutor returns an executor for spawning activities from
// outside a handler. Spawns made through it are roots (no parent lineage).
func (e *WorkerEngine) GetActivityExecutor() *ActivityExecutor {
	return newActivityExecutor(e.queue, e.config.MaxActivityDepth)
}

// Start starts the worker engine and blocks until shutdown or error.
func (e *WorkerEngine) Start(ctx context.Context) error {
	e.mu.Lock()
	if e.active {
		e.mu.Unlock()
		return &WorkerError{Kind: ErrAlreadyRunning}
	}
	if err := ctx.Err(); err != nil {
		e.mu.Unlock()
		return err
	}
	if e.config.MaxConcurrentActivities <= 0 || len(e.handlers) == 0 {
		e.mu.Unlock()
		return &WorkerError{Kind: ErrConfiguration, Message: "at least one worker and registered handler are required"}
	}
	types := slices.Clone(e.config.ActivityTypes)
	if len(types) == 0 {
		for t := range e.handlers {
			types = append(types, t)
		}
		slices.Sort(types)
	}
	for _, t := range types {
		if _, ok := e.handlers[t]; !ok {
			e.mu.Unlock()
			return &WorkerError{Kind: ErrConfiguration, Message: fmt.Sprintf("no handler registered for activity type %q", t)}
		}
	}
	if r := e.config.Retention; r != nil && (r.Completed < 0 || r.Failed < 0 || r.Interval < 0 || r.BatchSize < 0) {
		e.mu.Unlock()
		return &WorkerError{Kind: ErrConfiguration, Message: "retention values must be non-negative"}
	}
	if q, ok := e.queue.(activityTypeFilter); ok {
		q.setActivityTypes(types)
	}
	e.active = true
	e.running.Store(true)
	// Parent cancellation stops intake below. Handler and persistence lifetime
	// ends after the drain, including when the caller uses a signal context.
	engineCtx, engineCancel := context.WithCancel(context.WithoutCancel(ctx))
	intakeCtx, intakeCancel := context.WithCancel(engineCtx)
	e.cancelFunc = engineCancel
	e.intakeCancel = intakeCancel
	e.shutdownCh = make(chan struct{})
	shutdownCh := e.shutdownCh
	e.mu.Unlock()
	defer engineCancel()
	slog.Info("Starting worker engine", "max_concurrent_activities", e.config.MaxConcurrentActivities)

	// Register this pool so cluster-wide capacity reporting stays accurate.
	// Registration failure is non-fatal — the engine still runs, the KPI just
	// under-reports until a later heartbeat re-establishes the row.
	e.poolID = uuid.New()
	info := storage.WorkerPoolInfo{
		PoolID:        e.poolID,
		QueueName:     e.config.QueueName,
		MaxWorkers:    e.config.MaxConcurrentActivities,
		ActivityTypes: types,
	}
	registrationCtx, registrationCancel := context.WithTimeout(intakeCtx, storageAttemptTimeout)
	if err := e.backend.RegisterWorkerPool(registrationCtx, info); err != nil {
		slog.Warn("Failed to register worker pool", "error", err, "pool_id", e.poolID)
		e.poolID = uuid.Nil
	} else {
		slog.Info("Registered worker pool", "pool_id", e.poolID, "max_workers", info.MaxWorkers)
	}

	registrationCancel()

	var wg sync.WaitGroup

	// Heartbeat the worker_pools row so we keep counting toward cluster capacity.
	if e.poolID != uuid.Nil {
		wg.Go(func() {
			e.runWorkerPoolHeartbeat(intakeCtx)
		})
	}

	// Scheduled activities processor (skipped if backend handles it natively)
	if !e.queue.SchedulesNatively() {
		wg.Go(func() {
			e.runScheduledProcessor(intakeCtx)
		})
	}

	// Reaper processor
	wg.Go(func() {
		e.runReaperProcessor(intakeCtx)
	})

	// Retention sweeper (opt-in). Safe to run on every engine: the backend
	// elects one sweeper per queue via an advisory lock.
	if e.config.Retention != nil {
		wg.Go(func() {
			e.runRetentionProcessor(intakeCtx)
		})
	}

	// Intake. A backend that can claim in bulk gets one dispatcher that
	// claims exactly as many activities as the engine has idle slots; every
	// other backend gets the fixed pool of one blocking-claim loop per slot.
	// Long waits inside handlers (child awaits, sleeps, signal waits)
	// yield-park the activity rather than holding the slot in either mode.
	if batchQueue, ok := e.queue.(batchActivityQueue); ok {
		wg.Go(func() {
			e.runBatchDispatcher(intakeCtx, engineCtx, batchQueue)
		})
	} else {
		for i := range e.config.MaxConcurrentActivities {
			wg.Go(func() {
				e.runWorkerLoop(intakeCtx, engineCtx, i)
			})
		}
	}

	// Wait for shutdown signal or context cancellation
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Stop may have cancelled intake during pool registration.

	if !e.running.Load() {
		slog.Info("Shutdown requested during startup")
	} else {
		select {
		case sig := <-sigCh:
			slog.Info("Received shutdown signal", "signal", sig)
		case <-ctx.Done():
			slog.Info("Context cancelled")
		case <-shutdownCh:
			slog.Info("Shutdown requested")
		}
	}

	// Phase 1: stop intake. Loops unwind; in-flight handlers keep running on
	// the still-live engineCtx so they can complete and ack instead of being
	// guaranteed to fail their ack and rerun. engineCtx is cancelled by the
	// deferred engineCancel once the drain below finishes (or the grace
	// budget expires).
	e.stop()

	// Phase 2: drain. Single shutdown grace covering everything in parallel —
	// worker loops, in-flight activity goroutines, and pool
	// deregistration. Previous design ran them sequentially with
	// per-stage timeouts (wg.Wait unbounded + 30s + 10s + 5s) which on a
	// busy engine could push shutdown past a minute and time out the
	// orchestrator's SIGTERM grace. Worst case is now ShutdownGraceSeconds
	// regardless of how many goroutines are still in flight.
	graceSec := uint64(30)
	if e.config.ShutdownGraceSeconds != nil {
		graceSec = max(*e.config.ShutdownGraceSeconds, 1)
	}
	graceCtx, graceCancel := context.WithTimeout(context.Background(), time.Duration(graceSec)*time.Second)
	defer graceCancel()

	type drainTask struct {
		name string
		fn   func()
	}
	tasks := []drainTask{
		// Worker loops, reaper, scheduled processor, heartbeat — everything
		// that wg.Add'd into the supervisor wg above. In-flight activities
		// run inside the worker goroutines, so they're covered too.
		{"workers", func() { wg.Wait() }},
		// Best-effort pool deregister — uses graceCtx so it can't outlive
		// the budget on its own.
		{"deregister", func() {
			if e.poolID == uuid.Nil {
				return
			}
			if err := e.backend.DeregisterWorkerPool(graceCtx, e.poolID); err != nil && graceCtx.Err() == nil {
				slog.Warn("Failed to deregister worker pool", "error", err, "pool_id", e.poolID)
			}
		}},
	}

	done := make(chan string, len(tasks))
	var drains sync.WaitGroup
	for _, t := range tasks {
		drains.Go(func() {
			t.fn()
			done <- t.name
		})
	}
	drainsDone := make(chan struct{})
	go func() { drains.Wait(); close(drainsDone) }()
	defer func() {
		engineCancel()
		finish := func() { e.mu.Lock(); e.active = false; e.mu.Unlock() }
		select {
		case <-drainsDone:
			finish()
		default:
			go func() { <-drainsDone; finish() }()
		}
	}()

	finished := 0
	for finished < len(tasks) {
		select {
		case name := <-done:
			slog.Debug("Shutdown drain complete", "stage", name)
			finished++
		case <-graceCtx.Done():
			pending := len(tasks) - finished
			slog.Warn("Shutdown grace exceeded; returning with drains in flight",
				"grace_seconds", graceSec, "pending_drains", pending)
			signal.Stop(sigCh)
			return nil
		}
	}

	signal.Stop(sigCh)
	slog.Info("Worker engine stopped")
	return nil
}

// Stop initiates a graceful shutdown of the worker engine.
func (e *WorkerEngine) Stop() {
	e.stop()
}

// stop is phase 1 of shutdown: it stops work intake (dequeue/poll loops) but
// deliberately leaves the engine context alive so in-flight handlers can
// finish and ack. Cancelling everything here — as earlier versions did — meant
// a handler that completed during the drain acked with a dead context, failed,
// and was guaranteed to rerun on another worker. Start performs the drain
// (phase 2) and the final engine-context cancel (phase 3).
func (e *WorkerEngine) stop() {
	slog.Info("Stopping worker engine")
	e.mu.Lock()
	e.running.Store(false)
	defer e.mu.Unlock()
	if e.intakeCancel != nil {
		e.intakeCancel()
	}
	if e.shutdownCh != nil {
		select {
		case <-e.shutdownCh:
		default:
			close(e.shutdownCh)
		}
	}
}

// workerDequeueBlock is how long a worker's Dequeue call parks waiting for
// work. The backend wakes parked dequeuers on new-work notifications and
// re-probes internally, so this only bounds how often the loop comes up for
// air; ctx cancellation aborts the wait immediately for shutdown.
const workerDequeueBlock = 15 * time.Second

// runWorkerLoop polls for work on ctx (the intake context, cancelled first
// during shutdown) but executes claimed activities on handlerCtx (the engine
// context, which outlives intake so a draining handler can still ack).
func (e *WorkerEngine) runWorkerLoop(ctx, handlerCtx context.Context, workerID int) {
	slog.Debug("Starting worker loop", "worker_id", workerID)
	// Every claim receives a fresh execution identity, including subsequent
	// claims on the same worker slot. Old acknowledgements cannot match a new attempt.

	for e.running.Load() {
		select {
		case <-ctx.Done():
			slog.Debug("Worker loop stopped (context)", "worker_id", workerID)
			return
		default:
		}

		// Blocking dequeue: parks inside the backend until work is signalled
		// or the block window elapses. No idle sleep/backoff needed here —
		// an empty return means the window expired and we simply re-enter.
		workerLabel := fmt.Sprintf("%s:worker-%d:%s", e.instanceID, workerID, uuid.New())
		act, err := e.queue.Dequeue(ctx, workerDequeueBlock, workerLabel)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("Failed to dequeue activity", "worker_id", workerID, "error", err)
			if !pauseAfterDequeueError(ctx) {
				return
			}
			continue
		}

		if act == nil {
			continue
		}

		e.processActivity(handlerCtx, act, workerLabel, workerID)
	}

	slog.Debug("Worker loop stopped", "worker_id", workerID)
}

// runBatchDispatcher is the intake loop for batch-capable backends. One
// goroutine owns the engine's concurrency budget: idle slots are tokens in a
// channel, the dispatcher gathers every slot idle right now, claims that many
// activities in one round trip, and hands each claim to its own goroutine,
// which returns the slot when the activity completes, parks, or fails.
//
// Claiming only what is idle means a lease is never held by an activity
// waiting behind an in-memory backlog, and a busy engine issues one claim per
// wave of freed slots instead of one per slot. Like runWorkerLoop it claims on
// ctx (intake) and executes on handlerCtx (engine), and it does not return
// until every activity it dispatched has finished, so Start's drain sees the
// same thing it would from the fixed pool.
func (e *WorkerEngine) runBatchDispatcher(ctx, handlerCtx context.Context, queue batchActivityQueue) {
	n := e.config.MaxConcurrentActivities
	slog.Debug("Starting batch dispatcher", "max_concurrent_activities", n)

	// Slot numbers double as the worker_id in logs, matching the fixed pool.
	idle := make(chan int, n)
	for slot := range n {
		idle <- slot
	}
	var inFlight sync.WaitGroup
	defer inFlight.Wait()

	var held []int // slots taken from idle and not yet assigned to a claim
	for e.running.Load() {
		if len(held) == 0 {
			select {
			case <-ctx.Done():
				return
			case slot := <-idle:
				held = append(held, slot)
			}
		}
		held = takeIdle(idle, held)

		// A fresh prefix per call: the backend appends each activity's id, so
		// a token is unique per (call, activity), as the fixed pool's
		// per-claim uuid is unique per claim.
		prefix := fmt.Sprintf("%s:batch:%s", e.instanceID, uuid.New())
		claims, err := queue.DequeueBatch(ctx, len(held), workerDequeueBlock, prefix)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("Failed to batch dequeue activities", "error", err, "limit", len(held))
			if !pauseAfterDequeueError(ctx) {
				return
			}
			continue
		}
		for _, claim := range claims {
			slot := held[len(held)-1]
			held = held[:len(held)-1]
			inFlight.Go(func() {
				defer func() { idle <- slot }()
				e.processActivity(handlerCtx, claim.activity, claim.leaseID, slot)
			})
		}
	}

	slog.Debug("Batch dispatcher stopped")
}

// takeIdle appends every slot that is idle right now to held, without waiting.
func takeIdle(idle <-chan int, held []int) []int {
	for {
		select {
		case slot := <-idle:
			held = append(held, slot)
		default:
			return held
		}
	}
}

// pauseAfterDequeueError holds an intake loop back for a second after a
// failed claim so a struggling backend is not hammered. False means ctx ended
// during the pause and the loop should exit.
func pauseAfterDequeueError(ctx context.Context) bool {
	select {
	case <-time.After(time.Second):
		return true
	case <-ctx.Done():
		return false
	}
}

func (e *WorkerEngine) processActivity(ctx context.Context, act *activity, workerLabel string, workerID int) {
	activityID := act.ID
	activityType := act.ActivityType

	slog.Debug("Worker processing activity", "worker_id", workerID, "activity_id", activityID, "activity_type", activityType)

	handler, ok := e.handlers[activityType]
	if !ok {
		slog.Error("No handler found for activity type", "worker_id", workerID, "activity_id", activityID, "activity_type", activityType)
		if _, err := e.queue.MarkFailed(ctx, act, "handler_not_found", true, workerLabel); err != nil {
			slog.Error("Failed to mark activity as failed", "worker_id", workerID, "activity_id", activityID, "error", err)
		}
		return
	}

	activityTimeout := time.Duration(act.TimeoutSeconds) * time.Second
	deadlineCtx, timeoutCancel := context.WithTimeout(ctx, activityTimeout)
	defer timeoutCancel()
	// revoke cancels the handler early when the heartbeat finds the claim
	// lost; context.Cause then reports the ErrClaimLost error.
	timeoutCtx, revoke := context.WithCancelCause(deadlineCtx)
	defer revoke(nil)
	// Mark the context as handler-scoped so in-handler GetResult calls may
	// yield-park this activity; awaits outside a handler block normally.
	timeoutCtx = withHandlerScope(timeoutCtx)

	attemptQ := &attemptQueue{activityQueue: e.queue, backend: e.backend, owner: act.ID, worker: workerLabel, persistenceCtx: ctx, metrics: e.metrics}
	timeoutCtx = context.WithValue(timeoutCtx, attemptQueueKey{}, attemptQ)
	scopedExecutor := newActivityExecutor(attemptQ, e.config.MaxActivityDepth).scopedForChild(act)

	actCtx := ActivityContext{
		ActivityID:       activityID,
		ActivityType:     activityType,
		RetryCount:       act.RetryCount,
		Metadata:         act.Metadata,
		Ctx:              timeoutCtx,
		ActivityExecutor: scopedExecutor,
		ParentActivityID: act.ParentActivityID,
		RootActivityID:   act.RootActivityID,
		Depth:            act.Depth,
		queue:            attemptQ,
	}

	payloadForDL := make(json.RawMessage, len(act.Payload))
	copy(payloadForDL, act.Payload)

	stopHeartbeat := attemptQ.heartbeat(timeoutCtx, e.heartbeatInterval, revoke)
	result, handlerErr := e.safeHandle(handler, actCtx, act.Payload)
	stopHeartbeat()

	// Another execution owns the activity now. Whatever the handler returned —
	// a result included — belongs to a superseded attempt; the acks would be
	// rejected by the fence anyway.
	if cause := context.Cause(timeoutCtx); cause != nil {
		if se, ok := storage.IsStorageError(cause); ok && se.Kind == storage.ErrClaimLost {
			e.metrics.IncCounter("activity_claim_lost", 1)
			slog.Warn("Activity execution lost its claim; handler was cancelled", "worker_id", workerID, "activity_id", activityID, "activity_type", activityType, "error", cause)
			return
		}
	}

	// A yielding durable Sleep is not a failure: park the activity until its
	// wake time without consuming a retry. Checked before the timeout so a
	// yield that raced the deadline is still honored as a yield.
	var ys *yieldPark
	if errors.As(handlerErr, &ys) {
		e.handleYield(ctx, act, ys, workerLabel, workerID, activityID, activityType)
		return
	}

	// A successfully returned outcome is worth persisting even if the handler
	// deadline elapsed during checkpoint recovery. Ownership still fences it.
	if handlerErr == nil {
		e.handleSuccess(ctx, act, result, workerLabel, workerID, activityID, activityType)
		return
	}
	if se, ok := storage.IsStorageError(handlerErr); ok && se.Kind == storage.ErrClaimLost {
		e.metrics.IncCounter("activity_claim_lost", 1)
		slog.Warn("Activity execution lost its claim", "activity_id", activityID, "error", handlerErr)
		return
	}
	retryable := retryableError(handlerErr)
	if !retryable {
		e.handleNonRetryableFailure(ctx, act, handlerErr.Error(), workerLabel, workerID, activityID, activityType)
		return
	}
	if timeoutCtx.Err() == context.DeadlineExceeded {
		e.handleTimeout(ctx, act, handler, actCtx, payloadForDL, workerLabel, workerID, activityID, activityType, activityTimeout)
		return
	}
	e.handleRetryableFailure(ctx, act, handler, actCtx, payloadForDL, handlerErr.Error(), workerLabel, workerID, activityID, activityType)
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
	started := time.Now()
	e.metrics.IncCounter("activity_completion_pending_started", 1)
	defer func() {
		e.metrics.IncCounter("activity_completion_pending_finished", 1)
		e.metrics.ObserveDuration("activity_completion_persistence", time.Since(started))
	}()
	attemptQ := &attemptQueue{backend: e.backend, owner: act.ID, worker: workerLabel}
	err := retryStorage(ctx, "complete activity", e.metrics, attemptQ.renew, func(ctx context.Context) error {
		return e.queue.MarkCompleted(ctx, act, result, workerLabel)
	})
	if err != nil {
		counter := "activity_completion_error"
		if se, ok := storage.IsStorageError(err); ok && se.Kind == storage.ErrClaimLost {
			counter = "activity_claim_lost"
		}
		e.metrics.IncCounter(counter, 1)
		slog.Error("Failed to confirm activity completion", "worker_id", workerID, "activity_id", activityID, "activity_type", activityType, "error", err)
		return
	}
	e.metrics.IncCounter("activity_completed", 1)
	slog.Info("Activity completed successfully", "worker_id", workerID, "activity_id", activityID, "activity_type", activityType)
}

// handleYield parks a sleeping activity as scheduled until its wake time.
// On failure the row simply stays processing: its lease expires, the reaper
// requeues it, and the handler replays to the same Sleep — degraded latency
// and one consumed retry, not lost work.
func (e *WorkerEngine) handleYield(ctx context.Context, act *activity, ys *yieldPark, workerLabel string, workerID int, activityID any, activityType string) {
	// Recheck waits periodically even if this process dies immediately after
	// park commits. The original signal/timer deadline remains checkpointed;
	// this only schedules a replay to discover a result or repark safely.
	wakeAt := ys.wakeAt
	if ys.recheck != uuid.Nil {
		if recheckAt := time.Now().UTC().Add(time.Minute); wakeAt.After(recheckAt) {
			wakeAt = recheckAt
		}
	}
	attemptQ := &attemptQueue{backend: e.backend, owner: act.ID, worker: workerLabel}
	err := retryStorage(ctx, "park activity", e.metrics, attemptQ.renew, func(ctx context.Context) error {
		if b, ok := e.backend.(storage.DependencyStorage); ok && ys.recheck != uuid.Nil {
			var producer *uuid.UUID
			if ys.kind == "await" {
				producer = &ys.recheck
			}
			return b.YieldForResult(ctx, act.ID, ys.recheck, producer, wakeAt, workerLabel, ys.kind, ys.step)
		}
		return e.queue.Yield(ctx, act, wakeAt, workerLabel, ys.kind, ys.step)
	})
	if err != nil {
		slog.Error("Failed to confirm durable park", "activity_id", activityID, "error", err)
		return
	}
	e.metrics.IncCounter("activity_yielded", 1)
	slog.Debug("Activity yielded for durable wait",
		"worker_id", workerID, "activity_id", activityID, "activity_type", activityType,
		"step", ys.step, "wake_at", ys.wakeAt)

	// Close the park race: a result (signal, child completion) that committed
	// between the handler's final check and the park above produced no wake —
	// the row wasn't 'waiting' yet when the producer looked. Re-check now
	// that the park is committed and self-wake if the awaited result exists.
	// Failure here is bounded by the one-minute replay deadline above plus
	// queue availability. Durable dependency registration can avoid polling.
	if ys.recheck == uuid.Nil {
		return
	}
	if _, durable := e.backend.(storage.DependencyStorage); durable {
		return
	}
	res, err := e.queue.GetResult(ctx, ys.recheck)
	if err != nil || res == nil {
		if err != nil {
			slog.Warn("Post-park recheck failed; the park deadline will recover",
				"activity_id", activityID, "error", err)
		}
		return
	}
	if _, err := e.backend.WakeWaiting(ctx, act.ID); err != nil {
		slog.Warn("Post-park self-wake failed; the park deadline will recover",
			"activity_id", activityID, "error", err)
	}
}

func (e *WorkerEngine) handleRetryableFailure(ctx context.Context, act *activity, handler ActivityHandler, actCtx ActivityContext, payloadForDL json.RawMessage, reason string, workerLabel string, workerID int, activityID any, activityType string) {
	e.metrics.IncCounter("activity_retry", 1)
	slog.Warn("Activity requesting retry", "worker_id", workerID, "activity_id", activityID, "activity_type", activityType, "reason", reason)

	deadLettered, err := e.persistFailure(ctx, act, reason, true, workerLabel)
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
			ActivityExecutor: newActivityExecutor(e.queue, e.config.MaxActivityDepth).scopedForChild(act),
			ParentActivityID: act.ParentActivityID,
			RootActivityID:   act.RootActivityID,
			Depth:            act.Depth,
		}
		e.callDeadLetter(handler, dlCtx, payloadForDL, reason)
	}
}

func (e *WorkerEngine) handleNonRetryableFailure(ctx context.Context, act *activity, reason string, workerLabel string, workerID int, activityID any, activityType string) {
	e.metrics.IncCounter("activity_failed_non_retry", 1)
	slog.Error("Activity failed", "worker_id", workerID, "activity_id", activityID, "activity_type", activityType, "reason", reason)

	// The error result row is written inside the AckFailure transaction by the
	// backend, so no separate result-storage step is needed here.
	if _, err := e.persistFailure(ctx, act, reason, false, workerLabel); err != nil {
		slog.Error("Failed to mark activity as failed", "worker_id", workerID, "activity_id", activityID, "error", err)
	}
}

func (e *WorkerEngine) handleTimeout(ctx context.Context, act *activity, handler ActivityHandler, actCtx ActivityContext, payloadForDL json.RawMessage, workerLabel string, workerID int, activityID any, activityType string, timeout time.Duration) {
	e.metrics.IncCounter("activity_timeout", 1)
	errorMsg := "Activity execution timed out"
	slog.Error("Activity timed out", "worker_id", workerID, "activity_id", activityID, "activity_type", activityType, "timeout", timeout)

	deadLettered, err := e.persistFailure(ctx, act, errorMsg, true, workerLabel)
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
			ActivityExecutor: newActivityExecutor(e.queue, e.config.MaxActivityDepth).scopedForChild(act),
			ParentActivityID: act.ParentActivityID,
			RootActivityID:   act.RootActivityID,
			Depth:            act.Depth,
		}
		e.callDeadLetter(handler, dlCtx, payloadForDL, errorMsg)
	}
}

func (e *WorkerEngine) callDeadLetter(handler ActivityHandler, ctx ActivityContext, payload json.RawMessage, reason string) {
	defer func() {
		if r := recover(); r != nil {
			e.metrics.IncCounter("activity_dead_letter_hook_panic", 1)
			slog.Error("Dead-letter hook panicked", "activity_id", ctx.ActivityID, "panic", r)
		}
	}()
	handler.OnDeadLetter(ctx, payload, reason)
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

// runWorkerPoolHeartbeat keeps this engine's runnerq_worker_pools row marked
// as alive so the cluster-wide MaxWorkers reported by Stats() stays accurate.
// A failure to heartbeat is logged but doesn't shut down the engine — the row
// will simply age out of the liveness window until the next successful beat.
func (e *WorkerEngine) runWorkerPoolHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(workerPoolHeartbeatInterval)
	defer ticker.Stop()
	for e.running.Load() {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := e.backend.HeartbeatWorkerPool(ctx, e.poolID); err != nil {
				slog.Warn("Worker pool heartbeat failed", "error", err, "pool_id", e.poolID)
			}
		}
	}
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

// runRetentionProcessor periodically asks the backend to delete terminal
// workflow trees older than the configured TTLs. Each tick drains: it keeps
// sweeping until a batch comes back short, so a backlog accumulated while the
// engine was down clears quickly instead of one batch per interval.
func (e *WorkerEngine) runRetentionProcessor(ctx context.Context) {
	cfg := e.config.Retention
	interval := cfg.Interval
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	policy := storage.RetentionPolicy{Completed: cfg.Completed, Failed: cfg.Failed}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slog.Debug("Starting retention sweeper", "completed_ttl", cfg.Completed, "failed_ttl", cfg.Failed, "interval", interval)
	for e.running.Load() {
		select {
		case <-ctx.Done():
			slog.Debug("Retention sweeper stopped")
			return
		case <-ticker.C:
			for {
				n, err := e.backend.CleanupExpired(ctx, policy, batchSize)
				if err != nil {
					if ctx.Err() == nil {
						slog.Error("Retention sweep failed", "error", err)
					}
					break
				}
				if n > 0 {
					e.metrics.IncCounter("activity_trees_swept", n)
					slog.Debug("Retention sweep deleted workflow trees", "trees", n)
				}
				if int(n) < batchSize {
					break
				}
			}
		}
	}
	slog.Debug("Retention sweeper stopped")
}

// WorkerEngineBuilder provides fluent configuration for WorkerEngine.
type WorkerEngineBuilder struct {
	queueName     *string
	maxWorkers    *int
	pollInterval  *time.Duration
	metrics       MetricsSink
	backend       storage.Storage
	activityTypes []string
	shutdownGrace *time.Duration
	retention     *RetentionConfig
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

// Retention opts the engine into deleting old terminal workflow trees —
// see RetentionConfig. Safe to set on every engine in a cluster; the backend
// elects one sweeper per queue.
func (b *WorkerEngineBuilder) Retention(cfg RetentionConfig) *WorkerEngineBuilder {
	b.retention = &cfg
	return b
}

func (b *WorkerEngineBuilder) ShutdownGrace(d time.Duration) *WorkerEngineBuilder {
	b.shutdownGrace = &d
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
		Retention:                   b.retention,
	}
	if b.shutdownGrace != nil {
		secs := uint64((*b.shutdownGrace).Seconds())
		if secs == 0 {
			secs = 1
		}
		config.ShutdownGraceSeconds = &secs
	}

	engine := NewWorkerEngineWithBackend(b.backend, config)
	if b.metrics != nil {
		engine.SetMetrics(b.metrics)
	}

	return engine, nil
}

// Failure decisions are outcomes too: retry their acknowledgement independently
// of the handler. An idempotent backend resolves an already-committed transition.
func (e *WorkerEngine) persistFailure(ctx context.Context, act *activity, reason string, retryable bool, worker string) (dead bool, err error) {
	q := &attemptQueue{backend: e.backend, owner: act.ID, worker: worker}
	err = retryStorage(ctx, "record activity failure", e.metrics, q.renew, func(ctx context.Context) error {
		var err error
		dead, err = e.queue.MarkFailed(ctx, act, reason, retryable, worker)
		return err
	})
	return
}
