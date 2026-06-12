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
	cancelFunc   context.CancelFunc // cancels the engine context (handlers, acks) — phase 3
	intakeCancel context.CancelFunc // cancels only the dequeue/poll loops — phase 1
	shutdownCh   chan struct{}

	// instanceID makes worker labels unique per engine instance, and claimSeq
	// makes dispatcher claims unique per attempt. Both feed the
	// current_worker_id ack fence: without them, every process claims work as
	// "worker-N"/"dispatcher", so a stale worker whose lease expired could ack
	// (and mark completed/failed) a row that a different process had since
	// reclaimed and was still running.
	instanceID string
	claimSeq   atomic.Uint64

	poolID uuid.UUID // identity used for worker_pools registration; zero if backend doesn't support it

	// SuspendOnAwait dispatcher state. These are nil and unused when the
	// flag is off; the engine falls back to the fixed-goroutine pool.
	workerSem  chan struct{}  // primary slot pool; capacity = MaxConcurrentActivities - SuspendLeavesReserved
	leafSem    chan struct{}  // reservation pool, only populated when SuspendLeavesReserved>0 && len(SuspendLeafActivityTypes)>0
	leafQueue  activityQueue  // adapter filtered to leaf types (paired with leafSem)
	activityWg sync.WaitGroup // tracks in-flight activity goroutines (suspend mode only)
}

// NewWorkerEngineWithBackend creates a WorkerEngine from a custom backend.
func NewWorkerEngineWithBackend(backend storage.Storage, config WorkerConfig) *WorkerEngine {
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
		instanceID: uuid.New().String()[:8],
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
// Spawns made through the returned executor are roots (no parent lineage).
func (e *WorkerEngine) GetActivityExecutor() ActivityExecutor {
	return newWorkerEngineWrapperWithDepth(e.queue, e.config.MaxActivityDepth)
}

// Start starts the worker engine and blocks until shutdown or error.
func (e *WorkerEngine) Start(ctx context.Context) error {
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

	// Retention config sanity: a negative TTL would silently behave like
	// "keep forever" (the backend treats <= 0 as disabled), which is the
	// opposite of what a misconfigured caller intended. Fail fast instead.
	if r := e.config.Retention; r != nil {
		if r.Completed < 0 || r.Failed < 0 || r.Interval < 0 || r.BatchSize < 0 {
			return &WorkerError{
				Kind:    ErrConfiguration,
				Message: "Retention TTLs, Interval, and BatchSize must be >= 0 (zero = keep forever / use default)",
			}
		}
	}

	// SuspendOnAwait config sanity. Fail fast on the misconfigurations that
	// would otherwise silently degrade capacity, waste reserved slots, or
	// (worst case) cause this engine's leaf dispatcher to dequeue activities
	// it can't run — stealing work that another engine in the cluster would
	// have handled.
	if e.config.SuspendOnAwait {
		if e.config.SuspendLeavesReserved < 0 {
			return &WorkerError{
				Kind:    ErrConfiguration,
				Message: "SuspendLeavesReserved must be >= 0",
			}
		}
		if e.config.SuspendLeavesReserved >= e.config.MaxConcurrentActivities {
			return &WorkerError{
				Kind: ErrConfiguration,
				Message: fmt.Sprintf(
					"SuspendLeavesReserved (%d) must be < MaxConcurrentActivities (%d); otherwise the primary pool collapses to 1 slot",
					e.config.SuspendLeavesReserved, e.config.MaxConcurrentActivities,
				),
			}
		}
		if e.config.SuspendLeavesReserved > 0 && len(e.config.SuspendLeafActivityTypes) == 0 {
			return &WorkerError{
				Kind:    ErrConfiguration,
				Message: "SuspendLeavesReserved > 0 requires SuspendLeafActivityTypes to be non-empty; otherwise reserved slots become dead capacity",
			}
		}
		var missingLeaf []string
		for _, t := range e.config.SuspendLeafActivityTypes {
			if _, ok := e.handlers[t]; !ok {
				missingLeaf = append(missingLeaf, t)
			}
		}
		if len(missingLeaf) > 0 {
			return &WorkerError{
				Kind: ErrConfiguration,
				Message: fmt.Sprintf(
					"SuspendLeafActivityTypes contains types with no registered handler: %v — leaf dispatcher would dequeue them and mark them handler_not_found, stealing work from other engines",
					missingLeaf,
				),
			}
		}
	}

	if !e.running.CompareAndSwap(false, true) {
		return &WorkerError{Kind: ErrAlreadyRunning}
	}

	slog.Info("Starting worker engine", "max_concurrent_activities", e.config.MaxConcurrentActivities)

	// Two nested contexts implement two-phase shutdown: intakeCtx is cancelled
	// first (stops dequeue/poll loops), engineCtx stays live through the drain
	// so in-flight handlers can complete and ack, and is cancelled last.
	engineCtx, engineCancel := context.WithCancel(ctx)
	defer engineCancel()
	intakeCtx, intakeCancel := context.WithCancel(engineCtx)

	e.mu.Lock()
	e.cancelFunc = engineCancel
	e.intakeCancel = intakeCancel
	e.shutdownCh = make(chan struct{})
	shutdownCh := e.shutdownCh
	e.mu.Unlock()

	// Register this pool so cluster-wide capacity reporting stays accurate.
	// Registration failure is non-fatal — the engine still runs, the KPI just
	// under-reports until a later heartbeat re-establishes the row.
	e.poolID = uuid.New()
	info := storage.WorkerPoolInfo{
		PoolID:        e.poolID,
		QueueName:     e.config.QueueName,
		MaxWorkers:    e.config.MaxConcurrentActivities,
		ActivityTypes: e.config.ActivityTypes,
	}
	if err := e.backend.RegisterWorkerPool(engineCtx, info); err != nil {
		slog.Warn("Failed to register worker pool", "error", err, "pool_id", e.poolID)
		e.poolID = uuid.Nil
	} else {
		slog.Info("Registered worker pool", "pool_id", e.poolID, "max_workers", info.MaxWorkers)
	}

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

	if e.config.SuspendOnAwait {
		// Semaphore-based dispatcher model. A parent's slot is released
		// while it waits on a child future (see suspend.go) so leaves can
		// keep dequeueing — eliminates the parent-blocking starvation
		// pattern on recursive fan-out workloads.
		primary := max(e.config.MaxConcurrentActivities-e.config.SuspendLeavesReserved, 1)
		e.workerSem = make(chan struct{}, primary)
		wg.Go(func() {
			e.runSuspendDispatcher(intakeCtx, engineCtx, e.queue, e.workerSem, "dispatcher")
		})

		// Optional reservation: a separate dispatcher with its own slot
		// pool that dequeues only leaf types, so leaves can always run
		// even when every primary slot holds a suspended parent that's
		// about to wake.
		if e.config.SuspendLeavesReserved > 0 && len(e.config.SuspendLeafActivityTypes) > 0 {
			e.leafSem = make(chan struct{}, e.config.SuspendLeavesReserved)
			e.leafQueue = newBackendQueueAdapter(e.backend, e.config.SuspendLeafActivityTypes)
			wg.Go(func() {
				e.runSuspendDispatcher(intakeCtx, engineCtx, e.leafQueue, e.leafSem, "dispatcher-leaf")
			})
		}
	} else {
		// Existing fixed-goroutine pool. One worker per slot.
		for i := 0; i < e.config.MaxConcurrentActivities; i++ {
			workerID := i
			wg.Go(func() {
				e.runWorkerLoop(intakeCtx, engineCtx, workerID)
			})
		}
	}

	// Wait for shutdown signal or context cancellation
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// A Stop that ran to completion between the running CAS and the mu block
	// above closed the previous shutdownCh, not the one published there — so
	// re-check the flag instead of waiting on a signal that already fired.
	// Any Stop after the mu block closes the published channel and wakes the
	// select normally.
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
	// worker loops, dispatchers, in-flight activity goroutines, and pool
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
		// Worker loops, dispatchers, reaper, scheduled processor, heartbeat —
		// everything that wg.Add'd into the supervisor wg above.
		{"workers", func() { wg.Wait() }},
		// In-flight activity goroutines (suspend mode only — non-suspend
		// activities are inside the worker goroutines covered by wg).
		{"activities", func() {
			if e.config.SuspendOnAwait {
				e.activityWg.Wait()
			}
		}},
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
	for _, t := range tasks {
		t := t
		go func() {
			t.fn()
			done <- t.name
		}()
	}

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
	e.running.Store(false)
	e.mu.Lock()
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

// dispatcherDequeueBlock is the suspend-dispatcher equivalent. It is short
// because the dispatcher holds a semaphore slot while blocked in Dequeue, and
// a waking suspended parent may need that slot to resume — a long block here
// would delay parents on an otherwise idle engine.
const dispatcherDequeueBlock = time.Second

// runWorkerLoop polls for work on ctx (the intake context, cancelled first
// during shutdown) but executes claimed activities on handlerCtx (the engine
// context, which outlives intake so a draining handler can still ack).
func (e *WorkerEngine) runWorkerLoop(ctx, handlerCtx context.Context, workerID int) {
	slog.Debug("Starting worker loop", "worker_id", workerID)
	// instanceID prefix keeps the label unique across engine processes; the
	// loop itself is synchronous (claim → handle → ack), so one label per
	// worker can never have two attempts in flight.
	workerLabel := fmt.Sprintf("%s:worker-%d", e.instanceID, workerID)

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
		act, err := e.queue.Dequeue(ctx, workerDequeueBlock, workerLabel)
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
			continue
		}

		e.processActivity(handlerCtx, act, workerLabel, workerID)
	}

	slog.Debug("Worker loop stopped", "worker_id", workerID)
}

// runSuspendDispatcher is the SuspendOnAwait equivalent of runWorkerLoop.
// One dispatcher goroutine acquires a slot from sem, dequeues, and hands the
// activity off to a per-activity goroutine that owns the slot via a
// slotHolder. The slotHolder is installed in the activity's context so that
// ActivityFuture.GetResult inside the handler can release the slot for the
// duration of the wait — that's the whole point of SuspendOnAwait.
//
// Two dispatchers can run concurrently against different (queue, sem) pairs
// to implement leaf-slot reservation; they share the engine's activityWg so
// shutdown waits for both.
// ctx is the intake context (cancelled first during shutdown); handlerCtx is
// the engine context activities execute on, which outlives intake so draining
// handlers can still ack — see runWorkerLoop.
func (e *WorkerEngine) runSuspendDispatcher(ctx, handlerCtx context.Context, q activityQueue, sem chan struct{}, label string) {
	slog.Debug("Starting suspend dispatcher", "label", label, "slots", cap(sem))
	baseLabel := fmt.Sprintf("%s:%s", e.instanceID, label)

	for e.running.Load() {
		// Acquire a slot before dequeueing — keeps in-flight activities
		// bounded by the semaphore capacity even under bursty load.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			slog.Debug("Suspend dispatcher stopped (context)", "label", label)
			return
		}

		// Unlike runWorkerLoop, this dispatcher has many attempts in flight at
		// once, so the claim label must be unique per attempt: if every claim
		// shared one label, a stale goroutine whose lease expired could ack a
		// row this same dispatcher had since reclaimed for a fresh attempt.
		claimLabel := fmt.Sprintf("%s#%d", baseLabel, e.claimSeq.Add(1))

		// Short block (see dispatcherDequeueBlock): we hold a semaphore slot
		// during the wait, and an empty return releases it below so waking
		// parents can reacquire. No idle sleep — the block IS the idle wait.
		act, err := q.Dequeue(ctx, dispatcherDequeueBlock, claimLabel)
		if err != nil {
			<-sem
			if ctx.Err() != nil {
				return
			}
			slog.Error("Suspend dispatcher dequeue failed", "label", label, "error", err)
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return
			}
			continue
		}

		if act == nil {
			<-sem
			continue
		}

		holder := newSlotHolder(sem)
		e.activityWg.Add(1)
		go func(act *activity) {
			defer e.activityWg.Done()
			defer holder.release() // safety net — release() is idempotent

			// MUST pass the same label that was sent to Dequeue above. The
			// AckSuccess/AckFailure SQL guards on `current_worker_id = $2`,
			// which was set to the claim label at dequeue time — acking with
			// any other label fails the WHERE clause and the activity is
			// requeued and re-run.
			ctxWithSlot := withSuspendSlot(handlerCtx, holder)
			e.processActivity(ctxWithSlot, act, claimLabel, 0)
		}(act)
	}
	slog.Debug("Suspend dispatcher stopped", "label", label)
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

	scopedExecutor := newWorkerEngineWrapperWithDepth(e.queue, e.config.MaxActivityDepth).scopedForChild(act)

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
		queue:            e.queue,
	}

	payloadForDL := make(json.RawMessage, len(act.Payload))
	copy(payloadForDL, act.Payload)

	result, handlerErr := e.safeHandle(handler, actCtx, act.Payload)

	// A yielding durable Sleep is not a failure: park the activity until its
	// wake time without consuming a retry. Checked before the timeout so a
	// yield that raced the deadline is still honored as a yield.
	var ys *yieldSleep
	if errors.As(handlerErr, &ys) {
		e.handleYield(ctx, act, ys, workerLabel, workerID, activityID, activityType)
		return
	}

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
	// Result is persisted in the same transaction as the status flip: a crash
	// here either leaves the row 'processing' (lease expires, activity reruns)
	// or 'completed' with its result row present. There is no window where the
	// activity is completed but the result is missing, which previously left
	// awaiting parents polling until their own timeout.
	if err := e.queue.MarkCompleted(ctx, act, result, workerLabel); err != nil {
		// MarkCompleted failed — the row was no longer in 'processing' with
		// this worker (lease likely expired and the reaper requeued it).
		// Don't claim success: another worker will rerun the activity.
		slog.Warn("Activity completed in handler but row was no longer claimable; another worker will rerun it",
			"worker_id", workerID, "activity_id", activityID, "activity_type", activityType, "error", err)
		e.metrics.IncCounter("activity_lost_completion", 1)
		return
	}
	e.metrics.IncCounter("activity_completed", 1)
	slog.Info("Activity completed successfully", "worker_id", workerID, "activity_id", activityID, "activity_type", activityType)
}

// handleYield parks a sleeping activity as scheduled until its wake time.
// On failure the row simply stays processing: its lease expires, the reaper
// requeues it, and the handler replays to the same Sleep — degraded latency
// and one consumed retry, not lost work.
func (e *WorkerEngine) handleYield(ctx context.Context, act *activity, ys *yieldSleep, workerLabel string, workerID int, activityID any, activityType string) {
	if err := e.queue.Yield(ctx, act, ys.wakeAt, workerLabel); err != nil {
		slog.Error("Failed to park yielding activity; lease expiry will recover it",
			"worker_id", workerID, "activity_id", activityID, "activity_type", activityType,
			"wake_at", ys.wakeAt, "error", err)
		return
	}
	e.metrics.IncCounter("activity_yielded", 1)
	slog.Debug("Activity yielded for durable sleep",
		"worker_id", workerID, "activity_id", activityID, "activity_type", activityType,
		"step", ys.step, "wake_at", ys.wakeAt)
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
			ActivityExecutor: newWorkerEngineWrapperWithDepth(e.queue, e.config.MaxActivityDepth).scopedForChild(act),
			ParentActivityID: act.ParentActivityID,
			RootActivityID:   act.RootActivityID,
			Depth:            act.Depth,
		}
		handler.OnDeadLetter(dlCtx, payloadForDL, reason)
	}
}

func (e *WorkerEngine) handleNonRetryableFailure(ctx context.Context, act *activity, reason string, workerLabel string, workerID int, activityID any, activityType string) {
	e.metrics.IncCounter("activity_failed_non_retry", 1)
	slog.Error("Activity failed", "worker_id", workerID, "activity_id", activityID, "activity_type", activityType, "reason", reason)

	// The error result row is written inside the AckFailure transaction by the
	// backend, so no separate result-storage step is needed here.
	if _, err := e.queue.MarkFailed(ctx, act, reason, false, workerLabel); err != nil {
		slog.Error("Failed to mark activity as failed", "worker_id", workerID, "activity_id", activityID, "error", err)
	}
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
			ActivityExecutor: newWorkerEngineWrapperWithDepth(e.queue, e.config.MaxActivityDepth).scopedForChild(act),
			ParentActivityID: act.ParentActivityID,
			RootActivityID:   act.RootActivityID,
			Depth:            act.Depth,
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
	queueName        *string
	maxWorkers       *int
	pollInterval     *time.Duration
	metrics          MetricsSink
	backend          storage.Storage
	activityTypes    []string
	suspendOnAwait   bool
	suspendLeafTypes []string
	suspendLeavesRes int
	shutdownGrace    *time.Duration
	retention        *RetentionConfig
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

// SuspendOnAwait toggles the semaphore-based dispatcher that releases a
// parent activity's worker slot while it waits for child futures. Default
// false (existing fixed-goroutine pool). See WorkerConfig.SuspendOnAwait for
// the operational notes.
func (b *WorkerEngineBuilder) SuspendOnAwait(enabled bool) *WorkerEngineBuilder {
	b.suspendOnAwait = enabled
	return b
}

// ReserveSlotsForLeaves reserves `n` slots out of MaxWorkers for the listed
// "leaf" activity types when SuspendOnAwait is on. Prevents the wake-up
// deadlock where every freed slot is immediately retaken by a waking parent
// and no leaf can run. No effect when SuspendOnAwait is off.
func (b *WorkerEngineBuilder) ReserveSlotsForLeaves(n int, leafTypes []string) *WorkerEngineBuilder {
	b.suspendLeavesRes = n
	b.suspendLeafTypes = leafTypes
	return b
}

// ShutdownGrace bounds the entire shutdown drain — workers, dispatchers,
// in-flight activity goroutines, result-storage goroutines, and pool
// deregistration all run in parallel under this single budget. When the
// budget expires Start() returns even if some goroutines remain in flight.
// Defaults to 30s when not set; clamped to >= 1s.
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
		SuspendOnAwait:              b.suspendOnAwait,
		SuspendLeafActivityTypes:    b.suspendLeafTypes,
		SuspendLeavesReserved:       b.suspendLeavesRes,
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
