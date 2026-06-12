package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alob-mtc/runnerq-go/storage"
)

// statsCacheTTL bounds how often Stats() actually hits the database.
// Stats() runs ~14 COUNT subqueries per call and is polled by every
// connected SSE client; without this the dashboard amplifies into real
// load on the activities table.
const statsCacheTTL = time.Second

// workerPoolLivenessWindow is how stale a worker_pools row can be before
// Stats() stops counting it toward cluster-wide max-workers. Engines should
// heartbeat at roughly 1/6 this interval so a single missed heartbeat doesn't
// drop them out.
const workerPoolLivenessWindow = 60 * time.Second

// PostgresBackend is a PostgreSQL-based storage backend for RunnerQ.
type PostgresBackend struct {
	pool           *pgxpool.Pool
	queueName      string
	defaultLeaseMS int64

	// sig batches post-commit wake-up signals; watch is the lazily-started
	// shared listener that turns them into in-process wake-ups. See signals.go.
	sig       *signaler
	watcherMu sync.Mutex
	watch     *watcher

	statsMu     sync.Mutex
	statsCache  *storage.QueueStats
	statsExpiry time.Time
}

// New creates a new PostgresBackend with default pool size 25 and lease 30s.
// In multi-process deployments the operator should still pick a size that
// matches the worker count via WithConfig (rule of thumb: maxWorkers + ~5
// for reaper/stats/scheduled-processor headroom).
func New(ctx context.Context, databaseURL, queueName string) (*PostgresBackend, error) {
	return WithConfig(ctx, databaseURL, queueName, 30_000, 25)
}

// WithConfig creates a new PostgresBackend with custom configuration.
func WithConfig(ctx context.Context, databaseURL, queueName string, defaultLeaseMS int64, poolSize int32) (*PostgresBackend, error) {
	if err := validateQueueName(queueName); err != nil {
		return nil, err
	}

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, storage.NewUnavailableError(fmt.Sprintf("Failed to parse PostgreSQL URL: %v", err))
	}
	config.MaxConns = poolSize

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, storage.NewUnavailableError(fmt.Sprintf("Failed to connect to PostgreSQL: %v", err))
	}

	backend := &PostgresBackend{
		pool:           pool,
		queueName:      queueName,
		defaultLeaseMS: defaultLeaseMS,
	}

	if err := backend.initSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	backend.sig = newSignaler(backend)

	return backend, nil
}

// Close stops the signal machinery and closes the connection pool.
func (b *PostgresBackend) Close() {
	b.watcherMu.Lock()
	w := b.watch
	b.watch = nil
	b.watcherMu.Unlock()
	if w != nil {
		w.stop()
	}
	if b.sig != nil {
		b.sig.stop()
	}
	b.pool.Close()
}

// SetLeaseMS updates the default lease duration. Implements storage.LeaseConfigurer.
func (b *PostgresBackend) SetLeaseMS(leaseMS int64) {
	b.defaultLeaseMS = leaseMS
}

func validateQueueName(name string) error {
	if name == "" {
		return storage.NewConfigurationError("queue_name cannot be empty")
	}
	if len(name) > 48 {
		return storage.NewConfigurationError(fmt.Sprintf("queue_name '%s' is too long (max 48 characters)", name))
	}
	runes := []rune(name)
	first := runes[0]
	if !unicode.IsLetter(first) && first != '_' {
		return storage.NewConfigurationError(fmt.Sprintf("queue_name '%s' must start with a letter or underscore", name))
	}
	for _, c := range runes[1:] {
		if !unicode.IsLetter(c) && !unicode.IsDigit(c) && c != '_' {
			return storage.NewConfigurationError(fmt.Sprintf("queue_name '%s' contains invalid character '%c'; only letters, digits, and underscores are allowed", name, c))
		}
	}
	return nil
}

func (b *PostgresBackend) initSchema(ctx context.Context) error {
	_, err := b.pool.Exec(ctx, schemaSql)
	if err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to initialize schema: %v", err))
	}
	return nil
}

func priorityToInt(p storage.ActivityPriority) int32 {
	return int32(p)
}

func intToPriority(val int32) storage.ActivityPriority {
	if val <= int32(storage.PriorityLow) {
		return storage.PriorityLow
	}
	if val >= int32(storage.PriorityLow) && val <= int32(storage.PriorityCritical) {
		return storage.ActivityPriority(val)
	}
	return storage.PriorityNormal
}

// recordEvent inserts the lifecycle event row — and ONLY the row. It must
// never NOTIFY: this runs inside the hot-path transactions, and Postgres
// serializes the commit of every NOTIFY-carrying transaction on one global
// lock, capping cluster-wide throughput. Live consumers are fed by the
// post-commit signaler + event tailer instead (signals.go); callers emit
// b.signalEvent() after their transaction commits.
func (b *PostgresBackend) recordEvent(ctx context.Context, tx pgx.Tx, activityID uuid.UUID, eventType string, workerID *string, detail json.RawMessage) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO runnerq_events (activity_id, queue_name, event_type, worker_id, detail, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		activityID, b.queueName, eventType, workerID, detail, time.Now().UTC())
	if err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to record event: %v", err))
	}
	return nil
}

func (b *PostgresBackend) storeResultTx(ctx context.Context, tx pgx.Tx, activityID uuid.UUID, result *storage.ActivityResult, now time.Time) error {
	stateStr := "Ok"
	if result.State == storage.ResultErr {
		stateStr = "Err"
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO runnerq_results (activity_id, queue_name, state, data, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (activity_id) DO UPDATE
		SET state = $3, data = $4, created_at = $5`,
		activityID, b.queueName, stateStr, result.Data, now)
	if err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to store result: %v", err))
	}
	return nil
}

func toDetail(kv map[string]any) json.RawMessage {
	data, _ := json.Marshal(kv)
	return data
}

func strPtr(s string) *string { return &s }

// ============================================================================
// QueueStorage Implementation
// ============================================================================

func (b *PostgresBackend) Enqueue(ctx context.Context, a storage.QueuedActivity) error {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	if err := b.enqueueInTx(ctx, tx, &a); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to commit enqueue: %v", err))
	}

	b.signalEnqueued(&a)
	return nil
}

// signalEnqueued emits the post-commit signals for a newly enqueued activity:
// the event edge always, and the work edge when the activity is immediately
// runnable (future-scheduled rows are picked up by the blocked dequeuers'
// periodic probes when they come due).
func (b *PostgresBackend) signalEnqueued(a *storage.QueuedActivity) {
	b.signalEvent()
	if a.ScheduledAt == nil || !a.ScheduledAt.After(time.Now().UTC()) {
		b.signalWork()
	}
}

// enqueueInTx inserts the activity row and its lifecycle event inside the
// caller's transaction, so callers can make the enqueue atomic with other
// writes (e.g. the idempotency-key claim in EnqueueIdempotent).
func (b *PostgresBackend) enqueueInTx(ctx context.Context, tx pgx.Tx, a *storage.QueuedActivity) error {
	status := "pending"
	if a.ScheduledAt != nil {
		status = "scheduled"
	}

	metadataJSON, err := json.Marshal(a.Metadata)
	if err != nil {
		return storage.NewSerializationError(err.Error())
	}

	var idempotencyKey *string
	if a.IdempotencyKey != nil {
		idempotencyKey = &a.IdempotencyKey.Key
	}

	rootID := a.RootActivityID
	if rootID == (uuid.UUID{}) {
		rootID = a.ID
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO runnerq_activities (
			id, queue_name, activity_type, payload, priority, status,
			created_at, scheduled_at, retry_count, max_retries,
			timeout_seconds, retry_delay_seconds, max_retry_delay_seconds,
			metadata, idempotency_key,
			parent_activity_id, root_activity_id, depth
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`,
		a.ID, b.queueName, a.ActivityType, a.Payload,
		priorityToInt(a.Priority), status, a.CreatedAt, a.ScheduledAt,
		int32(a.RetryCount), int32(a.MaxRetries),
		int64(a.TimeoutSeconds), int64(a.RetryDelaySeconds),
		int64(a.MaxRetryDelaySeconds),
		metadataJSON, idempotencyKey,
		a.ParentActivityID, rootID, int16(a.Depth))
	if err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to enqueue activity: %v", err))
	}

	var eventType string
	var detail json.RawMessage
	if a.ScheduledAt != nil {
		eventType = storage.EventScheduled
		detail = toDetail(map[string]any{"scheduled_at": a.ScheduledAt})
	} else {
		eventType = storage.EventEnqueued
		detail = toDetail(map[string]any{
			"priority":     fmt.Sprintf("%d", a.Priority),
			"scheduled_at": a.ScheduledAt,
		})
	}

	return b.recordEvent(ctx, tx, a.ID, eventType, nil, detail)
}

// Dequeue claim SQL comes in three static forms so the planner can pick the
// right index for each instead of compromising on one plan for an
// `($x IS NULL OR activity_type = ANY($x))` OR-pattern:
//
//   - no type filter  → idx_runnerq_dequeue_order (key order == ORDER BY)
//   - one type        → idx_runnerq_dequeue_effective (type pinned by equality,
//     remaining key order == ORDER BY)
//   - multiple types  → idx_runnerq_dequeue_order with an in-scan type filter
//
// In all three cases the claim is an ordered index walk that stops at the
// first unlocked qualifying row — never a sort over the whole backlog.
//
// Lease arithmetic uses the DATABASE clock (NOW()), never the caller's: the
// deadline set here is compared against NOW() in RequeueExpired, so with app
// clocks a fast-clocked reaper node would systematically reclaim other nodes'
// live leases and double-execute their activities. The deadline derives from
// the row's per-activity timeout, clamped to at least the engine default
// lease, folded into the single claim UPDATE.
const (
	// The eligibility predicate is written as
	//   status IN (...) AND (status = 'pending' OR scheduled_at <= NOW())
	// rather than the equivalent
	//   status = 'pending' OR (status IN ('scheduled','retrying') AND scheduled_at <= NOW())
	// deliberately: the first conjunct textually matches the partial-index
	// predicate, which Postgres's predicate prover needs in order to offer an
	// ORDERED scan of the partial index. With the OR-only form the planner
	// can still use the index for bitmap scans, but not for ordered scans —
	// forcing a top-1 sort over the whole eligible backlog on every claim
	// (verified with EXPLAIN ANALYZE on a 200k-row backlog: external merge
	// sort vs a 0.2ms index walk).
	dequeueSQLHead = `
		UPDATE runnerq_activities
		SET status = 'processing',
			current_worker_id = $1,
			started_at = NOW(),
			lease_deadline_ms = (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint + GREATEST($2::bigint, (timeout_seconds + 10) * 1000)
		WHERE id = (
			SELECT id FROM runnerq_activities
			WHERE queue_name = $3
			  AND status IN ('pending', 'scheduled', 'retrying')
			  AND (status = 'pending' OR scheduled_at <= NOW())`
	dequeueSQLTail = `
			ORDER BY
				priority DESC,
				retry_count DESC,
				COALESCE(scheduled_at, created_at) ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, activity_type, payload, priority, retry_count, max_retries,
			timeout_seconds, retry_delay_seconds, max_retry_delay_seconds,
			scheduled_at, metadata, idempotency_key, created_at,
			parent_activity_id, root_activity_id, depth, lease_deadline_ms`

	dequeueSQLAllTypes  = dequeueSQLHead + dequeueSQLTail
	dequeueSQLOneType   = dequeueSQLHead + ` AND activity_type = $4` + dequeueSQLTail
	dequeueSQLManyTypes = dequeueSQLHead + ` AND activity_type = ANY($4)` + dequeueSQLTail
)

// Dequeue claims the next runnable activity. When the queue is empty and
// maxBlock > 0, it parks until new work is signalled (LISTEN/NOTIFY via the
// shared watcher), re-probing every workWaitProbe as a fallback for lost
// signals and for scheduled/retrying rows coming due, up to maxBlock. Returns
// (nil, nil) when nothing became claimable in time.
func (b *PostgresBackend) Dequeue(ctx context.Context, workerID string, maxBlock time.Duration, activityTypes []string) (*storage.QueuedActivity, error) {
	deadline := time.Now().Add(maxBlock)

	a, err := b.dequeueOnce(ctx, workerID, activityTypes)
	if a != nil || err != nil || maxBlock <= 0 {
		return a, err
	}

	w := b.getWatcher()
	ch := w.registerWork()
	defer w.unregisterWork(ch)

	for {
		// Re-probe after registering so a signal emitted between the previous
		// probe and registration can't be missed.
		a, err := b.dequeueOnce(ctx, workerID, activityTypes)
		if a != nil || err != nil {
			return a, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
		case <-time.After(min(remaining, workWaitProbe)):
		}
	}
}

func (b *PostgresBackend) dequeueOnce(ctx context.Context, workerID string, activityTypes []string) (*storage.QueuedActivity, error) {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	var row pgx.Row
	switch len(activityTypes) {
	case 0:
		row = tx.QueryRow(ctx, dequeueSQLAllTypes, workerID, b.defaultLeaseMS, b.queueName)
	case 1:
		row = tx.QueryRow(ctx, dequeueSQLOneType, workerID, b.defaultLeaseMS, b.queueName, activityTypes[0])
	default:
		row = tx.QueryRow(ctx, dequeueSQLManyTypes, workerID, b.defaultLeaseMS, b.queueName, activityTypes)
	}

	var ar activityRow
	var leaseDeadlineMS int64
	err = row.Scan(
		&ar.id, &ar.activityType, &ar.payload, &ar.priority,
		&ar.retryCount, &ar.maxRetries, &ar.timeoutSeconds,
		&ar.retryDelaySeconds, &ar.maxRetryDelaySeconds,
		&ar.scheduledAt, &ar.metadata,
		&ar.idempotencyKey, &ar.createdAt,
		&ar.parentActivityID, &ar.rootActivityID, &ar.depth,
		&leaseDeadlineMS)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to dequeue: %v", err))
	}

	a := ar.toQueuedActivity()

	detail := toDetail(map[string]any{"lease_deadline_ms": leaseDeadlineMS})
	if err := b.recordEvent(ctx, tx, a.ID, storage.EventDequeued, &workerID, detail); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to commit dequeue: %v", err))
	}

	b.signalEvent()

	slog.Debug("Activity claimed",
		"activity_id", a.ID,
		"activity_type", a.ActivityType,
		"priority", a.Priority)

	return a, nil
}

func (b *PostgresBackend) AckSuccess(ctx context.Context, activityID uuid.UUID, result json.RawMessage, workerID string) error {
	now := time.Now().UTC()

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	var actType *string
	err = tx.QueryRow(ctx, `
		UPDATE runnerq_activities
		SET status = 'completed',
			completed_at = $1,
			last_worker_id = $2,
			current_worker_id = NULL,
			lease_deadline_ms = NULL
		WHERE id = $3 AND queue_name = $4
		  AND status = 'processing'
		  AND current_worker_id = $2
		RETURNING activity_type`,
		now, workerID, activityID, b.queueName).Scan(&actType)
	if err != nil {
		if err == pgx.ErrNoRows {
			return storage.NewNotFoundError(fmt.Sprintf("Activity %s is not in a claimable state for ack_success", activityID))
		}
		return storage.NewInternalError(fmt.Sprintf("Failed to ack success: %v", err))
	}

	// Always persist a result row, even when result is nil: awaiting parents
	// poll runnerq_results for the row's existence, so a completed activity
	// without one would hang them until their own timeout. Storing it in this
	// transaction means completion and result are atomic — a crash can't leave
	// a completed activity with no result.
	ar := &storage.ActivityResult{Data: result, State: storage.ResultOk}
	if err := b.storeResultTx(ctx, tx, activityID, ar, now); err != nil {
		return err
	}

	actTypeStr := ""
	if actType != nil {
		actTypeStr = *actType
	}
	detailMap := map[string]any{"activity_type": actTypeStr, "result_stored": true}

	if err := b.recordEvent(ctx, tx, activityID, storage.EventCompleted, &workerID, toDetail(detailMap)); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to commit ack: %v", err))
	}

	b.signalEvent()
	b.signalResult(activityID)
	return nil
}

func (b *PostgresBackend) AckFailure(ctx context.Context, activityID uuid.UUID, failure storage.FailureKind, workerID string) (bool, error) {
	now := time.Now().UTC()

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return false, storage.NewInternalError(fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	// Lock the activity row
	var ar activityRow
	err = tx.QueryRow(ctx, `
		SELECT id, retry_count, max_retries, retry_delay_seconds, max_retry_delay_seconds
		FROM runnerq_activities
		WHERE id = $1 AND queue_name = $2 AND status = 'processing' AND current_worker_id = $3
		FOR UPDATE`,
		activityID, b.queueName, workerID).Scan(
		&ar.id, &ar.retryCount, &ar.maxRetries, &ar.retryDelaySeconds, &ar.maxRetryDelaySeconds)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, storage.NewNotFoundError(fmt.Sprintf("Activity %s not found or not claimed by this worker", activityID))
		}
		return false, storage.NewInternalError(fmt.Sprintf("Failed to lock activity: %v", err))
	}

	errorMessage := failure.Reason

	// Non-retryable: go straight to failed
	if !failure.Retryable {
		_, err = tx.Exec(ctx, `
			UPDATE runnerq_activities
			SET status = 'failed',
				completed_at = $1,
				last_error = $2,
				last_error_at = $3,
				last_worker_id = $4,
				current_worker_id = NULL,
				lease_deadline_ms = NULL
			WHERE id = $5 AND queue_name = $6 AND status = 'processing' AND current_worker_id = $4`,
			now, errorMessage, now, workerID, activityID, b.queueName)
		if err != nil {
			return false, storage.NewInternalError(fmt.Sprintf("Failed to mark as failed: %v", err))
		}

		res := &storage.ActivityResult{Data: toDetail(map[string]any{
			"error":     errorMessage,
			"type":      "non_retryable",
			"failed_at": now.Format(time.RFC3339),
		}), State: storage.ResultErr}
		if err := b.storeResultTx(ctx, tx, activityID, res, now); err != nil {
			return false, err
		}
		if err := b.recordEvent(ctx, tx, activityID, storage.EventFailed, &workerID,
			toDetail(map[string]any{"retryable": false, "error": errorMessage})); err != nil {
			return false, err
		}

		if err := tx.Commit(ctx); err != nil {
			return false, storage.NewInternalError(fmt.Sprintf("Failed to commit ack_failure: %v", err))
		}
		b.signalEvent()
		b.signalResult(activityID)
		return false, nil
	}

	// Retryable: check if we can retry
	canRetry := ar.maxRetries == 0 || (ar.retryCount+1) < ar.maxRetries

	if canRetry {
		const overflowCap = int64(math.MaxInt64 / int64(time.Second))
		baseDelay := int64(ar.retryDelaySeconds)
		shift := min(ar.retryCount+1, 62)
		backoffMult := int64(1) << shift
		retryDelay := overflowCap
		if baseDelay > 0 && baseDelay <= overflowCap/backoffMult {
			retryDelay = baseDelay * backoffMult
		} else if baseDelay <= 0 {
			retryDelay = 0
		}
		// Apply per-activity max retry delay cap (0 = use default 3600s).
		maxCap := ar.maxRetryDelaySeconds
		if maxCap <= 0 {
			maxCap = 3600
		}
		if retryDelay > maxCap {
			retryDelay = maxCap
		}
		// Estimate for the event detail only — the authoritative scheduled_at
		// is computed from the database clock in the UPDATE below, so retry
		// timing is immune to app-clock skew (dequeue compares it to NOW()).
		scheduledAt := now.Add(time.Duration(retryDelay) * time.Second)
		newRetryCount := ar.retryCount + 1

		// started_at reset to NULL alongside the flip to 'retrying' for the
		// same reason as RequeueExpired: started_at carries "when did the
		// current/last attempt start", and the failed attempt is no longer
		// current. The next Dequeue (when scheduled_at fires) writes a
		// fresh started_at for the new attempt. last_error_at preserves
		// the failed-attempt timestamp for the runs-list sort.
		_, err = tx.Exec(ctx, `
			UPDATE runnerq_activities
			SET status = 'retrying',
				retry_count = retry_count + 1,
				scheduled_at = NOW() + make_interval(secs => $1),
				last_error = $2,
				last_error_at = $3,
				last_worker_id = $4,
				current_worker_id = NULL,
				lease_deadline_ms = NULL,
				started_at = NULL
			WHERE id = $5 AND queue_name = $6 AND status = 'processing' AND current_worker_id = $4`,
			retryDelay, errorMessage, now, workerID, activityID, b.queueName)
		if err != nil {
			return false, storage.NewInternalError(fmt.Sprintf("Failed to schedule retry: %v", err))
		}

		if err := b.recordEvent(ctx, tx, activityID, storage.EventRetrying, &workerID,
			toDetail(map[string]any{
				"retry_count":  newRetryCount,
				"scheduled_at": scheduledAt.Format(time.RFC3339),
				"error":        errorMessage,
			})); err != nil {
			return false, err
		}

		if err := tx.Commit(ctx); err != nil {
			return false, storage.NewInternalError(fmt.Sprintf("Failed to commit ack_failure retry: %v", err))
		}
		b.signalEvent()
		if retryDelay == 0 {
			// Immediate retry is runnable right now; delayed retries are
			// picked up by blocked dequeuers' periodic probes when due.
			b.signalWork()
		}
		return false, nil
	}

	// Dead letter queue - exhausted all retries
	_, err = tx.Exec(ctx, `
		UPDATE runnerq_activities
		SET status = 'dead_letter',
			completed_at = $1,
			last_error = $2,
			last_error_at = $3,
			last_worker_id = $4,
			current_worker_id = NULL,
			lease_deadline_ms = NULL
		WHERE id = $5 AND queue_name = $6 AND status = 'processing' AND current_worker_id = $4`,
		now, errorMessage, now, workerID, activityID, b.queueName)
	if err != nil {
		return false, storage.NewInternalError(fmt.Sprintf("Failed to move to DLQ: %v", err))
	}

	res := &storage.ActivityResult{Data: toDetail(map[string]any{
		"error":     errorMessage,
		"type":      "dead_letter",
		"failed_at": now.Format(time.RFC3339),
	}), State: storage.ResultErr}
	if err := b.storeResultTx(ctx, tx, activityID, res, now); err != nil {
		return false, err
	}
	if err := b.recordEvent(ctx, tx, activityID, storage.EventDeadLetter, &workerID,
		toDetail(map[string]any{"error": errorMessage})); err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, storage.NewInternalError(fmt.Sprintf("Failed to commit ack_failure DLQ: %v", err))
	}

	b.signalEvent()
	b.signalResult(activityID)
	return true, nil
}

func (b *PostgresBackend) ProcessScheduled(_ context.Context) (uint64, error) {
	return 0, nil
}

func (b *PostgresBackend) SchedulesNatively() bool {
	return true
}

func (b *PostgresBackend) RequeueExpired(ctx context.Context, batchSize int) (uint64, error) {
	now := time.Now().UTC()
	const leaseExpiredError = "lease expired before completion; worker presumed crashed or wedged"

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return 0, storage.NewInternalError(fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	// A lease expiry counts as a failed attempt: retry_count is incremented
	// and, when retries are exhausted (same rule as AckFailure's canRetry),
	// the row goes to dead_letter instead of pending. Without this, an
	// activity whose handler crashes the whole process is reclaimed and
	// re-crashes workers forever without ever reaching the DLQ.
	//
	// started_at is reset to NULL alongside the status flip back to pending
	// because it carries "when did the current/last attempt start". Leaving
	// the previous attempt's value in place makes pending rows look like
	// they're already running. The next Dequeue will populate started_at
	// fresh for the new attempt.
	rows, err := tx.Query(ctx, `
		UPDATE runnerq_activities
		SET retry_count = retry_count + 1,
			status = CASE WHEN max_retries > 0 AND retry_count + 1 >= max_retries
				THEN 'dead_letter' ELSE 'pending' END,
			completed_at = CASE WHEN max_retries > 0 AND retry_count + 1 >= max_retries
				THEN $3::timestamptz ELSE completed_at END,
			last_error = $4,
			last_error_at = $3,
			current_worker_id = NULL,
			lease_deadline_ms = NULL,
			started_at = NULL
		WHERE id IN (
			SELECT id FROM runnerq_activities
			WHERE queue_name = $1
			  AND status = 'processing'
			  AND lease_deadline_ms < (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, status, retry_count`,
		b.queueName, batchSize, now, leaseExpiredError)
	if err != nil {
		return 0, storage.NewInternalError(fmt.Sprintf("Failed to requeue expired: %v", err))
	}

	type reapedRow struct {
		id         uuid.UUID
		deadLetter bool
		retryCount int32
	}
	var reaped []reapedRow
	for rows.Next() {
		var r reapedRow
		var status string
		if err := rows.Scan(&r.id, &status, &r.retryCount); err != nil {
			rows.Close()
			return 0, storage.NewInternalError(fmt.Sprintf("Failed to scan requeued row: %v", err))
		}
		r.deadLetter = status == "dead_letter"
		reaped = append(reaped, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, storage.NewInternalError(fmt.Sprintf("Failed to read requeued rows: %v", err))
	}
	if len(reaped) == 0 {
		return 0, nil
	}

	for _, r := range reaped {
		if r.deadLetter {
			// Store an error result so any parent awaiting this activity
			// resolves instead of polling until its own timeout. Note the
			// engine-side OnDeadLetter hook does not fire for reaper
			// dead-letters — there is no live handler to call it on.
			res := &storage.ActivityResult{Data: toDetail(map[string]any{
				"error":     leaseExpiredError,
				"type":      "dead_letter",
				"failed_at": now.Format(time.RFC3339),
			}), State: storage.ResultErr}
			if err := b.storeResultTx(ctx, tx, r.id, res, now); err != nil {
				return 0, err
			}
			if err := b.recordEvent(ctx, tx, r.id, storage.EventDeadLetter, nil,
				toDetail(map[string]any{"error": leaseExpiredError, "reason": "lease_expired"})); err != nil {
				return 0, err
			}
			continue
		}
		if err := b.recordEvent(ctx, tx, r.id, storage.EventRequeued, nil,
			toDetail(map[string]any{"retry_count": r.retryCount, "reason": "lease_expired"})); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, storage.NewInternalError(fmt.Sprintf("Failed to commit requeue expired: %v", err))
	}

	b.signalEvent()
	for _, r := range reaped {
		if r.deadLetter {
			b.signalResult(r.id)
		}
	}
	if len(reaped) > 0 {
		b.signalWork() // requeued rows are immediately runnable
	}
	return uint64(len(reaped)), nil
}

func (b *PostgresBackend) ExtendLease(ctx context.Context, activityID uuid.UUID, extendBy time.Duration) (bool, error) {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return false, storage.NewInternalError(fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	// Deadline computed from the database clock — see Dequeue for why lease
	// arithmetic must never use the caller's clock.
	var newDeadlineMS int64
	err = tx.QueryRow(ctx, `
		UPDATE runnerq_activities
		SET lease_deadline_ms = (EXTRACT(EPOCH FROM NOW()) * 1000)::bigint + $1
		WHERE id = $2 AND queue_name = $3 AND status = 'processing'
		RETURNING lease_deadline_ms`,
		extendBy.Milliseconds(), activityID, b.queueName).Scan(&newDeadlineMS)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}
		return false, storage.NewInternalError(fmt.Sprintf("Failed to extend lease: %v", err))
	}

	if err := b.recordEvent(ctx, tx, activityID, storage.EventLeaseExtended, nil,
		toDetail(map[string]any{
			"new_deadline_ms": newDeadlineMS,
			"extend_by_ms":    extendBy.Milliseconds(),
		})); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, storage.NewInternalError(fmt.Sprintf("Failed to commit extend_lease: %v", err))
	}
	b.signalEvent()
	return true, nil
}

func (b *PostgresBackend) RecordSpawnLinked(ctx context.Context, childID, parentID uuid.UUID) error {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	detail := toDetail(map[string]any{"parent_activity_id": parentID})
	if err := b.recordEvent(ctx, tx, childID, storage.EventSpawnLinked, nil, detail); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to commit spawn_linked: %v", err))
	}
	b.signalEvent()
	return nil
}

func (b *PostgresBackend) StoreResult(ctx context.Context, activityID uuid.UUID, result storage.ActivityResult) error {
	stateStr := "Ok"
	if result.State == storage.ResultErr {
		stateStr = "Err"
	}

	slog.Debug("Storing activity result",
		"activity_id", activityID,
		"state", stateStr,
		"has_data", result.Data != nil)

	now := time.Now().UTC()
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	if err := b.storeResultTx(ctx, tx, activityID, &result, now); err != nil {
		return err
	}
	if err := b.recordEvent(ctx, tx, activityID, storage.EventResultStored, nil,
		toDetail(map[string]any{"result_stored": true, "state": stateStr})); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to commit store_result: %v", err))
	}

	b.signalEvent()
	b.signalResult(activityID)

	slog.Debug("Activity result stored", "activity_id", activityID)
	return nil
}

func (b *PostgresBackend) GetResult(ctx context.Context, activityID uuid.UUID) (*storage.ActivityResult, error) {
	var stateStr string
	var data json.RawMessage

	err := b.pool.QueryRow(ctx, `SELECT state, data FROM runnerq_results WHERE activity_id = $1`, activityID).Scan(&stateStr, &data)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to get result: %v", err))
	}

	state := storage.ResultOk
	if stateStr != "Ok" {
		state = storage.ResultErr
	}

	return &storage.ActivityResult{Data: data, State: state}, nil
}

func (b *PostgresBackend) EnqueueIdempotent(ctx context.Context, a *storage.QueuedActivity) (*storage.IdempotencyResult, error) {
	if a.IdempotencyKey == nil {
		return nil, b.Enqueue(ctx, *a)
	}
	key := a.IdempotencyKey.Key
	behavior := a.IdempotencyKey.Behavior

	const maxAttempts = 3
	for range maxAttempts {
		result, done, err := b.tryEnqueueIdempotent(ctx, a, key, behavior)
		if err != nil {
			return nil, err
		}
		if done {
			return result, nil
		}
		// Key row vanished between our INSERT conflict and the locking
		// SELECT — retry from the top.
	}
	return nil, storage.NewInternalError(fmt.Sprintf("Failed to resolve idempotency key '%s' after %d attempts", key, maxAttempts))
}

// tryEnqueueIdempotent runs one claim-and-enqueue attempt in a single
// transaction. The key claim, behavior resolution, and activity insert all
// commit (or roll back) together — there is no state where the key points at
// an activity that was never enqueued. Returns done=false when the key row
// disappeared mid-attempt and the caller should retry.
func (b *PostgresBackend) tryEnqueueIdempotent(ctx context.Context, a *storage.QueuedActivity, key string, behavior storage.IdempotencyBehavior) (*storage.IdempotencyResult, bool, error) {
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return nil, false, storage.NewInternalError(fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		INSERT INTO runnerq_idempotency (queue_name, idempotency_key, activity_id, created_at, updated_at)
		VALUES ($1, $2, $3, NOW(), NOW())
		ON CONFLICT (queue_name, idempotency_key) DO NOTHING`,
		b.queueName, key, a.ID)
	if err != nil {
		return nil, false, storage.NewInternalError(fmt.Sprintf("Failed to claim idempotency key: %v", err))
	}

	if tag.RowsAffected() > 0 {
		// Fresh claim — enqueue in the same transaction.
		if err := b.enqueueInTx(ctx, tx, a); err != nil {
			return nil, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, false, storage.NewInternalError(fmt.Sprintf("Failed to commit idempotent enqueue: %v", err))
		}
		b.signalEnqueued(a)
		return nil, true, nil
	}

	// Key exists. Lock the key row so behavior resolution can't race a
	// concurrent claimer, then look up the activity it points at. FOR UPDATE
	// OF i is valid here because i is the non-nullable side of the join.
	var existingID uuid.UUID
	var existingParentID *uuid.UUID
	var status *string
	err = tx.QueryRow(ctx, `
		SELECT i.activity_id, a.status, a.parent_activity_id
		FROM runnerq_idempotency i
		LEFT JOIN runnerq_activities a ON i.activity_id = a.id
		WHERE i.queue_name = $1 AND i.idempotency_key = $2
		FOR UPDATE OF i`,
		b.queueName, key).Scan(&existingID, &status, &existingParentID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, false, nil
		}
		return nil, false, storage.NewInternalError(fmt.Sprintf("Failed to get existing key: %v", err))
	}

	// Repoint the key at our new activity and enqueue it, atomically. Used by
	// the reuse behaviors and for orphan repair.
	reclaim := func() (*storage.IdempotencyResult, bool, error) {
		if _, err := tx.Exec(ctx, `
			UPDATE runnerq_idempotency
			SET activity_id = $1, updated_at = NOW()
			WHERE queue_name = $2 AND idempotency_key = $3`,
			a.ID, b.queueName, key); err != nil {
			return nil, false, storage.NewInternalError(fmt.Sprintf("Failed to update key: %v", err))
		}
		if err := b.enqueueInTx(ctx, tx, a); err != nil {
			return nil, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, false, storage.NewInternalError(fmt.Sprintf("Failed to commit idempotent enqueue: %v", err))
		}
		b.signalEnqueued(a)
		return nil, true, nil
	}

	// status == nil means the key points at an activity row that doesn't
	// exist — an orphan left by an older version that claimed the key and
	// enqueued in separate transactions and crashed in between. The key is
	// reclaimable regardless of behavior; leaving it would brick the key
	// forever (ReturnExisting would hand out a future that never resolves).
	if status == nil {
		return reclaim()
	}

	switch behavior {
	case storage.BehaviorReturnExisting:
		return &storage.IdempotencyResult{ExistingID: existingID, ExistingParentID: existingParentID}, true, nil

	case storage.BehaviorAllowReuse:
		return reclaim()

	case storage.BehaviorAllowReuseOnFailure:
		if *status == "dead_letter" || *status == "failed" {
			return reclaim()
		}
		return nil, false, storage.NewIdempotencyConflictError(fmt.Sprintf("Idempotency key '%s' exists with status '%s'", key, *status))

	case storage.BehaviorNoReuse:
		return nil, false, storage.NewDuplicateActivityError(fmt.Sprintf("Activity with idempotency key '%s' already exists: %s", key, existingID))

	default:
		return reclaim()
	}
}

// ============================================================================
// InspectionStorage Implementation
// ============================================================================

func (b *PostgresBackend) Stats(ctx context.Context) (*storage.QueueStats, error) {
	b.statsMu.Lock()
	if b.statsCache != nil && time.Now().Before(b.statsExpiry) {
		s := *b.statsCache
		b.statsMu.Unlock()
		return &s, nil
	}
	b.statsMu.Unlock()

	// One round trip, but each scalar subquery is planned independently so the
	// matching partial index handles its own count. A single FILTER aggregate
	// over the whole table would force a full scan and degrade as completed
	// history accumulates — this form keeps each count cheap.
	stats := &storage.QueueStats{}
	err := b.pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM runnerq_activities WHERE queue_name = $1 AND status = 'pending')                                                                AS pending,
			(SELECT COUNT(*) FROM runnerq_activities WHERE queue_name = $1 AND status = 'processing')                                                             AS processing,
			(SELECT COUNT(*) FROM runnerq_activities WHERE queue_name = $1 AND status = 'scheduled')                                                              AS scheduled,
			(SELECT COUNT(*) FROM runnerq_activities WHERE queue_name = $1 AND status = 'retrying')                                                               AS retrying,
			(SELECT COUNT(*) FROM runnerq_activities WHERE queue_name = $1 AND status = 'failed')                                                                 AS failed,
			(SELECT COUNT(*) FROM runnerq_activities WHERE queue_name = $1 AND status = 'dead_letter')                                                            AS dead_letter,
			(SELECT COUNT(DISTINCT current_worker_id) FROM runnerq_activities WHERE queue_name = $1 AND status = 'processing' AND current_worker_id IS NOT NULL) AS active_workers,
			(SELECT COUNT(*) FROM runnerq_activities WHERE queue_name = $1 AND parent_activity_id IS NULL AND status = 'pending')     AS root_pending,
			(SELECT COUNT(*) FROM runnerq_activities WHERE queue_name = $1 AND parent_activity_id IS NULL AND status = 'processing')  AS root_processing,
			(SELECT COUNT(*) FROM runnerq_activities WHERE queue_name = $1 AND parent_activity_id IS NULL AND status = 'scheduled')   AS root_scheduled,
			(SELECT COUNT(*) FROM runnerq_activities WHERE queue_name = $1 AND parent_activity_id IS NULL AND status = 'retrying')    AS root_retrying,
			(SELECT COUNT(*) FROM runnerq_activities WHERE queue_name = $1 AND parent_activity_id IS NULL AND status = 'completed')   AS root_completed,
			(SELECT COUNT(*) FROM runnerq_activities WHERE queue_name = $1 AND parent_activity_id IS NULL AND status = 'failed')      AS root_failed,
			(SELECT COUNT(*) FROM runnerq_activities WHERE queue_name = $1 AND parent_activity_id IS NULL AND status = 'dead_letter') AS root_dead_letter`,
		b.queueName).Scan(
		&stats.Pending,
		&stats.Processing,
		&stats.Scheduled,
		&stats.Retrying,
		&stats.Failed,
		&stats.DeadLetter,
		&stats.ActiveWorkers,
		&stats.Roots.Pending,
		&stats.Roots.Processing,
		&stats.Roots.Scheduled,
		&stats.Roots.Retrying,
		&stats.Roots.Completed,
		&stats.Roots.Failed,
		&stats.Roots.DeadLetter,
	)
	if err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to compute queue stats: %v", err))
	}

	// Priority breakdown for pending activities — hits the dequeue partial index.
	pRows, err := b.pool.Query(ctx, `
		SELECT priority, COUNT(*) as count
		FROM runnerq_activities
		WHERE queue_name = $1 AND status = 'pending'
		GROUP BY priority`, b.queueName)
	if err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to get priority stats: %v", err))
	}
	defer pRows.Close()

	for pRows.Next() {
		var priority int32
		var count int64
		if err := pRows.Scan(&priority, &count); err != nil {
			return nil, storage.NewInternalError(fmt.Sprintf("Failed to scan priority stats: %v", err))
		}
		switch storage.ActivityPriority(priority) {
		case storage.PriorityCritical:
			stats.ByPriority.Critical = uint64(count)
		case storage.PriorityHigh:
			stats.ByPriority.High = uint64(count)
		case storage.PriorityNormal:
			stats.ByPriority.Normal = uint64(count)
		case storage.PriorityLow:
			stats.ByPriority.Low = uint64(count)
		}
	}

	// Cluster-wide max workers: sum max_workers across pools that have
	// heartbeated within workerPoolLivenessWindow. A pool that crashes
	// without deregistering drops out of the total once its row ages out.
	var totalMax int
	if err := b.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(max_workers), 0)::int
		FROM runnerq_worker_pools
		WHERE queue_name = $1
		  AND last_seen_at > NOW() - make_interval(secs => $2)`,
		b.queueName, int(workerPoolLivenessWindow.Seconds())).Scan(&totalMax); err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to compute cluster worker capacity: %v", err))
	}
	if totalMax > 0 {
		stats.MaxWorkers = &totalMax
	}

	b.statsMu.Lock()
	b.statsCache = stats
	b.statsExpiry = time.Now().Add(statsCacheTTL)
	b.statsMu.Unlock()

	return stats, nil
}

func (b *PostgresBackend) listByStatus(ctx context.Context, status string, offset, limit int) ([]storage.ActivitySnapshot, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT id, activity_type, payload, priority, status, created_at,
			scheduled_at, started_at, completed_at, current_worker_id,
			last_worker_id, retry_count, max_retries, timeout_seconds,
			retry_delay_seconds, last_error, last_error_at, metadata,
			idempotency_key, lease_deadline_ms,
			parent_activity_id, root_activity_id, depth
		FROM runnerq_activities
		WHERE queue_name = $1 AND status = $2
		ORDER BY priority DESC, retry_count DESC, created_at ASC
		LIMIT $3 OFFSET $4`,
		b.queueName, status, limit, offset)
	if err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to list %s: %v", status, err))
	}
	defer rows.Close()

	return b.scanSnapshots(rows)
}

func (b *PostgresBackend) ListPending(ctx context.Context, offset, limit int) ([]storage.ActivitySnapshot, error) {
	return b.listByStatus(ctx, "pending", offset, limit)
}

func (b *PostgresBackend) ListProcessing(ctx context.Context, offset, limit int) ([]storage.ActivitySnapshot, error) {
	return b.listByStatus(ctx, "processing", offset, limit)
}

func (b *PostgresBackend) ListScheduled(ctx context.Context, offset, limit int) ([]storage.ActivitySnapshot, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT id, activity_type, payload, priority, status, created_at,
			scheduled_at, started_at, completed_at, current_worker_id,
			last_worker_id, retry_count, max_retries, timeout_seconds,
			retry_delay_seconds, last_error, last_error_at, metadata,
			idempotency_key, lease_deadline_ms,
			parent_activity_id, root_activity_id, depth
		FROM runnerq_activities
		WHERE queue_name = $1 AND status IN ('scheduled', 'retrying')
		ORDER BY scheduled_at ASC, created_at ASC
		LIMIT $2 OFFSET $3`,
		b.queueName, limit, offset)
	if err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to list scheduled: %v", err))
	}
	defer rows.Close()

	return b.scanSnapshots(rows)
}

func (b *PostgresBackend) ListCompleted(ctx context.Context, offset, limit int) ([]storage.ActivitySnapshot, error) {
	return b.ListCompletedNonCron(ctx, offset, limit)
}

func (b *PostgresBackend) ListCompletedNonCron(ctx context.Context, offset, limit int) ([]storage.ActivitySnapshot, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT id, activity_type, payload, priority, status, created_at,
			scheduled_at, started_at, completed_at, current_worker_id,
			last_worker_id, retry_count, max_retries, timeout_seconds,
			retry_delay_seconds, last_error, last_error_at, metadata,
			idempotency_key, lease_deadline_ms,
			parent_activity_id, root_activity_id, depth
		FROM runnerq_activities
		WHERE queue_name = $1 AND status IN ('completed', 'failed')
		  AND (metadata->>'source') IS DISTINCT FROM 'cron'
		ORDER BY completed_at DESC, created_at DESC
		LIMIT $2 OFFSET $3`,
		b.queueName, limit, offset)
	if err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to list completed non-cron: %v", err))
	}
	defer rows.Close()

	return b.scanSnapshots(rows)
}

func (b *PostgresBackend) ListCompletedCron(ctx context.Context, offset, limit int) ([]storage.ActivitySnapshot, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT id, activity_type, payload, priority, status, created_at,
			scheduled_at, started_at, completed_at, current_worker_id,
			last_worker_id, retry_count, max_retries, timeout_seconds,
			retry_delay_seconds, last_error, last_error_at, metadata,
			idempotency_key, lease_deadline_ms,
			parent_activity_id, root_activity_id, depth
		FROM runnerq_activities
		WHERE queue_name = $1 AND status IN ('completed', 'failed')
		  AND metadata->>'source' = 'cron'
		ORDER BY completed_at DESC, created_at DESC
		LIMIT $2 OFFSET $3`,
		b.queueName, limit, offset)
	if err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to list completed cron: %v", err))
	}
	defer rows.Close()

	return b.scanSnapshots(rows)
}

func (b *PostgresBackend) ListDeadLetter(ctx context.Context, offset, limit int) ([]storage.DeadLetterRecord, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT id, activity_type, payload, priority, status, created_at,
			scheduled_at, started_at, completed_at, current_worker_id,
			last_worker_id, retry_count, max_retries, timeout_seconds,
			retry_delay_seconds, last_error, last_error_at, metadata,
			idempotency_key, lease_deadline_ms,
			parent_activity_id, root_activity_id, depth
		FROM runnerq_activities
		WHERE queue_name = $1 AND status = 'dead_letter'
		ORDER BY completed_at DESC
		LIMIT $2 OFFSET $3`,
		b.queueName, limit, offset)
	if err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to list dead letter: %v", err))
	}
	defer rows.Close()

	snapshots, err := b.scanSnapshots(rows)
	if err != nil {
		return nil, err
	}

	var records []storage.DeadLetterRecord
	for _, s := range snapshots {
		errorStr := ""
		if s.LastError != nil {
			errorStr = *s.LastError
		}
		failedAt := time.Now().UTC()
		if s.CompletedAt != nil {
			failedAt = *s.CompletedAt
		}
		records = append(records, storage.DeadLetterRecord{
			Activity: s,
			Error:    errorStr,
			FailedAt: failedAt,
		})
	}

	return records, nil
}

func (b *PostgresBackend) GetActivity(ctx context.Context, activityID uuid.UUID) (*storage.ActivitySnapshot, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT id, activity_type, payload, priority, status, created_at,
			scheduled_at, started_at, completed_at, current_worker_id,
			last_worker_id, retry_count, max_retries, timeout_seconds,
			retry_delay_seconds, last_error, last_error_at, metadata,
			idempotency_key, lease_deadline_ms,
			parent_activity_id, root_activity_id, depth
		FROM runnerq_activities
		WHERE id = $1 AND queue_name = $2`,
		activityID, b.queueName)
	if err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to get activity: %v", err))
	}
	defer rows.Close()

	snapshots, err := b.scanSnapshots(rows)
	if err != nil {
		return nil, err
	}
	if len(snapshots) == 0 {
		return nil, nil
	}
	return &snapshots[0], nil
}

func (b *PostgresBackend) GetActivityEvents(ctx context.Context, activityID uuid.UUID, limit int) ([]storage.ActivityEvent, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT activity_id, event_type, worker_id, detail, created_at
		FROM runnerq_events
		WHERE activity_id = $1
		ORDER BY created_at ASC
		LIMIT $2`,
		activityID, limit)
	if err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to get events: %v", err))
	}
	defer rows.Close()

	var events []storage.ActivityEvent
	for rows.Next() {
		var e storage.ActivityEvent
		var eventTypeRaw string
		err := rows.Scan(&e.ActivityID, &eventTypeRaw, &e.WorkerID, &e.Detail, &e.Timestamp)
		if err != nil {
			return nil, storage.NewInternalError(fmt.Sprintf("Failed to scan event: %v", err))
		}
		// Handle both legacy JSON-encoded strings ("\"Enqueued\"") and plain strings ("Enqueued")
		var et string
		if err := json.Unmarshal([]byte(eventTypeRaw), &et); err != nil {
			et = strings.Trim(eventTypeRaw, "\"")
		}
		e.EventType = et
		events = append(events, e)
	}

	return events, nil
}

func (b *PostgresBackend) GetChildren(ctx context.Context, parentID uuid.UUID, offset, limit int) ([]storage.ActivitySnapshot, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT id, activity_type, payload, priority, status, created_at,
			scheduled_at, started_at, completed_at, current_worker_id,
			last_worker_id, retry_count, max_retries, timeout_seconds,
			retry_delay_seconds, last_error, last_error_at, metadata,
			idempotency_key, lease_deadline_ms,
			parent_activity_id, root_activity_id, depth
		FROM runnerq_activities
		WHERE queue_name = $1 AND parent_activity_id = $2
		ORDER BY created_at ASC
		LIMIT $3 OFFSET $4`,
		b.queueName, parentID, limit, offset)
	if err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to list children: %v", err))
	}
	defer rows.Close()

	return b.scanSnapshots(rows)
}

// statusOrderTimestampSQL picks the timestamp column that's most meaningful
// for the active status filter so "newest first" actually means "most
// recently transitioned into this state":
//
//	completed  -> completed_at
//	failed     -> last_error_at
//	processing -> started_at
//	scheduled  -> scheduled_at
//	retrying   -> last_error_at  (most recent failure that triggered the retry)
//	dead_letter-> last_error_at
//	pending / "all" -> created_at (no later timestamp exists yet)
//
// Wrapped in COALESCE so a NULL on the status-specific column (defensive —
// shouldn't happen) and the unfiltered "all" case both fall back to
// created_at, preserving the prior behaviour as the safe default.
const statusOrderTimestampSQL = `COALESCE(
		CASE $2
			WHEN 'completed'   THEN completed_at
			WHEN 'failed'      THEN last_error_at
			WHEN 'processing'  THEN started_at
			WHEN 'scheduled'   THEN scheduled_at
			WHEN 'retrying'    THEN last_error_at
			WHEN 'dead_letter' THEN last_error_at
		END,
		created_at
	) DESC`

func (b *PostgresBackend) ListRecentRoots(ctx context.Context, status string, offset, limit int) ([]storage.ActivitySnapshot, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT id, activity_type, payload, priority, status, created_at,
			scheduled_at, started_at, completed_at, current_worker_id,
			last_worker_id, retry_count, max_retries, timeout_seconds,
			retry_delay_seconds, last_error, last_error_at, metadata,
			idempotency_key, lease_deadline_ms,
			parent_activity_id, root_activity_id, depth
		FROM runnerq_activities
		WHERE queue_name = $1 AND parent_activity_id IS NULL
		  AND ($2 = '' OR status = $2)
		  -- The Schedules tab is the canonical home for cron runs; suppress
		  -- them from the runs list when the user is browsing terminal
		  -- statuses so the Completed/Failed views aren't dominated by
		  -- recurring-job clutter. Other statuses (pending/processing/etc.)
		  -- still surface cron rows so live cron work stays observable.
		  AND ($2 NOT IN ('completed', 'failed') OR (metadata->>'source') IS DISTINCT FROM 'cron')
		ORDER BY `+statusOrderTimestampSQL+`
		LIMIT $3 OFFSET $4`,
		b.queueName, status, limit, offset)
	if err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to list recent roots: %v", err))
	}
	defer rows.Close()

	return b.scanSnapshots(rows)
}

func (b *PostgresBackend) ListRecentActivities(ctx context.Context, status string, offset, limit int) ([]storage.ActivitySnapshot, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT id, activity_type, payload, priority, status, created_at,
			scheduled_at, started_at, completed_at, current_worker_id,
			last_worker_id, retry_count, max_retries, timeout_seconds,
			retry_delay_seconds, last_error, last_error_at, metadata,
			idempotency_key, lease_deadline_ms,
			parent_activity_id, root_activity_id, depth
		FROM runnerq_activities
		WHERE queue_name = $1
		  AND ($2 = '' OR status = $2)
		  -- See ListRecentRoots: cron runs live in the Schedules tab; hide
		  -- them from the flattened runs list on terminal statuses.
		  AND ($2 NOT IN ('completed', 'failed') OR (metadata->>'source') IS DISTINCT FROM 'cron')
		ORDER BY `+statusOrderTimestampSQL+`
		LIMIT $3 OFFSET $4`,
		b.queueName, status, limit, offset)
	if err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to list recent activities: %v", err))
	}
	defer rows.Close()

	return b.scanSnapshots(rows)
}

func (b *PostgresBackend) ListCronActivities(ctx context.Context, offset, limit int) ([]storage.ActivitySnapshot, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT id, activity_type, payload, priority, status, created_at,
			scheduled_at, started_at, completed_at, current_worker_id,
			last_worker_id, retry_count, max_retries, timeout_seconds,
			retry_delay_seconds, last_error, last_error_at, metadata,
			idempotency_key, lease_deadline_ms,
			parent_activity_id, root_activity_id, depth
		FROM runnerq_activities
		WHERE queue_name = $1 AND metadata->>'source' = 'cron'
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3`,
		b.queueName, limit, offset)
	if err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to list cron activities: %v", err))
	}
	defer rows.Close()

	return b.scanSnapshots(rows)
}

func (b *PostgresBackend) GetSubtree(ctx context.Context, rootID uuid.UUID) ([]storage.ActivitySnapshot, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT id, activity_type, payload, priority, status, created_at,
			scheduled_at, started_at, completed_at, current_worker_id,
			last_worker_id, retry_count, max_retries, timeout_seconds,
			retry_delay_seconds, last_error, last_error_at, metadata,
			idempotency_key, lease_deadline_ms,
			parent_activity_id, root_activity_id, depth
		FROM runnerq_activities
		WHERE queue_name = $1 AND root_activity_id = $2
		ORDER BY depth ASC, created_at ASC`,
		b.queueName, rootID)
	if err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to get subtree: %v", err))
	}
	defer rows.Close()

	return b.scanSnapshots(rows)
}

// EventStream yields lifecycle events committed after the subscription. It is
// fed by the shared event tailer (signals.go): writers only insert rows, a
// debounced post-commit notification kicks the tailer, and the tailer reads
// full event rows from the table by id cursor. Consequences vs the old
// per-event NOTIFY design: subscribers no longer pin a dedicated pool
// connection each (one shared LISTEN connection serves everything), event
// payloads are no longer truncated at the 8KB NOTIFY limit, and writers pay
// zero notification cost inside their transactions.
func (b *PostgresBackend) EventStream(ctx context.Context) (<-chan storage.ActivityEvent, error) {
	return b.getWatcher().subscribeEvents(ctx), nil
}

// ============================================================================
// Internal helpers
// ============================================================================

type activityRow struct {
	id                   uuid.UUID
	activityType         string
	payload              json.RawMessage
	priority             int32
	status               string
	createdAt            time.Time
	scheduledAt          *time.Time
	startedAt            *time.Time
	completedAt          *time.Time
	currentWorkerID      *string
	lastWorkerID         *string
	retryCount           int32
	maxRetries           int32
	timeoutSeconds       int64
	retryDelaySeconds    int64
	maxRetryDelaySeconds int64
	lastError            *string
	lastErrorAt          *time.Time
	metadata             json.RawMessage
	idempotencyKey       *string
	leaseDeadlineMS      *int64
	parentActivityID     *uuid.UUID
	rootActivityID       *uuid.UUID
	depth                int16
}

func (r *activityRow) toQueuedActivity() *storage.QueuedActivity {
	meta := make(map[string]string)
	if r.metadata != nil {
		_ = json.Unmarshal(r.metadata, &meta)
	}

	var idempKey *storage.IdempotencyKeyConfig
	if r.idempotencyKey != nil {
		idempKey = &storage.IdempotencyKeyConfig{
			Key:      *r.idempotencyKey,
			Behavior: storage.BehaviorReturnExisting,
		}
	}

	rootID := r.id
	if r.rootActivityID != nil {
		rootID = *r.rootActivityID
	}
	return &storage.QueuedActivity{
		ID:                   r.id,
		ActivityType:         r.activityType,
		Payload:              r.payload,
		Priority:             intToPriority(r.priority),
		MaxRetries:           uint32(r.maxRetries),
		RetryCount:           uint32(r.retryCount),
		TimeoutSeconds:       uint64(r.timeoutSeconds),
		RetryDelaySeconds:    uint64(r.retryDelaySeconds),
		MaxRetryDelaySeconds: uint64(r.maxRetryDelaySeconds),
		ScheduledAt:          r.scheduledAt,
		Metadata:             meta,
		IdempotencyKey:       idempKey,
		CreatedAt:            r.createdAt,
		ParentActivityID:     r.parentActivityID,
		RootActivityID:       rootID,
		Depth:                uint16(r.depth),
	}
}

func (r *activityRow) toSnapshot() storage.ActivitySnapshot {
	meta := make(map[string]string)
	if r.metadata != nil {
		_ = json.Unmarshal(r.metadata, &meta)
	}

	var status string
	switch r.status {
	case "pending":
		status = "Pending"
	case "processing":
		status = "Running"
	case "completed":
		status = "Completed"
	case "scheduled":
		status = "Scheduled"
	case "retrying":
		status = "Retrying"
	case "failed":
		status = "Failed"
	case "dead_letter":
		status = "DeadLetter"
	default:
		status = "Pending"
	}

	statusUpdatedAt := r.createdAt
	if r.startedAt != nil {
		statusUpdatedAt = *r.startedAt
	} else if r.completedAt != nil {
		statusUpdatedAt = *r.completedAt
	}

	return storage.ActivitySnapshot{
		ID:                r.id,
		ActivityType:      r.activityType,
		Payload:           r.payload,
		Priority:          intToPriority(r.priority),
		Status:            status,
		CreatedAt:         r.createdAt,
		ScheduledAt:       r.scheduledAt,
		StartedAt:         r.startedAt,
		CompletedAt:       r.completedAt,
		CurrentWorkerID:   r.currentWorkerID,
		LastWorkerID:      r.lastWorkerID,
		RetryCount:        uint32(r.retryCount),
		MaxRetries:        uint32(r.maxRetries),
		TimeoutSeconds:    uint64(r.timeoutSeconds),
		RetryDelaySeconds: uint64(r.retryDelaySeconds),
		Metadata:          meta,
		LastError:         r.lastError,
		LastErrorAt:       r.lastErrorAt,
		StatusUpdatedAt:   statusUpdatedAt,
		LeaseDeadlineMS:   r.leaseDeadlineMS,
		ProcessingMember:  r.currentWorkerID,
		IdempotencyKey:    r.idempotencyKey,
		ParentActivityID:  r.parentActivityID,
		RootActivityID:    r.rootActivityID,
		Depth:             uint16(r.depth),
	}
}

// ============================================================================
// WorkerPoolRegistrar Implementation
// ============================================================================

// RegisterWorkerPool inserts (or refreshes, on UUID collision) the row that
// identifies this engine instance's pool. The same call doubles as the first
// heartbeat — last_seen_at is set to NOW() so it counts immediately.
func (b *PostgresBackend) RegisterWorkerPool(ctx context.Context, pool storage.WorkerPoolInfo) error {
	var types []string
	if len(pool.ActivityTypes) > 0 {
		types = pool.ActivityTypes
	}
	_, err := b.pool.Exec(ctx, `
		INSERT INTO runnerq_worker_pools (pool_id, queue_name, max_workers, activity_types, started_at, last_seen_at)
		VALUES ($1, $2, $3, $4, NOW(), NOW())
		ON CONFLICT (pool_id) DO UPDATE
		SET max_workers    = EXCLUDED.max_workers,
		    activity_types = EXCLUDED.activity_types,
		    last_seen_at   = NOW()`,
		pool.PoolID, pool.QueueName, pool.MaxWorkers, types)
	if err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to register worker pool: %v", err))
	}
	return nil
}

// HeartbeatWorkerPool refreshes last_seen_at. A pool that misses heartbeats
// for longer than workerPoolLivenessWindow stops counting toward Stats()
// max_workers but its row is left in place — registration on next start
// reuses the same UUID slot via the upsert in RegisterWorkerPool, and dead
// rows can be vacuumed independently.
func (b *PostgresBackend) HeartbeatWorkerPool(ctx context.Context, poolID uuid.UUID) error {
	_, err := b.pool.Exec(ctx, `
		UPDATE runnerq_worker_pools SET last_seen_at = NOW() WHERE pool_id = $1`,
		poolID)
	if err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to heartbeat worker pool: %v", err))
	}
	return nil
}

// DeregisterWorkerPool removes the row on graceful shutdown. Best-effort —
// callers should log but not fail on errors. Crashed pools are reaped
// implicitly by the liveness window.
func (b *PostgresBackend) DeregisterWorkerPool(ctx context.Context, poolID uuid.UUID) error {
	_, err := b.pool.Exec(ctx, `DELETE FROM runnerq_worker_pools WHERE pool_id = $1`, poolID)
	if err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to deregister worker pool: %v", err))
	}
	return nil
}

func (b *PostgresBackend) scanSnapshots(rows pgx.Rows) ([]storage.ActivitySnapshot, error) {
	var snapshots []storage.ActivitySnapshot
	for rows.Next() {
		var r activityRow
		err := rows.Scan(
			&r.id, &r.activityType, &r.payload, &r.priority, &r.status,
			&r.createdAt, &r.scheduledAt, &r.startedAt, &r.completedAt,
			&r.currentWorkerID, &r.lastWorkerID, &r.retryCount, &r.maxRetries,
			&r.timeoutSeconds, &r.retryDelaySeconds, &r.lastError, &r.lastErrorAt,
			&r.metadata, &r.idempotencyKey, &r.leaseDeadlineMS,
			&r.parentActivityID, &r.rootActivityID, &r.depth)
		if err != nil {
			return nil, storage.NewInternalError(fmt.Sprintf("Failed to scan row: %v", err))
		}
		snapshots = append(snapshots, r.toSnapshot())
	}
	return snapshots, nil
}
