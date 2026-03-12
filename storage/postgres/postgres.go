package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alob-mtc/runnerq-go/storage"
)

const notifyPayloadMax = 7900

// PostgresBackend is a PostgreSQL-based storage backend for RunnerQ.
type PostgresBackend struct {
	pool           *pgxpool.Pool
	queueName      string
	defaultLeaseMS int64
}

// New creates a new PostgresBackend with default pool size 10 and lease 30s.
func New(ctx context.Context, databaseURL, queueName string) (*PostgresBackend, error) {
	return WithConfig(ctx, databaseURL, queueName, 30_000, 10)
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

	return backend, nil
}

// Close closes the connection pool.
func (b *PostgresBackend) Close() {
	b.pool.Close()
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

func (b *PostgresBackend) notificationChannel() string {
	return fmt.Sprintf("runnerq_events_%s", b.queueName)
}

func priorityToInt(p storage.ActivityPriority) int32 {
	switch p {
	case storage.PriorityCritical:
		return 3
	case storage.PriorityHigh:
		return 2
	case storage.PriorityNormal:
		return 1
	case storage.PriorityLow:
		return 0
	default:
		return 1
	}
}

func intToPriority(val int32) storage.ActivityPriority {
	switch val {
	case 3:
		return storage.PriorityCritical
	case 2:
		return storage.PriorityHigh
	case 1:
		return storage.PriorityNormal
	default:
		return storage.PriorityLow
	}
}

func (b *PostgresBackend) recordEvent(ctx context.Context, tx pgx.Tx, activityID uuid.UUID, eventType string, workerID *string, detail json.RawMessage) error {
	now := time.Now().UTC()
	event := storage.ActivityEvent{
		ActivityID: activityID,
		Timestamp:  now,
		EventType:  eventType,
		WorkerID:   workerID,
		Detail:     detail,
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO runnerq_events (activity_id, queue_name, event_type, worker_id, detail, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		activityID, b.queueName, jsonString(eventType), workerID, detail, now)
	if err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to record event: %v", err))
	}

	payload := b.prepareNotifyPayload(&event)
	channel := b.notificationChannel()
	_, err = tx.Exec(ctx, "SELECT pg_notify($1::text, $2::text)", channel, payload)
	if err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to notify: %v", err))
	}

	return nil
}

func (b *PostgresBackend) prepareNotifyPayload(event *storage.ActivityEvent) string {
	data, err := json.Marshal(event)
	if err != nil {
		return "{}"
	}
	payload := string(data)
	if len(payload) > notifyPayloadMax {
		slog.Warn("NOTIFY payload truncated to avoid PostgreSQL limit",
			"activity_id", event.ActivityID,
			"len", len(payload))
		truncated := map[string]interface{}{
			"activity_id": event.ActivityID,
			"event_type":  event.EventType,
			"truncated":   true,
		}
		data, _ = json.Marshal(truncated)
		return string(data)
	}
	return payload
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

func jsonString(s string) string {
	data, _ := json.Marshal(s)
	return string(data)
}

func toDetail(kv map[string]interface{}) json.RawMessage {
	data, _ := json.Marshal(kv)
	return data
}

func strPtr(s string) *string { return &s }

// ============================================================================
// QueueStorage Implementation
// ============================================================================

func (b *PostgresBackend) Enqueue(ctx context.Context, a storage.QueuedActivity) error {
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

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO runnerq_activities (
			id, queue_name, activity_type, payload, priority, status,
			created_at, scheduled_at, retry_count, max_retries,
			timeout_seconds, retry_delay_seconds, metadata, idempotency_key
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
		a.ID, b.queueName, a.ActivityType, a.Payload,
		priorityToInt(a.Priority), status, a.CreatedAt, a.ScheduledAt,
		int32(a.RetryCount), int32(a.MaxRetries),
		int64(a.TimeoutSeconds), int64(a.RetryDelaySeconds),
		metadataJSON, idempotencyKey)
	if err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to enqueue activity: %v", err))
	}

	var eventType string
	var detail json.RawMessage
	if a.ScheduledAt != nil {
		eventType = storage.EventScheduled
		detail = toDetail(map[string]interface{}{"scheduled_at": a.ScheduledAt})
	} else {
		eventType = storage.EventEnqueued
		detail = toDetail(map[string]interface{}{
			"priority":     fmt.Sprintf("%d", a.Priority),
			"scheduled_at": a.ScheduledAt,
		})
	}

	if err := b.recordEvent(ctx, tx, a.ID, eventType, nil, detail); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to commit enqueue: %v", err))
	}

	return nil
}

func (b *PostgresBackend) Dequeue(ctx context.Context, workerID string, _ time.Duration, activityTypes []string) (*storage.QueuedActivity, error) {
	deadline := time.Now().UTC().Add(time.Duration(b.defaultLeaseMS) * time.Millisecond)
	now := time.Now().UTC()

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	var types []string
	if len(activityTypes) > 0 {
		types = activityTypes
	}

	row := tx.QueryRow(ctx, `
		UPDATE runnerq_activities
		SET status = 'processing',
			current_worker_id = $1,
			lease_deadline_ms = $2,
			started_at = $3
		WHERE id = (
			SELECT id FROM runnerq_activities
			WHERE queue_name = $4
			  AND (
				status = 'pending'
				OR (status IN ('scheduled', 'retrying') AND scheduled_at <= $5)
			  )
			  AND ($6::text[] IS NULL OR activity_type = ANY($6))
			ORDER BY
				priority DESC,
				retry_count DESC,
				COALESCE(scheduled_at, created_at) ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, activity_type, payload, priority, retry_count, max_retries,
			timeout_seconds, retry_delay_seconds, scheduled_at, metadata,
			idempotency_key, created_at`,
		workerID, deadline.UnixMilli(), now, b.queueName, now, types)

	var ar activityRow
	err = row.Scan(
		&ar.id, &ar.activityType, &ar.payload, &ar.priority,
		&ar.retryCount, &ar.maxRetries, &ar.timeoutSeconds,
		&ar.retryDelaySeconds, &ar.scheduledAt, &ar.metadata,
		&ar.idempotencyKey, &ar.createdAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to dequeue: %v", err))
	}

	a := ar.toQueuedActivity()

	detail := toDetail(map[string]interface{}{"lease_deadline_ms": deadline.UnixMilli()})
	if err := b.recordEvent(ctx, tx, a.ID, storage.EventDequeued, &workerID, detail); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to commit dequeue: %v", err))
	}

	// Auto-extend lease if timeout > default lease
	defaultLeaseSecs := b.defaultLeaseMS / 1000
	if defaultLeaseSecs < int64(a.TimeoutSeconds) {
		additionalSecs := int64(a.TimeoutSeconds) - defaultLeaseSecs
		extendBy := time.Duration(additionalSecs+10) * time.Second
		if _, err := b.ExtendLease(ctx, a.ID, extendBy); err != nil {
			slog.Warn("Failed to extend lease for long-running activity",
				"activity_id", a.ID,
				"extend_by_secs", extendBy.Seconds(),
				"error", err)
		}
	}

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

	if result != nil {
		ar := &storage.ActivityResult{Data: result, State: storage.ResultOk}
		if err := b.storeResultTx(ctx, tx, activityID, ar, now); err != nil {
			return err
		}
	}

	actTypeStr := ""
	if actType != nil {
		actTypeStr = *actType
	}
	detailMap := map[string]interface{}{"activity_type": actTypeStr}
	if result != nil {
		detailMap["result_stored"] = true
	}

	if err := b.recordEvent(ctx, tx, activityID, storage.EventCompleted, &workerID, toDetail(detailMap)); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to commit ack: %v", err))
	}

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
		SELECT id, retry_count, max_retries, retry_delay_seconds
		FROM runnerq_activities
		WHERE id = $1 AND queue_name = $2 AND status = 'processing' AND current_worker_id = $3
		FOR UPDATE`,
		activityID, b.queueName, workerID).Scan(
		&ar.id, &ar.retryCount, &ar.maxRetries, &ar.retryDelaySeconds)
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

		res := &storage.ActivityResult{Data: toDetail(map[string]interface{}{"error": errorMessage}), State: storage.ResultErr}
		if err := b.storeResultTx(ctx, tx, activityID, res, now); err != nil {
			return false, err
		}
		if err := b.recordEvent(ctx, tx, activityID, storage.EventFailed, &workerID,
			toDetail(map[string]interface{}{"retryable": false, "error": errorMessage})); err != nil {
			return false, err
		}

		if err := tx.Commit(ctx); err != nil {
			return false, storage.NewInternalError(fmt.Sprintf("Failed to commit ack_failure: %v", err))
		}
		return false, nil
	}

	// Retryable: check if we can retry
	canRetry := ar.maxRetries == 0 || (ar.retryCount+1) < ar.maxRetries

	if canRetry {
		baseDelay := int64(ar.retryDelaySeconds)
		backoffMult := int64(1) << (ar.retryCount + 1)
		retryDelay := baseDelay * backoffMult
		scheduledAt := now.Add(time.Duration(retryDelay) * time.Second)
		newRetryCount := ar.retryCount + 1

		_, err = tx.Exec(ctx, `
			UPDATE runnerq_activities
			SET status = 'retrying',
				retry_count = retry_count + 1,
				scheduled_at = $1,
				last_error = $2,
				last_error_at = $3,
				last_worker_id = $4,
				current_worker_id = NULL,
				lease_deadline_ms = NULL
			WHERE id = $5 AND queue_name = $6 AND status = 'processing' AND current_worker_id = $4`,
			scheduledAt, errorMessage, now, workerID, activityID, b.queueName)
		if err != nil {
			return false, storage.NewInternalError(fmt.Sprintf("Failed to schedule retry: %v", err))
		}

		if err := b.recordEvent(ctx, tx, activityID, storage.EventRetrying, &workerID,
			toDetail(map[string]interface{}{
				"retry_count":  newRetryCount,
				"scheduled_at": scheduledAt.Format(time.RFC3339),
				"error":        errorMessage,
			})); err != nil {
			return false, err
		}

		if err := tx.Commit(ctx); err != nil {
			return false, storage.NewInternalError(fmt.Sprintf("Failed to commit ack_failure retry: %v", err))
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

	res := &storage.ActivityResult{Data: toDetail(map[string]interface{}{"error": errorMessage}), State: storage.ResultErr}
	if err := b.storeResultTx(ctx, tx, activityID, res, now); err != nil {
		return false, err
	}
	if err := b.recordEvent(ctx, tx, activityID, storage.EventDeadLetter, &workerID,
		toDetail(map[string]interface{}{"error": errorMessage})); err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, storage.NewInternalError(fmt.Sprintf("Failed to commit ack_failure DLQ: %v", err))
	}

	return true, nil
}

func (b *PostgresBackend) ProcessScheduled(_ context.Context) (uint64, error) {
	return 0, nil
}

func (b *PostgresBackend) SchedulesNatively() bool {
	return true
}

func (b *PostgresBackend) RequeueExpired(ctx context.Context, batchSize int) (uint64, error) {
	nowMS := time.Now().UTC().UnixMilli()

	tag, err := b.pool.Exec(ctx, `
		UPDATE runnerq_activities
		SET status = 'pending',
			current_worker_id = NULL,
			lease_deadline_ms = NULL
		WHERE id IN (
			SELECT id FROM runnerq_activities
			WHERE queue_name = $1
			  AND status = 'processing'
			  AND lease_deadline_ms < $2
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		)`, b.queueName, nowMS, batchSize)
	if err != nil {
		return 0, storage.NewInternalError(fmt.Sprintf("Failed to requeue expired: %v", err))
	}
	return uint64(tag.RowsAffected()), nil
}

func (b *PostgresBackend) ExtendLease(ctx context.Context, activityID uuid.UUID, extendBy time.Duration) (bool, error) {
	newDeadline := time.Now().UTC().Add(extendBy)

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return false, storage.NewInternalError(fmt.Sprintf("Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE runnerq_activities
		SET lease_deadline_ms = $1
		WHERE id = $2 AND queue_name = $3 AND status = 'processing'`,
		newDeadline.UnixMilli(), activityID, b.queueName)
	if err != nil {
		return false, storage.NewInternalError(fmt.Sprintf("Failed to extend lease: %v", err))
	}

	if tag.RowsAffected() > 0 {
		if err := b.recordEvent(ctx, tx, activityID, storage.EventLeaseExtended, nil,
			toDetail(map[string]interface{}{
				"new_deadline_ms": newDeadline.UnixMilli(),
				"extend_by_ms":    extendBy.Milliseconds(),
			})); err != nil {
			return false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return false, storage.NewInternalError(fmt.Sprintf("Failed to commit extend_lease: %v", err))
		}
		return true, nil
	}

	return false, nil
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
		toDetail(map[string]interface{}{"result_stored": true, "state": stateStr})); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return storage.NewInternalError(fmt.Sprintf("Failed to commit store_result: %v", err))
	}

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

func (b *PostgresBackend) CheckIdempotency(ctx context.Context, a *storage.QueuedActivity) (*uuid.UUID, error) {
	if a.IdempotencyKey == nil {
		return nil, nil
	}
	key := a.IdempotencyKey.Key
	behavior := a.IdempotencyKey.Behavior

	// Try to atomically claim the key
	tag, err := b.pool.Exec(ctx, `
		INSERT INTO runnerq_idempotency (queue_name, idempotency_key, activity_id, created_at, updated_at)
		VALUES ($1, $2, $3, NOW(), NOW())
		ON CONFLICT (queue_name, idempotency_key) DO NOTHING`,
		b.queueName, key, a.ID)
	if err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to claim idempotency key: %v", err))
	}

	if tag.RowsAffected() > 0 {
		return nil, nil
	}

	// Key exists - get existing activity
	var existingID uuid.UUID
	var status *string
	err = b.pool.QueryRow(ctx, `
		SELECT i.activity_id, a.status
		FROM runnerq_idempotency i
		LEFT JOIN runnerq_activities a ON i.activity_id = a.id
		WHERE i.queue_name = $1 AND i.idempotency_key = $2`,
		b.queueName, key).Scan(&existingID, &status)
	if err != nil {
		if err == pgx.ErrNoRows {
			return b.CheckIdempotency(ctx, a)
		}
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to get existing key: %v", err))
	}

	switch behavior {
	case storage.BehaviorReturnExisting:
		return &existingID, nil

	case storage.BehaviorAllowReuse:
		_, err = b.pool.Exec(ctx, `
			UPDATE runnerq_idempotency
			SET activity_id = $1, updated_at = NOW()
			WHERE queue_name = $2 AND idempotency_key = $3`,
			a.ID, b.queueName, key)
		if err != nil {
			return nil, storage.NewInternalError(fmt.Sprintf("Failed to update key: %v", err))
		}
		return nil, nil

	case storage.BehaviorAllowReuseOnFailure:
		statusStr := ""
		if status != nil {
			statusStr = *status
		}
		if statusStr == "dead_letter" || statusStr == "failed" {
			tag, err = b.pool.Exec(ctx, `
				UPDATE runnerq_idempotency i
				SET activity_id = $1, updated_at = NOW()
				FROM runnerq_activities a
				WHERE i.queue_name = $2
				  AND i.idempotency_key = $3
				  AND i.activity_id = a.id
				  AND a.status IN ('dead_letter', 'failed')`,
				a.ID, b.queueName, key)
			if err != nil {
				return nil, storage.NewInternalError(fmt.Sprintf("Failed to update key: %v", err))
			}
			if tag.RowsAffected() > 0 {
				return nil, nil
			}
			return nil, storage.NewIdempotencyConflictError(fmt.Sprintf("Idempotency key '%s' was claimed by another request", key))
		}
		return nil, storage.NewIdempotencyConflictError(fmt.Sprintf("Idempotency key '%s' exists with status '%s'", key, statusStr))

	case storage.BehaviorNoReuse:
		return nil, storage.NewDuplicateActivityError(fmt.Sprintf("Activity with idempotency key '%s' already exists: %s", key, existingID))

	default:
		return nil, nil
	}
}

// ============================================================================
// InspectionStorage Implementation
// ============================================================================

func (b *PostgresBackend) Stats(ctx context.Context) (*storage.QueueStats, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT status, COUNT(*) as count
		FROM runnerq_activities
		WHERE queue_name = $1
		GROUP BY status`, b.queueName)
	if err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to get stats: %v", err))
	}
	defer rows.Close()

	stats := &storage.QueueStats{}
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return nil, storage.NewInternalError(fmt.Sprintf("Failed to scan stats: %v", err))
		}
		switch status {
		case "pending":
			stats.Pending = uint64(count)
		case "processing":
			stats.Processing = uint64(count)
		case "scheduled", "retrying":
			stats.Scheduled += uint64(count)
		case "dead_letter":
			stats.DeadLetter = uint64(count)
		}
	}

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
		switch priority {
		case 3:
			stats.ByPriority.Critical = uint64(count)
		case 2:
			stats.ByPriority.High = uint64(count)
		case 1:
			stats.ByPriority.Normal = uint64(count)
		case 0:
			stats.ByPriority.Low = uint64(count)
		}
	}

	return stats, nil
}

func (b *PostgresBackend) listByStatus(ctx context.Context, status string, offset, limit int) ([]storage.ActivitySnapshot, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT id, activity_type, payload, priority, status, created_at,
			scheduled_at, started_at, completed_at, current_worker_id,
			last_worker_id, retry_count, max_retries, timeout_seconds,
			retry_delay_seconds, last_error, last_error_at, metadata,
			idempotency_key, lease_deadline_ms
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
			idempotency_key, lease_deadline_ms
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
	rows, err := b.pool.Query(ctx, `
		SELECT id, activity_type, payload, priority, status, created_at,
			scheduled_at, started_at, completed_at, current_worker_id,
			last_worker_id, retry_count, max_retries, timeout_seconds,
			retry_delay_seconds, last_error, last_error_at, metadata,
			idempotency_key, lease_deadline_ms
		FROM runnerq_activities
		WHERE queue_name = $1 AND status IN ('completed', 'failed')
		ORDER BY completed_at DESC NULLS LAST, created_at DESC
		LIMIT $2 OFFSET $3`,
		b.queueName, limit, offset)
	if err != nil {
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to list completed: %v", err))
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
			idempotency_key, lease_deadline_ms
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
			idempotency_key, lease_deadline_ms
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
		var eventTypeJSON string
		err := rows.Scan(&e.ActivityID, &eventTypeJSON, &e.WorkerID, &e.Detail, &e.Timestamp)
		if err != nil {
			return nil, storage.NewInternalError(fmt.Sprintf("Failed to scan event: %v", err))
		}
		// event_type stored as JSON string like "\"Enqueued\""
		var et string
		if err := json.Unmarshal([]byte(eventTypeJSON), &et); err != nil {
			et = strings.Trim(eventTypeJSON, "\"")
		}
		e.EventType = et
		events = append(events, e)
	}

	return events, nil
}

func (b *PostgresBackend) EventStream(ctx context.Context) (<-chan storage.ActivityEvent, error) {
	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		return nil, storage.NewUnavailableError(fmt.Sprintf("Failed to acquire connection: %v", err))
	}

	channel := b.notificationChannel()
	_, err = conn.Exec(ctx, fmt.Sprintf("LISTEN %s", pgx.Identifier{channel}.Sanitize()))
	if err != nil {
		conn.Release()
		return nil, storage.NewInternalError(fmt.Sprintf("Failed to listen on channel %s: %v", channel, err))
	}

	ch := make(chan storage.ActivityEvent, 100)

	go func() {
		defer conn.Release()
		defer close(ch)

		for {
			notification, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Error("Listener error", "error", err)
				return
			}

			var event storage.ActivityEvent
			if err := json.Unmarshal([]byte(notification.Payload), &event); err != nil {
				slog.Warn("Failed to parse event from notification",
					"error", err,
					"payload", notification.Payload)
				continue
			}

			select {
			case ch <- event:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}

// ============================================================================
// Internal helpers
// ============================================================================

type activityRow struct {
	id                uuid.UUID
	activityType      string
	payload           json.RawMessage
	priority          int32
	status            string
	createdAt         time.Time
	scheduledAt       *time.Time
	startedAt         *time.Time
	completedAt       *time.Time
	currentWorkerID   *string
	lastWorkerID      *string
	retryCount        int32
	maxRetries        int32
	timeoutSeconds    int64
	retryDelaySeconds int64
	lastError         *string
	lastErrorAt       *time.Time
	metadata          json.RawMessage
	idempotencyKey    *string
	leaseDeadlineMS   *int64
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

	return &storage.QueuedActivity{
		ID:                r.id,
		ActivityType:      r.activityType,
		Payload:           r.payload,
		Priority:          intToPriority(r.priority),
		MaxRetries:        uint32(r.maxRetries),
		RetryCount:        uint32(r.retryCount),
		TimeoutSeconds:    uint64(r.timeoutSeconds),
		RetryDelaySeconds: uint64(r.retryDelaySeconds),
		ScheduledAt:       r.scheduledAt,
		Metadata:          meta,
		IdempotencyKey:    idempKey,
		CreatedAt:         r.createdAt,
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
		status = "Pending"
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
	}
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
			&r.metadata, &r.idempotencyKey, &r.leaseDeadlineMS)
		if err != nil {
			return nil, storage.NewInternalError(fmt.Sprintf("Failed to scan row: %v", err))
		}
		snapshots = append(snapshots, r.toSnapshot())
	}
	return snapshots, nil
}
