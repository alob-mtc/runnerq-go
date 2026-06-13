package runnerq

import (
	"errors"
	"fmt"

	"github.com/alob-mtc/runnerq-go/storage"
)

// ---------------------------------------------------------------------------
// ActivityError
// ---------------------------------------------------------------------------

// ActivityError represents an error from activity handler execution.
// Retryable indicates whether the activity should be retried.
type ActivityError struct {
	Retryable bool
	Message   string
}

func (e *ActivityError) Error() string {
	if e.Retryable {
		return fmt.Sprintf("Retryable error: %s", e.Message)
	}
	return fmt.Sprintf("Non-retryable error: %s", e.Message)
}

// NewRetryError creates an ActivityError that triggers a retry.
func NewRetryError(msg string) *ActivityError {
	return &ActivityError{Retryable: true, Message: msg}
}

// NewNonRetryError creates an ActivityError that should not be retried.
func NewNonRetryError(msg string) *ActivityError {
	return &ActivityError{Retryable: false, Message: msg}
}

// IsRetryable implements the RetryableError interface.
func (e *ActivityError) IsRetryable() bool {
	return e.Retryable
}

// RetryableError is an interface for errors that know if they are retryable.
// Handlers can return any error implementing this interface to control retry behavior.
type RetryableError interface {
	IsRetryable() bool
}

// ---------------------------------------------------------------------------
// WorkerError
// ---------------------------------------------------------------------------

// WorkerErrorKind classifies worker engine errors.
type WorkerErrorKind int

const (
	ErrCustom WorkerErrorKind = iota
	ErrQueue
	ErrSerializationW
	ErrTimeoutW
	ErrExecution
	ErrHandlerNotFound
	ErrBackend
	ErrDatabase
	ErrConfiguration
	ErrShutdown
	ErrAlreadyRunning
	ErrScheduling
	ErrDuplicateActivityW
	ErrIdempotencyConflictW
	ErrDepthExceeded
	ErrUnknown
	// ErrSignalTimeoutW means WaitForSignal's timeout elapsed before the
	// signal was delivered. Not retryable: retrying replays the persisted
	// wait deadline and times out again immediately — the handler must
	// decide what a missing signal means.
	ErrSignalTimeoutW
	// ErrActivityNotFoundW means a signal was addressed to an activity (by ID
	// or by idempotency key) that does not exist in the queue — never
	// enqueued, or already completed and retention-swept. Callers delivering
	// an external event typically treat this as "already settled / nothing to
	// wake" rather than a failure.
	ErrActivityNotFoundW
)

// WorkerError represents an error from the worker engine.
type WorkerError struct {
	Kind    WorkerErrorKind
	Message string
	Cause   error
}

func (e *WorkerError) Error() string {
	prefix := ""
	switch e.Kind {
	case ErrCustom:
		return e.Message
	case ErrQueue:
		prefix = "Activity queue error"
	case ErrSerializationW:
		prefix = "Activity serialization error"
	case ErrTimeoutW:
		return "Activity execution timeout"
	case ErrExecution:
		prefix = "Activity execution failed"
	case ErrHandlerNotFound:
		prefix = "Activity handler not found for activity type"
	case ErrBackend:
		prefix = "Backend error"
	case ErrDatabase:
		prefix = "Database error"
	case ErrConfiguration:
		prefix = "Configuration error"
	case ErrShutdown:
		return "Worker shutdown requested"
	case ErrAlreadyRunning:
		return "Worker is already running"
	case ErrScheduling:
		prefix = "Activity scheduling error"
	case ErrDuplicateActivityW:
		prefix = "Duplicate activity detected"
	case ErrIdempotencyConflictW:
		prefix = "Idempotency key conflict"
	case ErrDepthExceeded:
		prefix = "Activity depth limit exceeded"
	case ErrUnknown:
		prefix = "Unknown error"
	case ErrSignalTimeoutW:
		prefix = "Signal wait timed out"
	case ErrActivityNotFoundW:
		prefix = "Signal target activity not found"
	}
	return fmt.Sprintf("%s: %s", prefix, e.Message)
}

func (e *WorkerError) Unwrap() error {
	return e.Cause
}

// IsRetryable returns true if this error may be resolved by retrying.
func (e *WorkerError) IsRetryable() bool {
	switch e.Kind {
	case ErrQueue, ErrBackend, ErrDatabase, ErrTimeoutW, ErrExecution, ErrScheduling:
		return true
	default:
		return false
	}
}

// WorkerErrorFromStorage converts a StorageError to a WorkerError.
func WorkerErrorFromStorage(err error) *WorkerError {
	se, ok := storage.IsStorageError(err)
	if !ok {
		return &WorkerError{Kind: ErrUnknown, Message: err.Error(), Cause: err}
	}
	switch se.Kind {
	case storage.ErrUnavailable:
		return &WorkerError{Kind: ErrBackend, Message: se.Message, Cause: err}
	case storage.ErrConflict:
		return &WorkerError{Kind: ErrQueue, Message: se.Message, Cause: err}
	case storage.ErrNotFound:
		return &WorkerError{Kind: ErrQueue, Message: se.Message, Cause: err}
	case storage.ErrInternal:
		return &WorkerError{Kind: ErrQueue, Message: se.Message, Cause: err}
	case storage.ErrSerialization:
		return &WorkerError{Kind: ErrQueue, Message: se.Message, Cause: err}
	case storage.ErrConfiguration:
		return &WorkerError{Kind: ErrConfiguration, Message: se.Message, Cause: err}
	case storage.ErrTimeout:
		return &WorkerError{Kind: ErrExecution, Message: se.Message, Cause: err}
	case storage.ErrDuplicateActivity:
		return &WorkerError{Kind: ErrDuplicateActivityW, Message: se.Message, Cause: err}
	case storage.ErrIdempotencyConflict:
		return &WorkerError{Kind: ErrIdempotencyConflictW, Message: se.Message, Cause: err}
	default:
		return &WorkerError{Kind: ErrUnknown, Message: se.Message, Cause: err}
	}
}

// IsSignalTimeout reports whether err is a WaitForSignal timeout.
func IsSignalTimeout(err error) bool {
	we, ok := IsWorkerError(err)
	return ok && we.Kind == ErrSignalTimeoutW
}

// IsActivityNotFound reports whether err is a signal-to-a-missing-activity
// error (the target was never enqueued, or has completed and been swept).
// External deliverers (webhooks, reconciliation) usually treat this as
// "already settled" rather than a failure.
func IsActivityNotFound(err error) bool {
	we, ok := IsWorkerError(err)
	return ok && we.Kind == ErrActivityNotFoundW
}

// IsWorkerError extracts a *WorkerError from err (if any).
func IsWorkerError(err error) (*WorkerError, bool) {
	var we *WorkerError
	if errors.As(err, &we) {
		return we, true
	}
	return nil, false
}
