package storage

import (
	"errors"
	"fmt"
)

// StorageErrorKind classifies storage errors.
type StorageErrorKind int

const (
	ErrUnavailable StorageErrorKind = iota
	ErrConflict
	ErrNotFound
	ErrInternal
	ErrSerialization
	ErrConfiguration
	ErrTimeout
	ErrDuplicateActivity
	ErrIdempotencyConflict
)

// StorageError represents a backend-agnostic error from storage operations.
type StorageError struct {
	Kind    StorageErrorKind
	Message string
	Cause   error
}

func (e *StorageError) Error() string {
	prefix := ""
	switch e.Kind {
	case ErrUnavailable:
		prefix = "backend unavailable"
	case ErrConflict:
		prefix = "conflict"
	case ErrNotFound:
		prefix = "not found"
	case ErrInternal:
		prefix = "internal error"
	case ErrSerialization:
		prefix = "serialization error"
	case ErrConfiguration:
		prefix = "configuration error"
	case ErrTimeout:
		prefix = "operation timeout"
	case ErrDuplicateActivity:
		prefix = "duplicate activity"
	case ErrIdempotencyConflict:
		prefix = "idempotency conflict"
	}
	return fmt.Sprintf("%s: %s", prefix, e.Message)
}

func (e *StorageError) Unwrap() error {
	return e.Cause
}

// IsRetryable returns true if this error is potentially recoverable with a retry.
func (e *StorageError) IsRetryable() bool {
	switch e.Kind {
	case ErrUnavailable, ErrTimeout, ErrConflict:
		return true
	default:
		return false
	}
}

// Constructors for each error kind.

func NewUnavailableError(msg string) *StorageError {
	return &StorageError{Kind: ErrUnavailable, Message: msg}
}

func NewConflictError(msg string) *StorageError {
	return &StorageError{Kind: ErrConflict, Message: msg}
}

func NewNotFoundError(msg string) *StorageError {
	return &StorageError{Kind: ErrNotFound, Message: msg}
}

func NewInternalError(msg string) *StorageError {
	return &StorageError{Kind: ErrInternal, Message: msg}
}

func NewSerializationError(msg string) *StorageError {
	return &StorageError{Kind: ErrSerialization, Message: msg}
}

func NewConfigurationError(msg string) *StorageError {
	return &StorageError{Kind: ErrConfiguration, Message: msg}
}

func NewTimeoutError(msg string) *StorageError {
	return &StorageError{Kind: ErrTimeout, Message: msg}
}

func NewDuplicateActivityError(msg string) *StorageError {
	return &StorageError{Kind: ErrDuplicateActivity, Message: msg}
}

func NewIdempotencyConflictError(msg string) *StorageError {
	return &StorageError{Kind: ErrIdempotencyConflict, Message: msg}
}

// IsStorageError extracts a *StorageError from err (if any).
func IsStorageError(err error) (*StorageError, bool) {
	var se *StorageError
	if errors.As(err, &se) {
		return se, true
	}
	return nil, false
}
