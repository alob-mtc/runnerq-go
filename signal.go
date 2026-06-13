package runnerq

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
)

// SignalActivity delivers an external signal to an activity from ANY process
// that shares the database — no engine required (e.g. a webhook receiver or
// approval service holding only a backend handle). The payload is persisted
// (buffered) even if the activity hasn't reached its WaitForSignal yet, and a
// parked waiter is woken immediately. payload may be nil for a pure
// notification. Repeated signals with the same name overwrite the payload.
//
// Returns a not-found error when no activity with that ID exists in the
// backend's queue.
func SignalActivity(ctx context.Context, backend storage.Storage, activityID uuid.UUID, name string, payload json.RawMessage) error {
	if name == "" {
		return &WorkerError{Kind: ErrQueue, Message: "signal name must be non-empty"}
	}
	sigID := deriveCheckpointID(activityID, "signal", name)
	if err := backend.SignalActivity(ctx, activityID, sigID, payload); err != nil {
		return signalDeliveryError(err)
	}
	return nil
}

// Signal delivers an external signal to an activity — see SignalActivity.
func (e *WorkerEngine) Signal(ctx context.Context, activityID uuid.UUID, name string, payload json.RawMessage) error {
	return SignalActivity(ctx, e.backend, activityID, name, payload)
}

// SignalActivityByKey delivers a signal to whichever activity currently owns
// (activityType, idempotencyKey) in the backend's queue, instead of addressing
// it by internal activity ID. This lets an external deliverer (a webhook, a
// reconciliation job) wake a durable workflow using the same business key the
// workflow was enqueued with — no client-side reference→ID bookkeeping required.
//
// activityType and idempotencyKey together must match what the workflow was
// enqueued with via IdempotencyKeyOption: keys are namespaced per activity type
// internally, so the type is required to resolve the right instance. The signal
// name is unchanged from SignalActivity: the key pair selects the workflow
// instance, the name selects which of its waits to satisfy. A key owns at most
// one activity (idempotency primary key), so there is no fan-out ambiguity.
// When the key is unclaimed (never enqueued, or completed and retention-swept)
// it returns an ErrActivityNotFound error (check with IsActivityNotFound) —
// typically treated as "already settled / nothing to wake".
//
// Resolution and delivery are two steps rather than one transaction; this only
// matters under key-reuse behaviors (AllowReuse/AllowReuseOnFailure) that
// repoint a key mid-flight, in which case the signal targets the owner at
// delivery time. For the common ReturnExisting workflow case the owner is
// stable, and the delivery itself (store + wake) remains atomic.
func SignalActivityByKey(ctx context.Context, backend storage.Storage, activityType string, idempotencyKey string, name string, payload json.RawMessage) error {
	if name == "" {
		return &WorkerError{Kind: ErrQueue, Message: "signal name must be non-empty"}
	}
	if activityType == "" {
		return &WorkerError{Kind: ErrQueue, Message: "activity type must be non-empty"}
	}
	if idempotencyKey == "" {
		return &WorkerError{Kind: ErrQueue, Message: "idempotency key must be non-empty"}
	}
	activityID, err := backend.LookupIdempotencyActivityID(ctx, idempotencyStorageKey(idempotencyKey, activityType))
	if err != nil {
		return signalDeliveryError(err)
	}
	return SignalActivity(ctx, backend, activityID, name, payload)
}

// SignalByKey delivers a signal addressed by (activityType, idempotencyKey) —
// see SignalActivityByKey.
func (e *WorkerEngine) SignalByKey(ctx context.Context, activityType string, idempotencyKey string, name string, payload json.RawMessage) error {
	return SignalActivityByKey(ctx, e.backend, activityType, idempotencyKey, name, payload)
}

// signalDeliveryError maps a storage not-found (missing/swept activity or
// unclaimed key) to the typed ErrActivityNotFound so callers can detect
// "nothing to signal" distinctly; other storage errors pass through.
func signalDeliveryError(err error) error {
	if se, ok := storage.IsStorageError(err); ok && se.Kind == storage.ErrNotFound {
		return &WorkerError{Kind: ErrActivityNotFoundW, Message: se.Message, Cause: err}
	}
	return WorkerErrorFromStorage(err)
}
