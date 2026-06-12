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
		return WorkerErrorFromStorage(err)
	}
	return nil
}

// Signal delivers an external signal to an activity — see SignalActivity.
func (e *WorkerEngine) Signal(ctx context.Context, activityID uuid.UUID, name string, payload json.RawMessage) error {
	return SignalActivity(ctx, e.backend, activityID, name, payload)
}
