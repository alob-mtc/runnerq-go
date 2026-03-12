package runnerq

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
)

// ActivityContext is provided to activity handlers during execution.
type ActivityContext struct {
	// ActivityID is the unique identifier for this activity instance.
	ActivityID uuid.UUID

	// ActivityType is the type of activity being executed.
	ActivityType string

	// RetryCount is the current retry attempt number (0 for first execution).
	RetryCount uint32

	// Metadata is custom metadata associated with the activity.
	Metadata map[string]string

	// Ctx is the Go context for cancellation and deadline propagation.
	Ctx context.Context

	// ActivityExecutor allows executing other activities from within a handler.
	ActivityExecutor ActivityExecutor
}

// ActivityHandler is the interface that all activity handlers must implement.
// Implementations should be safe for concurrent use.
type ActivityHandler interface {
	// ActivityType returns the activity type string this handler processes.
	ActivityType() string

	// Handle processes the activity with the given payload and context.
	// Returns:
	//   (result, nil)       - completed successfully, result may be nil
	//   (nil, RetryError)   - failed but should be retried
	//   (nil, NonRetryError)- failed permanently
	Handle(ctx ActivityContext, payload json.RawMessage) (json.RawMessage, error)

	// OnDeadLetter is called when an activity enters the dead letter state.
	// The default behavior (if not overridden by embedding DefaultDeadLetterHandler) is a no-op.
	OnDeadLetter(ctx ActivityContext, payload json.RawMessage, errorMsg string)
}

// DefaultDeadLetterHandler provides a no-op OnDeadLetter implementation.
// Embed this in your handler struct if you don't need dead letter handling.
type DefaultDeadLetterHandler struct{}

// OnDeadLetter is a no-op implementation.
func (DefaultDeadLetterHandler) OnDeadLetter(_ ActivityContext, _ json.RawMessage, _ string) {}
