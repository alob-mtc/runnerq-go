package conductor

import (
	"context"
	"encoding/json"

	"github.com/alob-mtc/runnerq-go/storage"
)

// Handler serves the Cloud protocol's queries and commands from a storage
// backend, as an agent does, but without a worker engine or a connection:
// a service that holds a backend (such as RunnerQ Cloud's data plane)
// answers requests with it.
//
// Reads cover whatever the backend's queries cover (for the Postgres
// backend, every queue in its schema), and commands act on the backend's
// own queue. The Handler checks no caller identity: the service using it
// decides who may send what, and gives it a backend scoped to what they may
// see. It also sets each request's deadline and bounds its answer size.
type Handler struct {
	h     *handlers
	table map[string]handlerFunc
}

// HandlerConfig configures a Handler.
type HandlerConfig struct {
	// AllowControl serves commands (cancel, retry, run now, reschedule,
	// set priority, delete, signal) when the backend supports them and
	// reports its queue (QueueName() string); a command whose target names
	// another queue fails.
	AllowControl bool
}

// queueNamer is a backend that knows its own queue.
type queueNamer interface{ QueueName() string }

// Error is a failed request, as it goes on the wire.
type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// NewHandler returns a Handler for backend. It serves the query types when
// the backend implements storage.QueryStorage, and commands when it
// implements storage.CommandStorage, reports its queue and cfg allows them.
func NewHandler(backend storage.Storage, cfg HandlerConfig) *Handler {
	qs, _ := backend.(storage.QueryStorage)
	cs, _ := backend.(storage.CommandStorage)
	// Commands are checked against the queue the backend acts on, never a
	// name from elsewhere: without one, there are none.
	named, ok := backend.(queueNamer)
	if !ok {
		cs = nil
	}
	h := &handlers{backend: backend, qs: qs, cs: cs, allowControl: cfg.AllowControl}
	if ok {
		h.queue = named.QueueName()
	}
	t := h.table()
	delete(t, typeExecutorDescribe) // there's no executor
	return &Handler{h: h, table: t}
}

// Serve answers one request: msgType with its data, as in the request
// envelope. It returns the response data, or the error to send instead.
func (h *Handler) Serve(ctx context.Context, msgType string, data json.RawMessage) (res json.RawMessage, werr *Error) {
	defer func() {
		if p := recover(); p != nil {
			res, werr = nil, wire(errorf(codeInternal, "handler panicked"))
		}
	}()
	serve, ok := h.table[msgType]
	if !ok {
		return nil, wire(errorf(codeUnsupported, "this handler does not serve %q", msgType))
	}
	out, err := serve(ctx, data)
	if err != nil {
		return nil, wire(toWireError(err))
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		return nil, wire(errorf(codeInternal, "encode response: %v", err))
	}
	return encoded, nil
}

// Capabilities describes what the handler serves, as an agent's hello
// does: a JSON object keyed by message type.
func (h *Handler) Capabilities() json.RawMessage {
	caps := h.h.capabilities()
	delete(caps, typeExecutorDescribe)
	// Live events need a subscription on a connection, which a Handler
	// doesn't have.
	delete(caps, typeEventsSubscribe)
	delete(caps, typeEventsUnsubscribe)
	b, _ := json.Marshal(caps)
	return b
}

func wire(e *wireError) *Error {
	return &Error{Code: string(e.Code), Message: e.Message, Details: e.Details}
}
