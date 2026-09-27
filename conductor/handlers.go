package conductor

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go"
	"github.com/alob-mtc/runnerq-go/observability"
	"github.com/alob-mtc/runnerq-go/storage"
)

const (
	defaultLimit = 50
	maxLimit     = 1000
)

// handlerFunc serves one request type. Returning a *wireError picks the error
// code; any other error is reported as internal.
type handlerFunc func(ctx context.Context, data json.RawMessage) (any, error)

// handlers serves the Conductor request types from the engine's storage.
type handlers struct {
	engine       *runnerq.WorkerEngine
	backend      storage.Storage
	inspector    *observability.QueueInspector
	allowControl bool
	// metadataOnly strips payloads, results, errors and step data from
	// responses. Set from the gateway's welcome on every connection.
	metadataOnly atomic.Bool
}

func newHandlers(engine *runnerq.WorkerEngine, allowControl bool) *handlers {
	backend := engine.Backend()
	return &handlers{
		engine:       engine,
		backend:      backend,
		inspector:    observability.NewQueueInspector(backend).WithMaxWorkers(engine.MaxConcurrentActivities()),
		allowControl: allowControl,
	}
}

func (h *handlers) table() map[string]handlerFunc {
	return map[string]handlerFunc{
		typeStats:             h.stats,
		typeListActivities:    h.listActivities,
		typeListRoots:         h.listRoots,
		typeGetActivity:       h.getActivity,
		typeGetActivityEvents: h.getActivityEvents,
		typeGetActivitySteps:  h.getActivitySteps,
		typeGetActivityResult: h.getActivityResult,
		typeGetChildren:       h.getChildren,
		typeGetSubtree:        h.getSubtree,
		typeListDeadLetter:    h.listDeadLetter,
		typeSignal:            h.signal,
	}
}

// capabilities lists the request types this agent serves, sorted.
func (h *handlers) capabilities() []string {
	caps := make([]string, 0, len(h.table()))
	for t := range h.table() {
		caps = append(caps, t)
	}
	slices.Sort(caps)
	return caps
}

func decode[T any](data json.RawMessage) (T, error) {
	var v T
	if len(data) == 0 {
		return v, nil
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return v, errorf(codeInvalidArgument, "decode request: %v", err)
	}
	return v, nil
}

func parseID(s string) (uuid.UUID, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, errorf(codeInvalidArgument, "id must be a uuid")
	}
	return id, nil
}

func (p page) window() (offset, limit int) {
	offset, limit = max(p.Offset, 0), p.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	return offset, min(limit, maxLimit)
}

func (h *handlers) setMetadataOnly(on bool) { h.metadataOnly.Store(on) }

// --- reads ---

func (h *handlers) stats(ctx context.Context, _ json.RawMessage) (any, error) {
	return h.inspector.Stats(ctx)
}

func (h *handlers) listActivities(ctx context.Context, data json.RawMessage) (any, error) {
	req, err := decode[listRequest](data)
	if err != nil {
		return nil, err
	}
	offset, limit := req.window()
	var items []observability.ActivitySnapshot
	switch req.Status {
	case "pending":
		items, err = h.inspector.ListPending(ctx, offset, limit)
	case "processing":
		items, err = h.inspector.ListProcessing(ctx, offset, limit)
	case "scheduled":
		items, err = h.inspector.ListScheduled(ctx, offset, limit)
	case "completed":
		items, err = h.inspector.ListCompleted(ctx, offset, limit)
	case "cron":
		items, err = h.inspector.ListCronCompleted(ctx, offset, limit)
	case "dead_letter":
		var records []observability.DeadLetterRecord
		records, err = h.inspector.ListDeadLetter(ctx, offset, limit)
		for _, r := range records {
			items = append(items, r.Activity)
		}
	default:
		return nil, errorf(codeInvalidArgument, "unknown status %q", req.Status)
	}
	if err != nil {
		return nil, err
	}
	return h.snapshots(items), nil
}

// rootStatuses mirrors the console's filter allowlist; "" means no filter.
var rootStatuses = map[string]bool{
	"": true, "pending": true, "processing": true, "scheduled": true,
	"retrying": true, "completed": true, "failed": true, "dead_letter": true,
}

func (h *handlers) listRoots(ctx context.Context, data json.RawMessage) (any, error) {
	req, err := decode[listRequest](data)
	if err != nil {
		return nil, err
	}
	if !rootStatuses[req.Status] {
		return nil, errorf(codeInvalidArgument, "unknown status %q", req.Status)
	}
	offset, limit := req.window()
	items, err := h.inspector.ListRecentRoots(ctx, req.Status, offset, limit)
	if err != nil {
		return nil, err
	}
	return h.snapshots(items), nil
}

// activity loads one activity or fails with not_found.
func (h *handlers) activity(ctx context.Context, rawID string) (*observability.ActivitySnapshot, error) {
	id, err := parseID(rawID)
	if err != nil {
		return nil, err
	}
	act, err := h.inspector.GetActivity(ctx, id)
	if err != nil {
		return nil, err
	}
	if act == nil {
		return nil, errorf(codeNotFound, "activity %s not found", id)
	}
	return act, nil
}

func (h *handlers) getActivity(ctx context.Context, data json.RawMessage) (any, error) {
	req, err := decode[activityRef](data)
	if err != nil {
		return nil, err
	}
	act, err := h.activity(ctx, req.ID)
	if err != nil {
		return nil, err
	}
	return h.snapshot(*act), nil
}

func (h *handlers) getActivityEvents(ctx context.Context, data json.RawMessage) (any, error) {
	req, err := decode[activityPage](data)
	if err != nil {
		return nil, err
	}
	id, err := parseID(req.ID)
	if err != nil {
		return nil, err
	}
	_, limit := req.window()
	events, err := h.inspector.RecentEvents(ctx, id, limit)
	if err != nil {
		return nil, err
	}
	if h.metadataOnly.Load() {
		for i := range events {
			events[i].Detail = nil
		}
	}
	return nonNil(events), nil
}

func (h *handlers) getActivitySteps(ctx context.Context, data json.RawMessage) (any, error) {
	req, err := decode[activityRef](data)
	if err != nil {
		return nil, err
	}
	id, err := parseID(req.ID)
	if err != nil {
		return nil, err
	}
	steps, err := h.inspector.GetActivitySteps(ctx, id)
	if err != nil {
		return nil, err
	}
	if h.metadataOnly.Load() {
		for i := range steps {
			steps[i].Data = nil
		}
	}
	return nonNil(steps), nil
}

// activityResult is an activity's stored result on the wire.
type activityResult struct {
	State string          `json:"state"` // "Ok" | "Err"
	Data  json.RawMessage `json:"data,omitempty"`
}

func (h *handlers) getActivityResult(ctx context.Context, data json.RawMessage) (any, error) {
	if h.metadataOnly.Load() {
		return nil, errorf(codeForbidden, "results are not sent in metadata-only mode")
	}
	req, err := decode[activityRef](data)
	if err != nil {
		return nil, err
	}
	id, err := parseID(req.ID)
	if err != nil {
		return nil, err
	}
	res, err := h.backend.GetResult(ctx, id)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, errorf(codeNotFound, "no result stored for activity %s", id)
	}
	state := "Ok"
	if res.State == storage.ResultErr {
		state = "Err"
	}
	return activityResult{State: state, Data: res.Data}, nil
}

func (h *handlers) getChildren(ctx context.Context, data json.RawMessage) (any, error) {
	req, err := decode[activityPage](data)
	if err != nil {
		return nil, err
	}
	id, err := parseID(req.ID)
	if err != nil {
		return nil, err
	}
	offset, limit := req.window()
	items, err := h.inspector.GetChildren(ctx, id, offset, limit)
	if err != nil {
		return nil, err
	}
	return h.snapshots(items), nil
}

// getSubtree returns the whole tree the activity belongs to, resolving a
// child to its root first.
func (h *handlers) getSubtree(ctx context.Context, data json.RawMessage) (any, error) {
	req, err := decode[activityRef](data)
	if err != nil {
		return nil, err
	}
	act, err := h.activity(ctx, req.ID)
	if err != nil {
		return nil, err
	}
	root := act.ID
	if act.RootActivityID != nil {
		root = *act.RootActivityID
	}
	items, err := h.inspector.GetSubtree(ctx, root)
	if err != nil {
		return nil, err
	}
	return h.snapshots(items), nil
}

func (h *handlers) listDeadLetter(ctx context.Context, data json.RawMessage) (any, error) {
	req, err := decode[page](data)
	if err != nil {
		return nil, err
	}
	offset, limit := req.window()
	records, err := h.inspector.ListDeadLetter(ctx, offset, limit)
	if err != nil {
		return nil, err
	}
	for i := range records {
		records[i].Activity = h.snapshot(records[i].Activity)
		if h.metadataOnly.Load() {
			records[i].Error = ""
		}
	}
	return nonNil(records), nil
}

// --- control ---

func (h *handlers) signal(ctx context.Context, data json.RawMessage) (any, error) {
	if !h.allowControl {
		return nil, errorf(codeForbidden, "control actions are disabled on this agent")
	}
	req, err := decode[signalRequest](data)
	if err != nil {
		return nil, err
	}
	if req.Name == "" {
		return nil, errorf(codeInvalidArgument, "signal name is required")
	}
	switch {
	case req.ID != "" && req.Key == "":
		id, perr := parseID(req.ID)
		if perr != nil {
			return nil, perr
		}
		err = h.engine.Signal(ctx, id, req.Name, req.Payload)
	case req.Key != "" && req.ID == "":
		if req.ActivityType == "" {
			return nil, errorf(codeInvalidArgument, "activity_type is required to signal by key")
		}
		err = h.engine.SignalByKey(ctx, req.ActivityType, req.Key, req.Name, req.Payload)
	default:
		return nil, errorf(codeInvalidArgument, "set exactly one of id or key")
	}
	if runnerq.IsActivityNotFound(err) {
		return nil, errorf(codeNotFound, "no activity to signal")
	}
	if err != nil {
		return nil, err
	}
	return controlResult{}, nil
}

// --- redaction ---

func (h *handlers) snapshot(s observability.ActivitySnapshot) observability.ActivitySnapshot {
	if h.metadataOnly.Load() {
		s.Payload = nil
		s.LastError = nil
	}
	return s
}

func (h *handlers) snapshots(items []observability.ActivitySnapshot) []observability.ActivitySnapshot {
	out := make([]observability.ActivitySnapshot, 0, len(items))
	for _, s := range items {
		out = append(out, h.snapshot(s))
	}
	return out
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

// errorMessage keeps internal error text short on the wire.
func errorMessage(err error) string {
	msg := err.Error()
	if i := strings.IndexByte(msg, '\n'); i >= 0 {
		msg = msg[:i]
	}
	return msg
}
