package conductor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go"
	"github.com/alob-mtc/runnerq-go/executor"
	"github.com/alob-mtc/runnerq-go/storage"
)

// maxEmbedded bounds steps and events embedded in activities.get.
const maxEmbedded = 1000

// handlerFunc returns a *wireError to pick the error code; other errors go
// through toWireError.
type handlerFunc func(ctx context.Context, data json.RawMessage) (any, error)

type handlers struct {
	engine  *runnerq.WorkerEngine // nil in a Handler
	backend storage.Storage
	// queue is the queue commands act on.
	queue        string
	qs           storage.QueryStorage   // nil when unsupported
	cs           storage.CommandStorage // nil when unsupported
	allowControl bool
	// forceMetadataOnly is the local setting; it always wins over
	// cloudMetadataOnly (welcome/config.update).
	forceMetadataOnly bool
	cloudMetadataOnly atomic.Bool
	started           time.Time
}

func newHandlers(engine *runnerq.WorkerEngine, forceMetadataOnly, allowControl bool) *handlers {
	backend := engine.Backend()
	qs, _ := backend.(storage.QueryStorage)
	cs, _ := backend.(storage.CommandStorage)
	return &handlers{engine: engine, backend: backend, queue: engine.QueueName(), qs: qs, cs: cs, allowControl: allowControl,
		forceMetadataOnly: forceMetadataOnly, started: time.Now()}
}

func (h *handlers) metadataOnly() bool { return h.forceMetadataOnly || h.cloudMetadataOnly.Load() }

func (h *handlers) table() map[string]handlerFunc {
	t := map[string]handlerFunc{typeExecutorDescribe: h.executorDescribe}
	if h.qs != nil {
		t[typeActivitiesList] = h.activitiesList
		t[typeActivitiesGet] = h.activitiesGet
		t[typeActivitiesCount] = h.activitiesCount
		t[typeActivitiesAggregate] = h.activitiesAggregate
		t[typeStepsList] = h.stepsList
		t[typeEventsList] = h.eventsList
		t[typeResultsGet] = h.resultsGet
		t[typeTreesGet] = h.treesGet
	}
	h.addCommands(t)
	return t
}

var (
	recordIncludes = []string{"last_error", "payload", "result"}
	getIncludes    = []string{"events", "last_error", "payload", "result", "steps"}
)

func (h *handlers) capabilities() map[string]capability {
	caps := map[string]capability{typeExecutorDescribe: {V: 1}}
	h.commandCapabilities(caps)
	if h.qs == nil {
		return caps
	}
	qc := h.qs.QueryCapabilities()
	caps[typeActivitiesList] = capability{V: 1, Filters: qc.ActivityFilters, Sorts: qc.ActivitySorts, Include: recordIncludes}
	caps[typeActivitiesGet] = capability{V: 1, Include: getIncludes}
	caps[typeActivitiesCount] = capability{V: 1, Filters: qc.ActivityFilters}
	metrics := []string{"count"}
	for _, d := range qc.Durations {
		metrics = append(metrics, "duration."+d)
	}
	caps[typeActivitiesAggregate] = capability{V: 1, Filters: qc.ActivityFilters, GroupBy: qc.GroupBy, Buckets: qc.Buckets, Metrics: metrics}
	caps[typeStepsList] = capability{V: 1, Include: []string{"result"}}
	caps[typeEventsList] = capability{V: 1, Filters: qc.EventFilters, Sorts: []string{"at"}, Include: []string{"detail"}}
	caps[typeResultsGet] = capability{V: 1}
	caps[typeTreesGet] = capability{V: 1, Include: recordIncludes}
	caps[typeEventsSubscribe] = capability{V: 1, Filters: qc.EventFilters}
	caps[typeEventsUnsubscribe] = capability{V: 1}
	return caps
}

func decode[T any](data json.RawMessage) (T, error) {
	var v T
	if len(data) == 0 {
		return v, nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields() // an unknown request field could change meaning
	if err := dec.Decode(&v); err != nil {
		return v, errorf(codeInvalidArgument, "decode request: %v", err)
	}
	return v, nil
}

// toStorageFilter keeps filter values in their JSON types.
func toStorageFilter(f *wireFilter) (*storage.QueryFilter, error) {
	if f == nil {
		return nil, nil
	}
	out := &storage.QueryFilter{Field: f.Field, Op: f.Op}
	if len(f.Value) > 0 {
		if err := json.Unmarshal(f.Value, &out.Value); err != nil {
			return nil, fieldError(codeInvalidArgument, f.Field, "invalid filter value")
		}
	}
	for _, t := range f.And {
		c, err := toStorageFilter(&t)
		if err != nil {
			return nil, err
		}
		out.And = append(out.And, *c)
	}
	for _, t := range f.Or {
		c, err := toStorageFilter(&t)
		if err != nil {
			return nil, err
		}
		out.Or = append(out.Or, *c)
	}
	if f.Not != nil {
		c, err := toStorageFilter(f.Not)
		if err != nil {
			return nil, err
		}
		out.Not = c
	}
	return out, nil
}

// includes refuses anything carrying customer data in metadata-only mode.
func (h *handlers) includes(list, allowed []string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, inc := range list {
		if !slices.Contains(allowed, inc) {
			return nil, fieldError(codeUnsupported, "include", "cannot include %q", inc)
		}
		if h.metadataOnly() && (inc == "payload" || inc == "result" || inc == "last_error" || inc == "detail") {
			return nil, fieldError(codeForbidden, "include", "%s is not sent in metadata-only mode", inc)
		}
		out[inc] = true
	}
	return out, nil
}

func recordInclude(inc map[string]bool) storage.RecordInclude {
	return storage.RecordInclude{Payload: inc["payload"], Result: inc["result"], LastError: inc["last_error"]}
}

// parseID reports false for an id this backend could not have issued; on
// the wire such an id simply does not exist.
func parseID(s string) (uuid.UUID, bool) {
	id, err := uuid.Parse(s)
	return id, err == nil
}

func ts(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z07:00") }

func tsp(t *time.Time) string {
	if t == nil {
		return ""
	}
	return ts(*t)
}

func toResult(r *storage.ActivityResult) *result {
	if r == nil {
		return nil
	}
	if r.State == storage.ResultOk {
		return &result{State: "ok", Data: plainJSON(r)}
	}
	var e struct {
		Error string `json:"error"`
		Type  string `json:"type"`
	}
	if json.Unmarshal(r.Data, &e) == nil && e.Error != "" {
		return &result{State: "error", Error: &errorInfo{Message: e.Error, Kind: e.Type}}
	}
	return &result{State: "error", Data: r.Data}
}

func toActivity(r storage.ActivityRecord) activityView {
	v := activityView{
		ID: r.ID.String(), Type: r.Type, Queue: r.Queue, Status: r.Status, Priority: r.Priority,
		RootID: r.RootID.String(), Depth: r.Depth, IdempotencyKey: r.IdempotencyKey,
		Attempt: r.Attempt, MaxAttempts: r.MaxAttempts, CreatedAt: ts(r.CreatedAt),
		ScheduledFor: tsp(r.ScheduledFor), StartedAt: tsp(r.StartedAt), CompletedAt: tsp(r.CompletedAt),
		UpdatedAt: ts(r.UpdatedAt), TimeoutMS: r.Timeout.Milliseconds(), LeaseExpiresAt: tsp(r.LeaseExpiresAt),
		ExecutorID: r.ExecutorID, Metadata: r.Metadata, Payload: r.Payload, Result: toResult(r.Result),
	}
	if r.ParentID != nil {
		v.ParentID = r.ParentID.String()
	}
	if r.Wait != nil {
		v.Wait = &wait{Kind: r.Wait.Kind, Name: r.Wait.Name, Until: tsp(r.Wait.Until)}
	}
	if r.LastError != nil {
		v.LastError = &errorInfo{Message: r.LastError.Message, Kind: r.LastError.Kind, At: tsp(r.LastError.At)}
	}
	return v
}

func toActivities(items []storage.ActivityRecord) []activityView {
	out := make([]activityView, 0, len(items))
	for _, r := range items {
		out = append(out, toActivity(r))
	}
	return out
}

func toStep(s storage.StepEntry) stepView {
	v := stepView{
		ID: s.ID.String(), ActivityID: s.ActivityID.String(), Name: s.Name, Kind: s.Kind,
		Status: "completed", CreatedAt: ts(s.CreatedAt),
	}
	if s.State != storage.ResultOk {
		v.Status = "failed"
	}
	if s.Data != nil {
		v.Result = toResult(&storage.ActivityResult{State: s.State, Data: s.Data})
	}
	return v
}

func toEvent(e storage.EventRecord) eventView {
	return eventView{
		ID: e.Cursor, Cursor: e.Cursor, ActivityID: e.ActivityID.String(), Type: e.Type,
		At: ts(e.At), ExecutorID: e.ExecutorID, Detail: e.Detail,
	}
}

func (h *handlers) activitiesList(ctx context.Context, data json.RawMessage) (any, error) {
	q, err := decode[query](data)
	if err != nil {
		return nil, err
	}
	filter, err := toStorageFilter(q.Filter)
	if err != nil {
		return nil, err
	}
	inc, err := h.includes(q.Include, recordIncludes)
	if err != nil {
		return nil, err
	}
	sq := storage.ActivityQuery{Filter: filter, Include: recordInclude(inc), Limit: q.Limit, Cursor: q.Cursor}
	if sort, err := oneSort(q.Sort); err != nil {
		return nil, err
	} else if sort != nil {
		sq.Sort = sort
	}
	res, err := h.qs.QueryActivities(ctx, sq)
	if err != nil {
		return nil, err
	}
	return page[activityView]{Items: toActivities(res.Items), NextCursor: res.NextCursor}, nil
}

// oneSort accepts at most one sort key (the backend adds the tiebreaker).
func oneSort(sorts []wireSort) (*storage.QuerySort, error) {
	switch len(sorts) {
	case 0:
		return nil, nil
	case 1:
	default:
		return nil, fieldError(codeUnsupported, "sort", "only one sort key is supported")
	}
	s := sorts[0]
	switch s.Order {
	case "", "desc":
		return &storage.QuerySort{Field: s.Field, Desc: true}, nil
	case "asc":
		return &storage.QuerySort{Field: s.Field}, nil
	}
	return nil, fieldError(codeInvalidArgument, "sort", "order must be asc or desc")
}

func (h *handlers) activitiesGet(ctx context.Context, data json.RawMessage) (any, error) {
	req, err := decode[getRequest](data)
	if err != nil {
		return nil, err
	}
	inc, err := h.includes(req.Include, getIncludes)
	if err != nil {
		return nil, err
	}
	id, ok := parseID(req.ID)
	if !ok {
		return nil, errorf(codeNotFound, "activity %q not found", req.ID)
	}
	res, err := h.qs.QueryActivities(ctx, storage.ActivityQuery{
		Filter:  &storage.QueryFilter{Field: "id", Op: storage.OpEq, Value: id.String()},
		Include: recordInclude(inc), Limit: 1,
	})
	if err != nil {
		return nil, err
	}
	if len(res.Items) == 0 {
		return nil, errorf(codeNotFound, "activity %q not found", req.ID)
	}
	v := toActivity(res.Items[0])
	if inc["steps"] {
		steps, err := h.qs.ListStepEntries(ctx, id, false, maxEmbedded, "")
		if err != nil {
			return nil, err
		}
		list := make([]stepView, 0, len(steps.Items))
		for _, s := range steps.Items {
			list = append(list, toStep(s))
		}
		v.Steps = &list
	}
	if inc["events"] {
		events, err := h.qs.QueryEvents(ctx, storage.EventQuery{
			Filter: &storage.QueryFilter{Field: "activity_id", Op: storage.OpEq, Value: id.String()},
			Limit:  maxEmbedded,
		})
		if err != nil {
			return nil, err
		}
		list := make([]eventView, 0, len(events.Items))
		for _, e := range events.Items {
			list = append(list, toEvent(e))
		}
		v.Events = &list
	}
	return v, nil
}

func (h *handlers) activitiesCount(ctx context.Context, data json.RawMessage) (any, error) {
	req, err := decode[countRequest](data)
	if err != nil {
		return nil, err
	}
	filter, err := toStorageFilter(req.Filter)
	if err != nil {
		return nil, err
	}
	n, exact, err := h.qs.CountActivities(ctx, filter, 100_000)
	if err != nil {
		return nil, err
	}
	return countResult{Count: n, Exact: exact}, nil
}

func parseTime(field, s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return nil, fieldError(codeInvalidArgument, field, "expected an RFC 3339 timestamp")
	}
	return &t, nil
}

func (h *handlers) activitiesAggregate(ctx context.Context, data json.RawMessage) (any, error) {
	req, err := decode[aggregateRequest](data)
	if err != nil {
		return nil, err
	}
	filter, err := toStorageFilter(req.Filter)
	if err != nil {
		return nil, err
	}
	q := storage.AggregateQuery{Filter: filter, GroupBy: req.GroupBy, Limit: req.Limit}
	for _, m := range req.Metrics {
		switch m.Name {
		case "count":
			q.Count = true
		case "duration":
			q.Durations = append(q.Durations, storage.DurationMetric{Field: m.Field, Percentiles: m.Percentiles})
		default:
			return nil, fieldError(codeUnsupported, "metrics", "unknown metric %q", m.Name)
		}
	}
	if b := req.Bucket; b != nil {
		from, err := parseTime("bucket.from", b.From)
		if err != nil {
			return nil, err
		}
		to, err := parseTime("bucket.to", b.To)
		if err != nil {
			return nil, err
		}
		q.Bucket = &storage.AggregateBucket{Field: b.Field, Interval: time.Duration(b.IntervalMS) * time.Millisecond, From: from, To: to}
	}
	rows, err := h.qs.AggregateActivities(ctx, q)
	if err != nil {
		return nil, err
	}
	out := aggregateResult{Groups: make([]aggregateGroup, 0, len(rows.Rows)), Truncated: rows.Truncated}
	for _, r := range rows.Rows {
		g := aggregateGroup{Key: r.Key, Bucket: tsp(r.Bucket), Durations: r.Durations}
		if q.Count {
			n := r.Count
			g.Count = &n
		}
		out.Groups = append(out.Groups, g)
	}
	return out, nil
}

func (h *handlers) stepsList(ctx context.Context, data json.RawMessage) (any, error) {
	req, err := decode[stepsRequest](data)
	if err != nil {
		return nil, err
	}
	inc, err := h.includes(req.Include, []string{"result"})
	if err != nil {
		return nil, err
	}
	id, ok := parseID(req.ActivityID)
	if !ok {
		return page[stepView]{Items: []stepView{}}, nil
	}
	res, err := h.qs.ListStepEntries(ctx, id, inc["result"], req.Limit, req.Cursor)
	if err != nil {
		return nil, err
	}
	out := page[stepView]{Items: make([]stepView, 0, len(res.Items)), NextCursor: res.NextCursor}
	for _, s := range res.Items {
		out.Items = append(out.Items, toStep(s))
	}
	return out, nil
}

func (h *handlers) eventsList(ctx context.Context, data json.RawMessage) (any, error) {
	q, err := decode[query](data)
	if err != nil {
		return nil, err
	}
	filter, err := toStorageFilter(q.Filter)
	if err != nil {
		return nil, err
	}
	inc, err := h.includes(q.Include, []string{"detail"})
	if err != nil {
		return nil, err
	}
	eq := storage.EventQuery{Filter: filter, Limit: q.Limit, Cursor: q.Cursor, IncludeDetail: inc["detail"]}
	if sort, err := oneSort(q.Sort); err != nil {
		return nil, err
	} else if sort != nil {
		if sort.Field != "at" {
			return nil, fieldError(codeUnsupported, "sort", "events sort by at only")
		}
		eq.Desc = sort.Desc
	}
	res, err := h.qs.QueryEvents(ctx, eq)
	if err != nil {
		return nil, err
	}
	out := page[eventView]{Items: make([]eventView, 0, len(res.Items)), NextCursor: res.NextCursor}
	for _, e := range res.Items {
		out.Items = append(out.Items, toEvent(e))
	}
	return out, nil
}

func (h *handlers) resultsGet(ctx context.Context, data json.RawMessage) (any, error) {
	if h.metadataOnly() {
		return nil, errorf(codeForbidden, "results are not sent in metadata-only mode")
	}
	req, err := decode[resultRequest](data)
	if err != nil {
		return nil, err
	}
	id, ok := parseID(req.ActivityID)
	if !ok {
		return nil, errorf(codeNotFound, "no result for activity %q", req.ActivityID)
	}
	res, err := h.backend.GetResult(ctx, id)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, errorf(codeNotFound, "no result for activity %q", req.ActivityID)
	}
	return toResult(res), nil
}

func (h *handlers) treesGet(ctx context.Context, data json.RawMessage) (any, error) {
	req, err := decode[treeRequest](data)
	if err != nil {
		return nil, err
	}
	inc, err := h.includes(req.Include, recordIncludes)
	if err != nil {
		return nil, err
	}
	id, ok := parseID(req.ID)
	if !ok {
		return nil, errorf(codeNotFound, "activity %q not found", req.ID)
	}
	tree, err := h.qs.GetActivityTree(ctx, id, recordInclude(inc), req.MaxNodes)
	if err != nil {
		return nil, err
	}
	return treeView{RootID: tree.RootID.String(), Items: toActivities(tree.Items), Truncated: tree.Truncated}, nil
}

// state includes the in-flight list when withRunning (describe), not just
// its size (periodic reports).
func (h *handlers) state(withRunning bool) executorState {
	return stateOf(h.engine.Snapshot(), h.started, withRunning)
}

// stateOf counts uptime from started when the engine hasn't started.
func stateOf(snap executor.Snapshot, started time.Time, withRunning bool) executorState {
	if !snap.Info.StartedAt.IsZero() {
		started = snap.Info.StartedAt
	}
	c := snap.Counters
	st := executorState{
		ID:                snap.Info.ID,
		UptimeMS:          snap.At.Sub(started).Milliseconds(),
		MaxConcurrency:    snap.Info.MaxConcurrency,
		InFlight:          len(snap.State.Running),
		Draining:          snap.State.Draining,
		ClaimLagMS:        c.LastClaimLag.Milliseconds(),
		HeartbeatFailures: c.HeartbeatFailures,
		Counters: &executorCounters{
			Claimed: c.Claimed, Succeeded: c.Succeeded, Retried: c.Retried, Failed: c.Failed,
			TimedOut: c.TimedOut, DeadLettered: c.DeadLettered, ClaimsLost: c.ClaimsLost,
		},
	}
	if withRunning {
		for _, a := range snap.State.Running {
			st.Running = append(st.Running, runningActivity{
				ActivityID: a.ID.String(), Type: a.Type, Attempt: a.Attempt, StartedAt: ts(a.StartedAt),
			})
		}
	}
	return st
}

func (h *handlers) executorDescribe(context.Context, json.RawMessage) (any, error) {
	return h.state(true), nil
}

func toWireError(err error) *wireError {
	var we *wireError
	if errors.As(err, &we) {
		return we
	}
	var se *storage.StorageError
	if errors.As(err, &se) {
		var e *wireError
		switch se.Kind {
		case storage.ErrInvalidArgument:
			e = errorf(codeInvalidArgument, "%s", se.Message)
		case storage.ErrUnsupported:
			e = errorf(codeUnsupported, "%s", se.Message)
		case storage.ErrNotFound:
			e = errorf(codeNotFound, "%s", se.Message)
		case storage.ErrConflict:
			e = errorf(codeConflict, "%s", se.Message)
		case storage.ErrUnavailable, storage.ErrTimeout:
			e = errorf(codeUnavailable, "%s", se.Message)
		default:
			e = errorf(codeInternal, "%s", errorMessage(err))
		}
		if se.Field != "" {
			e.Details = map[string]any{"field": se.Field}
		}
		return e
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errorf(codeDeadlineExceeded, "the request ran past its deadline")
	}
	return errorf(codeInternal, "%s", errorMessage(err))
}

// errorMessage keeps internal error text to one line on the wire.
func errorMessage(err error) string {
	msg := err.Error()
	if i := strings.IndexByte(msg, '\n'); i >= 0 {
		msg = msg[:i]
	}
	return msg
}

// plainJSON unwraps the "json" part of the TypeScript SDK's superjson-v1
// results, which is what the console reads; other data passes through.
func plainJSON(r *storage.ActivityResult) json.RawMessage {
	if r.Serialization != "superjson-v1" {
		return r.Data
	}
	var envelope struct {
		JSON json.RawMessage `json:"json"`
	}
	if json.Unmarshal(r.Data, &envelope) != nil || envelope.JSON == nil {
		return r.Data
	}
	return envelope.JSON
}
