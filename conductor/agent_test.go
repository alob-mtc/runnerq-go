package conductor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go"
	"github.com/alob-mtc/runnerq-go/storage"
	"github.com/alob-mtc/runnerq-go/storage/postgres"
)

const testKey = "rqk_test"

// fakeGateway plays the Cloud side of the protocol.
type fakeGateway struct {
	t       *testing.T
	srv     *httptest.Server
	config  sessionConfig
	frame   int
	rejectN atomic.Int32 // answer this many dials with 401 first
	hellos  chan hello
	events  chan envelope

	mu      sync.Mutex
	conn    *websocket.Conn
	nextID  int
	replies map[string]chan envelope
}

func newFakeGateway(t *testing.T, cfg sessionConfig) *fakeGateway {
	g := &fakeGateway{
		t: t, config: cfg,
		hellos:  make(chan hello, 16),
		events:  make(chan envelope, 64),
		replies: map[string]chan envelope{},
	}
	g.srv = httptest.NewServer(http.HandlerFunc(g.handle))
	t.Cleanup(func() {
		g.mu.Lock()
		if g.conn != nil {
			g.conn.CloseNow()
		}
		g.mu.Unlock()
		g.srv.Close()
	})
	return g
}

func (g *fakeGateway) url() string { return g.srv.URL } // http:// on purpose: the agent maps it

func (g *fakeGateway) handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != agentPath || r.Header.Get("Authorization") != "Bearer "+testKey || g.rejectN.Add(-1) >= 0 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	conn.SetReadLimit(16 << 20)
	ctx := context.Background()
	var req envelope
	if err := wsjson.Read(ctx, conn, &req); err != nil {
		return
	}
	var h hello
	_ = json.Unmarshal(req.Data, &h)
	res, _ := json.Marshal(welcome{
		Version: 1, SessionID: uuid.NewString(), App: appRef{ID: "app-1", Name: "test-app"},
		Config: g.config, Limits: limits{MaxFrameBytes: g.frame},
	})
	if err := wsjson.Write(ctx, conn, envelope{V: 1, Kind: kindResponse, ID: req.ID, Type: typeHello, Data: res}); err != nil {
		return
	}
	g.mu.Lock()
	g.conn = conn
	g.mu.Unlock()
	g.hellos <- h

	for {
		var env envelope
		if err := wsjson.Read(ctx, conn, &env); err != nil {
			return
		}
		switch env.Kind {
		case kindResponse:
			g.mu.Lock()
			ch := g.replies[env.ID]
			g.mu.Unlock()
			if ch != nil {
				ch <- env
			}
		case kindEvent:
			select {
			case g.events <- env:
			default:
			}
		}
	}
}

// send writes a one-way event to the agent.
func (g *fakeGateway) send(msgType string, data any) {
	g.t.Helper()
	raw, _ := json.Marshal(data)
	g.mu.Lock()
	conn := g.conn
	g.mu.Unlock()
	if err := wsjson.Write(context.Background(), conn, envelope{V: 1, Kind: kindEvent, Type: msgType, Data: raw}); err != nil {
		g.t.Fatalf("send %s: %v", msgType, err)
	}
}

// callMeta sends a request with meta and waits for the reply.
func (g *fakeGateway) callMeta(msgType string, data any, meta map[string]json.RawMessage) envelope {
	g.t.Helper()
	raw, _ := json.Marshal(data)
	g.mu.Lock()
	g.nextID++
	id := strconv.Itoa(g.nextID)
	ch := make(chan envelope, 1)
	g.replies[id] = ch
	conn := g.conn
	g.mu.Unlock()
	if conn == nil {
		g.t.Fatal("no agent connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := wsjson.Write(ctx, conn, envelope{V: 1, Kind: kindRequest, ID: id, Type: msgType, Data: raw, Meta: meta}); err != nil {
		g.t.Fatalf("send %s: %v", msgType, err)
	}
	select {
	case res := <-ch:
		return res
	case <-ctx.Done():
		g.t.Fatalf("no reply to %s", msgType)
		return envelope{}
	}
}

func (g *fakeGateway) call(msgType string, data any) envelope { return g.callMeta(msgType, data, nil) }

// ok asserts a successful reply and decodes it into out.
func (g *fakeGateway) ok(msgType string, data, out any) {
	g.t.Helper()
	res := g.call(msgType, data)
	if res.Error != nil {
		g.t.Fatalf("%s: unexpected error %v", msgType, res.Error)
	}
	if out != nil {
		if err := json.Unmarshal(res.Data, out); err != nil {
			g.t.Fatalf("%s: decode %s: %v", msgType, res.Data, err)
		}
	}
}

// fails asserts a failed reply with the given code and returns the error.
func (g *fakeGateway) fails(msgType string, data any, code errorCode) *wireError {
	g.t.Helper()
	res := g.call(msgType, data)
	if res.Error == nil || res.Error.Code != code {
		g.t.Fatalf("%s: got error %v (data %s), want code %s", msgType, res.Error, res.Data, code)
	}
	return res.Error
}

func (g *fakeGateway) waitHello() hello {
	g.t.Helper()
	select {
	case h := <-g.hellos:
		return h
	case <-time.After(10 * time.Second):
		g.t.Fatal("agent never connected")
		return hello{}
	}
}

// waitEvent returns the next event of msgType.
func (g *fakeGateway) waitEvent(msgType string) envelope {
	g.t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case e := <-g.events:
			if e.Type == msgType {
				return e
			}
		case <-deadline:
			g.t.Fatalf("no %s event", msgType)
			return envelope{}
		}
	}
}

// nopStorage satisfies storage.Storage for tests that never touch storage.
type nopStorage struct{ storage.Storage }

type Echo struct {
	runnerq.DefaultDeadLetterHandler
}

func (Echo) Handle(_ runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	return payload, nil
}

func nopEngine(t *testing.T) *runnerq.WorkerEngine {
	t.Helper()
	e := runnerq.NewWorkerEngineWithBackend(nopStorage{}, runnerq.WorkerConfig{QueueName: "q1", MaxConcurrentActivities: 3})
	e.RegisterActivity(&Echo{})
	return e
}

func pgEngine(t *testing.T) (*runnerq.WorkerEngine, *postgres.PostgresBackend, string) {
	t.Helper()
	dsn := os.Getenv("RUNNERQ_TEST_DSN")
	if dsn == "" {
		t.Skip("RUNNERQ_TEST_DSN not set")
	}
	queue := "cond_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
	b, err := postgres.New(context.Background(), dsn, queue)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(b.Close)
	e := runnerq.NewWorkerEngineWithBackend(b, runnerq.WorkerConfig{QueueName: queue, MaxConcurrentActivities: 2})
	e.RegisterActivity(&Echo{})
	return e, b, queue
}

func startAgent(t *testing.T, e *runnerq.WorkerEngine, g *fakeGateway, cfg Config) *Agent {
	t.Helper()
	cfg.URL, cfg.APIKey = g.url(), testKey
	if cfg.MinReconnectDelay == 0 {
		cfg.MinReconnectDelay, cfg.MaxReconnectDelay = 10*time.Millisecond, 50*time.Millisecond
	}
	a, err := Start(context.Background(), e, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = a.Close(ctx)
	})
	return a
}

// enqueue adds a root activity directly through the backend.
func enqueue(t *testing.T, b *postgres.PostgresBackend, payload string, opts ...func(*storage.QueuedActivity)) uuid.UUID {
	t.Helper()
	id := uuid.New()
	a := storage.QueuedActivity{
		ID: id, ActivityType: "Echo", Payload: json.RawMessage(payload),
		Priority: storage.PriorityNormal, MaxRetries: 3, TimeoutSeconds: 60,
		RetryDelaySeconds: 1, CreatedAt: time.Now().UTC(), RootActivityID: id,
	}
	for _, o := range opts {
		o(&a)
	}
	if err := b.Enqueue(context.Background(), a); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return id
}

// inQueue scopes a query to the test's queue (queries span all queues).
func inQueue(queue string, terms ...map[string]any) map[string]any {
	and := []any{map[string]any{"field": "queue", "op": "eq", "value": queue}}
	for _, t := range terms {
		and = append(and, t)
	}
	return map[string]any{"and": and}
}

func TestConfigValidation(t *testing.T) {
	e := nopEngine(t)
	ctx := context.Background()
	cases := map[string]Config{
		"no key":     {URL: "wss://x"},
		"no url":     {APIKey: "k"},
		"bad scheme": {URL: "ftp://x", APIKey: "k"},
	}
	for name, cfg := range cases {
		if _, err := Start(ctx, e, cfg); err == nil {
			t.Fatalf("%s: Start accepted invalid config", name)
		}
	}
	if _, err := Start(ctx, nil, Config{URL: "wss://x", APIKey: "k"}); err == nil {
		t.Fatal("Start accepted a nil engine")
	}
	c := Config{URL: "https://cloud.example.com/base/", APIKey: "k"}
	u, err := c.validate()
	if err != nil || u.String() != "wss://cloud.example.com/base/v1/agent" {
		t.Fatalf("url mapping: %v %v", u, err)
	}
}

func TestHelloDescribesExecutor(t *testing.T) {
	g := newFakeGateway(t, sessionConfig{})
	e := runnerq.NewWorkerEngineWithBackend(nopStorage{}, runnerq.WorkerConfig{
		QueueName: "q1", MaxConcurrentActivities: 3, Labels: map[string]string{"region": "us", "deploy": "v7"},
	})
	e.RegisterActivity(&Echo{})
	// The agent's labels are added to the engine's, and win.
	a := startAgent(t, e, g, Config{Labels: map[string]string{"region": "eu"}})

	h := g.waitHello()
	ex := h.Executor
	if ex.ID != e.InstanceID() || ex.MaxConcurrency != 3 || len(ex.Queues) != 1 || ex.Queues[0] != "q1" ||
		ex.Labels["region"] != "eu" || ex.Labels["deploy"] != "v7" || len(ex.ActivityTypes) != 1 || ex.ActivityTypes[0] != "Echo" ||
		ex.Hostname == "" {
		t.Fatalf("executor %+v", ex)
	}
	if h.SDK.Name != "runnerq-go" || h.SDK.Language != "go" || h.Limits.MaxFrameBytes != maxMessageBytes {
		t.Fatalf("hello %+v", h)
	}
	// A backend without QueryStorage serves only executor-scoped messages.
	if _, ok := h.Capabilities[typeExecutorDescribe]; !ok || len(h.Capabilities) != 1 {
		t.Fatalf("capabilities %+v", h.Capabilities)
	}
	for deadline := time.Now().Add(5 * time.Second); !a.Connected() || a.SessionID() == ""; {
		if time.Now().After(deadline) {
			t.Fatal("agent never reported connected")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestExecutorDescribeAndReports(t *testing.T) {
	g := newFakeGateway(t, sessionConfig{ReportIntervalMS: 60_000}) // the first report is sent on connect
	e := nopEngine(t)
	startAgent(t, e, g, Config{})
	g.waitHello()

	var st executorState
	g.ok(typeExecutorDescribe, struct{}{}, &st)
	if st.ID != e.InstanceID() || st.MaxConcurrency != 3 || st.InFlight != 0 || st.Draining ||
		st.Counters == nil || *st.Counters != (executorCounters{}) {
		t.Fatalf("state %+v", st)
	}
	evt := g.waitEvent(typeExecutorReport)
	var report map[string]json.RawMessage
	if err := json.Unmarshal(evt.Data, &report); err != nil || string(report["id"]) != `"`+e.InstanceID()+`"` {
		t.Fatalf("report %s: %v", evt.Data, err)
	}
	if string(report["claim_lag_ms"]) != "0" || string(report["heartbeat_failures"]) != "0" {
		t.Fatalf("report %s", evt.Data)
	}
	var counters map[string]any
	if err := json.Unmarshal(report["counters"], &counters); err != nil || counters["claimed"] != 0.0 || counters["dead_lettered"] != 0.0 {
		t.Fatalf("report counters %s: %v", report["counters"], err)
	}
}

func TestReportsChanges(t *testing.T) {
	g := newFakeGateway(t, sessionConfig{ReportIntervalMS: 60_000})
	e := nopEngine(t)
	startAgent(t, e, g, Config{})
	g.waitHello()
	g.waitEvent(typeExecutorReport) // on connect

	// A drain beginning is reported without waiting out the interval.
	e.Stop()
	evt := g.waitEvent(typeExecutorReport)
	var st executorState
	if err := json.Unmarshal(evt.Data, &st); err != nil || !st.Draining {
		t.Fatalf("report after the stop: %s", evt.Data)
	}
}

func TestProtocolErrors(t *testing.T) {
	g := newFakeGateway(t, sessionConfig{})
	startAgent(t, nopEngine(t), g, Config{})
	g.waitHello()

	g.fails("launch_missiles", nil, codeUnsupported)
	g.fails(typeActivitiesList, nil, codeUnsupported) // backend lacks QueryStorage
	res := g.callMeta(typeExecutorDescribe, struct{}{}, map[string]json.RawMessage{
		metaDeadline: json.RawMessage(`"` + time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano) + `"`),
	})
	if res.Error == nil || res.Error.Code != codeDeadlineExceeded {
		t.Fatalf("expired deadline: %+v", res)
	}
}

func TestReconnectsAfterRejectionAndDrop(t *testing.T) {
	g := newFakeGateway(t, sessionConfig{})
	g.rejectN.Store(2) // two 401s before the key is accepted
	startAgent(t, nopEngine(t), g, Config{})
	first := g.waitHello()

	g.mu.Lock()
	g.conn.CloseNow() // gateway crash
	g.mu.Unlock()

	if second := g.waitHello(); first.Executor.ID != second.Executor.ID {
		t.Fatalf("executor id changed across reconnects: %s -> %s", first.Executor.ID, second.Executor.ID)
	}
}

func TestCloseSendsGoodbye(t *testing.T) {
	g := newFakeGateway(t, sessionConfig{})
	a := startAgent(t, nopEngine(t), g, Config{})
	g.waitHello()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Close(ctx); err != nil {
		t.Fatal(err)
	}
	var gb goodbye
	if err := json.Unmarshal(g.waitEvent(typeGoodbye).Data, &gb); err != nil || gb.Reason != "shutdown" {
		t.Fatalf("goodbye %+v %v", gb, err)
	}
	if a.Connected() {
		t.Fatal("agent still connected after Close")
	}
	select {
	case <-g.hellos:
		t.Fatal("agent reconnected after Close")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestCancelledContextSaysGoodbye(t *testing.T) {
	g := newFakeGateway(t, sessionConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	a, err := Start(ctx, nopEngine(t), Config{URL: g.url(), APIKey: testKey})
	if err != nil {
		t.Fatal(err)
	}
	g.waitHello()
	cancel() // e.g. a signal.NotifyContext firing on SIGTERM
	if e := g.waitEvent(typeGoodbye); e.Type != typeGoodbye {
		t.Fatalf("got %s", e.Type)
	}
	cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ccancel()
	if err := a.Close(cctx); err != nil {
		t.Fatal(err)
	}
}

func TestRequestLimit(t *testing.T) {
	g := newFakeGateway(t, sessionConfig{})
	a := startAgent(t, nopEngine(t), g, Config{MaxConcurrentRequests: 1})
	g.waitHello()

	release := make(chan struct{})
	started := make(chan struct{})
	a.table["block"] = func(context.Context, json.RawMessage) (any, error) {
		close(started)
		<-release
		return struct{}{}, nil
	}
	done := make(chan envelope, 1)
	go func() { done <- g.call("block", nil) }()
	<-started
	g.fails(typeExecutorDescribe, nil, codeResourceExhausted)
	close(release)
	if res := <-done; res.Error != nil {
		t.Fatalf("blocked request failed: %v", res.Error)
	}
}

func TestHandlerPanicAndOversizedReplies(t *testing.T) {
	g := newFakeGateway(t, sessionConfig{})
	g.frame = 64 << 10
	a := startAgent(t, nopEngine(t), g, Config{})
	g.waitHello()
	a.table["boom"] = func(context.Context, json.RawMessage) (any, error) { panic("boom") }
	a.table["huge"] = func(context.Context, json.RawMessage) (any, error) { return strings.Repeat("x", 128<<10), nil }
	g.fails("boom", nil, codeInternal)
	g.fails("huge", nil, codeResourceExhausted)
	g.ok(typeExecutorDescribe, struct{}{}, nil) // still serving
}

// --- against Postgres ---

func TestQueriesFromStorage(t *testing.T) {
	e, b, queue := pgEngine(t)
	g := newFakeGateway(t, sessionConfig{})
	startAgent(t, e, g, Config{})
	h := g.waitHello()
	for _, want := range []string{typeActivitiesList, typeActivitiesGet, typeActivitiesCount, typeActivitiesAggregate,
		typeStepsList, typeEventsList, typeResultsGet, typeTreesGet} {
		if _, ok := h.Capabilities[want]; !ok {
			t.Fatalf("capability %q missing", want)
		}
	}
	if c := h.Capabilities[typeActivitiesList]; len(c.Filters) == 0 || len(c.Sorts) == 0 {
		t.Fatalf("list capability without sub-features: %+v", c)
	}

	id := enqueue(t, b, `{"n":1}`)
	child := enqueue(t, b, `{"n":2}`, func(a *storage.QueuedActivity) { a.ParentActivityID, a.RootActivityID, a.Depth = &id, id, 1 })
	for range 3 {
		enqueue(t, b, `{}`, func(a *storage.QueuedActivity) { a.ActivityType = "Other" })
	}

	var list page[activityView]
	g.ok(typeActivitiesList, query{Filter: mustFilter(inQueue(queue, map[string]any{"field": "type", "op": "eq", "value": "Echo"})), Limit: 1}, &list)
	if len(list.Items) != 1 || list.NextCursor == "" || list.Items[0].Payload != nil {
		t.Fatalf("first page %+v", list)
	}
	var second page[activityView]
	g.ok(typeActivitiesList, query{Filter: mustFilter(inQueue(queue, map[string]any{"field": "type", "op": "eq", "value": "Echo"})), Limit: 1, Cursor: list.NextCursor}, &second)
	if len(second.Items) != 1 || second.NextCursor != "" || second.Items[0].ID == list.Items[0].ID {
		t.Fatalf("second page %+v", second)
	}

	var act activityView
	g.ok(typeActivitiesGet, getRequest{ID: id.String(), Include: []string{"payload", "events", "steps"}}, &act)
	if act.ID != id.String() || act.Status != "pending" || act.Type != "Echo" || string(act.Payload) != `{"n": 1}` && string(act.Payload) != `{"n":1}` {
		t.Fatalf("activity %+v (payload %s)", act, act.Payload)
	}
	if act.Events == nil || len(*act.Events) == 0 || (*act.Events)[0].Type != storage.RecordEventCreated || act.Steps == nil || len(*act.Steps) != 0 {
		t.Fatalf("embedded events/steps %+v %+v", act.Events, act.Steps)
	}
	g.fails(typeActivitiesGet, getRequest{ID: uuid.NewString()}, codeNotFound)
	g.fails(typeActivitiesGet, getRequest{ID: "not-an-id"}, codeNotFound)

	var count countResult
	g.ok(typeActivitiesCount, countRequest{Filter: mustFilter(inQueue(queue))}, &count)
	if count.Count != 5 || !count.Exact {
		t.Fatalf("count %+v", count)
	}

	var agg aggregateResult
	g.ok(typeActivitiesAggregate, aggregateRequest{Filter: mustFilter(inQueue(queue)), GroupBy: []string{"type"},
		Metrics: []metric{{Name: "count"}}}, &agg)
	counts := map[string]int64{}
	for _, gr := range agg.Groups {
		counts[gr.Key["type"]] = *gr.Count
	}
	if counts["Echo"] != 2 || counts["Other"] != 3 {
		t.Fatalf("aggregate %+v", agg)
	}

	var tree treeView
	g.ok(typeTreesGet, treeRequest{ID: child.String()}, &tree)
	if tree.RootID != id.String() || len(tree.Items) != 2 {
		t.Fatalf("tree %+v", tree)
	}
	var events page[eventView]
	g.ok(typeEventsList, query{Filter: mustFilter(map[string]any{"field": "root_id", "op": "eq", "value": id.String()}),
		Sort: []wireSort{{Field: "at", Order: "desc"}}}, &events)
	if len(events.Items) != 2 || events.Items[0].ActivityID != child.String() {
		t.Fatalf("events %+v", events)
	}
	g.fails(typeResultsGet, resultRequest{ActivityID: id.String()}, codeNotFound)

	// Errors carry the offending field.
	e1 := g.fails(typeActivitiesList, query{Filter: mustFilter(map[string]any{"field": "colour", "op": "eq", "value": "red"})}, codeUnsupported)
	if e1.Details["field"] != "colour" {
		t.Fatalf("details %+v", e1.Details)
	}
	g.fails(typeActivitiesList, query{Filter: mustFilter(map[string]any{"field": "status", "op": "eq", "value": "exploded"})}, codeInvalidArgument)
	g.fails(typeActivitiesList, query{Include: []string{"secrets"}}, codeUnsupported)
	g.fails(typeActivitiesList, query{Sort: []wireSort{{Field: "created_at"}, {Field: "priority"}}}, codeUnsupported)
	g.fails(typeActivitiesList, json.RawMessage(`{"filter":null,"surprise":1}`), codeInvalidArgument)
	g.fails(typeActivitiesAggregate, aggregateRequest{Metrics: []metric{{Name: "vibes"}}}, codeUnsupported)
}

func TestMetadataOnly(t *testing.T) {
	for name, tc := range map[string]struct {
		cloud sessionConfig
		local bool
	}{
		"cloud asks":   {cloud: sessionConfig{DataMode: dataModeMetadataOnly}},
		"agent forces": {cloud: sessionConfig{DataMode: "full"}, local: true},
	} {
		t.Run(name, func(t *testing.T) {
			e, b, _ := pgEngine(t)
			g := newFakeGateway(t, tc.cloud)
			startAgent(t, e, g, Config{MetadataOnly: tc.local})
			g.waitHello()

			id := enqueue(t, b, `{"secret":"pii"}`)
			g.fails(typeActivitiesGet, getRequest{ID: id.String(), Include: []string{"payload"}}, codeForbidden)
			g.fails(typeActivitiesList, query{Include: []string{"last_error"}}, codeForbidden)
			g.fails(typeEventsList, query{Include: []string{"detail"}}, codeForbidden)
			g.fails(typeResultsGet, resultRequest{ActivityID: id.String()}, codeForbidden)
			var act activityView
			g.ok(typeActivitiesGet, getRequest{ID: id.String(), Include: []string{"events"}}, &act)
			if raw, _ := json.Marshal(act); strings.Contains(string(raw), "pii") {
				t.Fatalf("customer data leaked: %s", raw)
			}
		})
	}
}

func TestConfigUpdateChangesDataMode(t *testing.T) {
	e, b, _ := pgEngine(t)
	g := newFakeGateway(t, sessionConfig{DataMode: "full"})
	startAgent(t, e, g, Config{})
	g.waitHello()
	id := enqueue(t, b, `{}`)

	g.ok(typeActivitiesGet, getRequest{ID: id.String(), Include: []string{"payload"}}, nil)
	g.send(typeConfigUpdate, sessionConfig{DataMode: dataModeMetadataOnly})
	for deadline := time.Now().Add(5 * time.Second); ; {
		res := g.call(typeActivitiesGet, getRequest{ID: id.String(), Include: []string{"payload"}})
		if res.Error != nil && res.Error.Code == codeForbidden {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("config.update never applied")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func mustFilter(m map[string]any) *wireFilter {
	raw, _ := json.Marshal(m)
	var f wireFilter
	if err := json.Unmarshal(raw, &f); err != nil {
		panic(err)
	}
	return &f
}
