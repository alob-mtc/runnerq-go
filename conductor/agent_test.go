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
	t        *testing.T
	srv      *httptest.Server
	mode     dataMode
	rejectN  atomic.Int32 // answer this many dials with 401 first
	hellos   chan hello
	goodbyes chan goodbye

	mu      sync.Mutex
	conn    *websocket.Conn
	nextID  int
	replies map[string]chan envelope
}

func newFakeGateway(t *testing.T, mode dataMode) *fakeGateway {
	g := &fakeGateway{
		t: t, mode: mode,
		hellos:   make(chan hello, 16),
		goodbyes: make(chan goodbye, 16),
		replies:  map[string]chan envelope{},
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
	ctx := context.Background()
	var req envelope
	if err := wsjson.Read(ctx, conn, &req); err != nil {
		return
	}
	var h hello
	_ = json.Unmarshal(req.Data, &h)
	res, _ := json.Marshal(welcome{Version: 1, SessionID: uuid.NewString(), App: "test-app", DataMode: g.mode})
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
			if env.Type == typeGoodbye {
				var gb goodbye
				_ = json.Unmarshal(env.Data, &gb)
				g.goodbyes <- gb
			}
		}
	}
}

// call sends a request to the connected agent and waits for its reply.
func (g *fakeGateway) call(msgType string, data any) envelope {
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
	if err := wsjson.Write(ctx, conn, envelope{V: 1, Kind: kindRequest, ID: id, Type: msgType, Data: raw}); err != nil {
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

// fails asserts a failed reply with the given code.
func (g *fakeGateway) fails(msgType string, data any, code errorCode) {
	g.t.Helper()
	res := g.call(msgType, data)
	if res.Error == nil || res.Error.Code != code {
		g.t.Fatalf("%s: got error %v (data %s), want code %s", msgType, res.Error, res.Data, code)
	}
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
	e := runnerq.NewWorkerEngineWithBackend(nopStorage{}, runnerq.WorkerConfig{MaxConcurrentActivities: 3})
	e.RegisterActivity(&Echo{})
	return e
}

func pgEngine(t *testing.T) (*runnerq.WorkerEngine, *postgres.PostgresBackend) {
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
	return e, b
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
func enqueue(t *testing.T, b *postgres.PostgresBackend, payload string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	err := b.Enqueue(context.Background(), storage.QueuedActivity{
		ID: id, ActivityType: "Echo", Payload: json.RawMessage(payload),
		Priority: storage.PriorityNormal, MaxRetries: 3, TimeoutSeconds: 60,
		RetryDelaySeconds: 1, CreatedAt: time.Now().UTC(), RootActivityID: id,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return id
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
	g := newFakeGateway(t, dataModeFull)
	e := nopEngine(t)
	a := startAgent(t, e, g, Config{AllowControl: true})

	h := g.waitHello()
	if h.ExecutorID != e.InstanceID() || h.MaxWorkers != 3 || h.SDK.Name != "runnerq-go" || h.SDK.Language != "go" {
		t.Fatalf("unexpected hello %+v", h)
	}
	if len(h.ActivityTypes) != 1 || h.ActivityTypes[0] != "Echo" {
		t.Fatalf("activity types %v", h.ActivityTypes)
	}
	if len(h.ProtocolVersions) != 1 || h.ProtocolVersions[0] != protocolVersion {
		t.Fatalf("versions %v", h.ProtocolVersions)
	}
	for _, want := range []string{typeStats, typeGetActivity, typeSignal} {
		found := false
		for _, c := range h.Capabilities {
			found = found || c == want
		}
		if !found {
			t.Fatalf("capability %q missing from %v", want, h.Capabilities)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for !a.Connected() || a.SessionID() == "" {
		if time.Now().After(deadline) {
			t.Fatal("agent never reported connected")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestUnknownTypeAndBadInput(t *testing.T) {
	g := newFakeGateway(t, dataModeFull)
	startAgent(t, nopEngine(t), g, Config{})
	g.waitHello()

	g.fails("launch_missiles", nil, codeUnsupported)
	g.fails(typeGetActivity, activityRef{ID: "not-a-uuid"}, codeInvalidArgument)
	g.fails(typeListActivities, listRequest{Status: "bogus"}, codeInvalidArgument)
	g.fails(typeListRoots, listRequest{Status: "cron"}, codeInvalidArgument)
	g.fails(typeGetActivity, json.RawMessage(`{"id":`), codeInvalidArgument)
}

func TestSignalNeedsAllowControl(t *testing.T) {
	g := newFakeGateway(t, dataModeFull)
	startAgent(t, nopEngine(t), g, Config{AllowControl: false})
	g.waitHello()
	g.fails(typeSignal, signalRequest{ID: uuid.NewString(), Name: "go"}, codeForbidden)
}

func TestSignalValidation(t *testing.T) {
	g := newFakeGateway(t, dataModeFull)
	startAgent(t, nopEngine(t), g, Config{AllowControl: true})
	g.waitHello()
	g.fails(typeSignal, signalRequest{ID: uuid.NewString()}, codeInvalidArgument)
	g.fails(typeSignal, signalRequest{Name: "go"}, codeInvalidArgument)
	g.fails(typeSignal, signalRequest{ID: uuid.NewString(), Key: "k", Name: "go"}, codeInvalidArgument)
	g.fails(typeSignal, signalRequest{Key: "k", Name: "go"}, codeInvalidArgument) // no activity_type
}

func TestReconnectsAfterRejectionAndDrop(t *testing.T) {
	g := newFakeGateway(t, dataModeFull)
	g.rejectN.Store(2) // two 401s before the key is accepted
	startAgent(t, nopEngine(t), g, Config{})
	first := g.waitHello()

	g.mu.Lock()
	g.conn.CloseNow() // gateway crash
	g.mu.Unlock()

	second := g.waitHello()
	if first.ExecutorID != second.ExecutorID {
		t.Fatalf("executor id changed across reconnects: %s -> %s", first.ExecutorID, second.ExecutorID)
	}
}

func TestCloseSendsGoodbye(t *testing.T) {
	g := newFakeGateway(t, dataModeFull)
	a := startAgent(t, nopEngine(t), g, Config{})
	g.waitHello()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case gb := <-g.goodbyes:
		if gb.Reason != "shutdown" {
			t.Fatalf("goodbye reason %q", gb.Reason)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no goodbye received")
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

func TestRequestLimitAnswersUnavailable(t *testing.T) {
	g := newFakeGateway(t, dataModeFull)
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
	g.fails(typeStats, nil, codeUnavailable)
	close(release)
	if res := <-done; res.Error != nil {
		t.Fatalf("blocked request failed: %v", res.Error)
	}
}

func TestHandlerPanicIsContained(t *testing.T) {
	g := newFakeGateway(t, dataModeFull)
	a := startAgent(t, nopEngine(t), g, Config{})
	g.waitHello()
	a.table["boom"] = func(context.Context, json.RawMessage) (any, error) { panic("boom") }
	g.fails("boom", nil, codeInternal)
	g.fails("launch_missiles", nil, codeUnsupported) // still serving
}

// --- against Postgres ---

func TestReadsFromStorage(t *testing.T) {
	e, b := pgEngine(t)
	g := newFakeGateway(t, dataModeFull)
	startAgent(t, e, g, Config{})
	g.waitHello()

	id := enqueue(t, b, `{"n":1}`)
	enqueue(t, b, `{"n":2}`)

	var stats struct {
		Pending    uint64 `json:"pending_activities"`
		MaxWorkers *int   `json:"max_workers"`
	}
	g.ok(typeStats, struct{}{}, &stats)
	if stats.Pending != 2 {
		t.Fatalf("stats pending %d, want 2", stats.Pending)
	}

	var list []map[string]any
	g.ok(typeListActivities, listRequest{Status: "pending", page: page{Limit: 1}}, &list)
	if len(list) != 1 {
		t.Fatalf("limit ignored: %d items", len(list))
	}
	g.ok(typeListRoots, listRequest{}, &list)
	if len(list) != 2 {
		t.Fatalf("roots: %d items", len(list))
	}

	var act map[string]any
	g.ok(typeGetActivity, activityRef{ID: id.String()}, &act)
	if act["id"] != id.String() || act["activity_type"] != "Echo" {
		t.Fatalf("get_activity %+v", act)
	}
	if p, _ := json.Marshal(act["payload"]); string(p) != `{"n":1}` {
		t.Fatalf("payload %s", p)
	}
	g.fails(typeGetActivity, activityRef{ID: uuid.NewString()}, codeNotFound)

	var events []map[string]any
	g.ok(typeGetActivityEvents, activityPage{ID: id.String()}, &events)
	if len(events) == 0 || events[0]["event_type"] != storage.EventEnqueued {
		t.Fatalf("events %+v", events)
	}

	var tree []map[string]any
	g.ok(typeGetSubtree, activityRef{ID: id.String()}, &tree)
	if len(tree) != 1 {
		t.Fatalf("subtree %+v", tree)
	}
	g.ok(typeGetChildren, activityPage{ID: id.String()}, &tree)
	if len(tree) != 0 {
		t.Fatalf("children %+v", tree)
	}
	var steps []any
	g.ok(typeGetActivitySteps, activityRef{ID: id.String()}, &steps)
	var dl []any
	g.ok(typeListDeadLetter, page{}, &dl)
	g.fails(typeGetActivityResult, activityRef{ID: id.String()}, codeNotFound)
}

func TestSignalDelivered(t *testing.T) {
	e, b := pgEngine(t)
	g := newFakeGateway(t, dataModeFull)
	startAgent(t, e, g, Config{AllowControl: true})
	g.waitHello()

	id := enqueue(t, b, `{}`)
	g.ok(typeSignal, signalRequest{ID: id.String(), Name: "approve", Payload: json.RawMessage(`{"ok":true}`)}, nil)
	g.fails(typeSignal, signalRequest{ID: uuid.NewString(), Name: "approve"}, codeNotFound)
	g.fails(typeSignal, signalRequest{Key: "nobody", ActivityType: "Echo", Name: "approve"}, codeNotFound)

	events, err := b.GetActivityEvents(context.Background(), id, 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, ev := range events {
		found = found || ev.EventType == storage.EventSignaled
	}
	if !found {
		t.Fatalf("no Signaled event after signal: %+v", events)
	}
}

func TestMetadataOnlyRedacts(t *testing.T) {
	for name, tc := range map[string]struct {
		gateway dataMode
		local   bool
	}{
		"cloud asks":   {gateway: dataModeMetadataOnly},
		"agent forces": {gateway: dataModeFull, local: true},
	} {
		t.Run(name, func(t *testing.T) {
			e, b := pgEngine(t)
			g := newFakeGateway(t, tc.gateway)
			startAgent(t, e, g, Config{MetadataOnly: tc.local})
			g.waitHello()

			id := enqueue(t, b, `{"secret":"pii"}`)
			var act map[string]any
			g.ok(typeGetActivity, activityRef{ID: id.String()}, &act)
			if p, ok := act["payload"]; ok && p != nil {
				t.Fatalf("payload leaked: %v", p)
			}
			var list []map[string]any
			g.ok(typeListActivities, listRequest{Status: "pending"}, &list)
			if raw, _ := json.Marshal(list); strings.Contains(string(raw), "pii") {
				t.Fatalf("payload leaked in list: %s", raw)
			}
			var events []map[string]any
			g.ok(typeGetActivityEvents, activityPage{ID: id.String()}, &events)
			for _, ev := range events {
				if d, ok := ev["detail"]; ok && d != nil {
					t.Fatalf("event detail leaked: %v", d)
				}
			}
			g.fails(typeGetActivityResult, activityRef{ID: id.String()}, codeForbidden)
		})
	}
}
