// Package conductor connects a worker process to RunnerQ Cloud.
//
// The agent dials out to the Cloud gateway over a WebSocket and answers its
// requests — queue stats, activity reads, signals — by running them against
// the engine's own storage backend. The Cloud never connects into your
// network and never holds database credentials.
//
// The agent is off the execution path: if the Cloud is unreachable, or the
// agent fails, activities keep running. It reconnects on its own with
// exponential backoff.
//
//	engine, _ := runnerq.Builder().Backend(backend).Build()
//	agent, err := conductor.Start(ctx, engine, conductor.Config{
//		URL:    "wss://cloud.runnerq.dev",
//		APIKey: os.Getenv("RUNNERQ_CONDUCTOR_KEY"),
//	})
//	if err != nil { ... }
//	defer agent.Close(context.Background()) // runs before the engine stops
//	engine.Start(ctx)
//
// The wire protocol is specified in the runnerq-cloud repository
// (docs/protocol.md).
package conductor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/alob-mtc/runnerq-go"
)

const (
	agentPath        = "/v1/agent"
	handshakeTimeout = 10 * time.Second
	pingInterval     = 20 * time.Second
	writeTimeout     = 10 * time.Second
	maxMessageBytes  = 1 << 20
	sdkModulePath    = "github.com/alob-mtc/runnerq-go"
)

// Config configures the agent.
type Config struct {
	// URL is the Cloud gateway, e.g. "wss://cloud.runnerq.dev". The agent
	// path (/v1/agent) is appended when missing; http(s) schemes are mapped
	// to ws(s).
	URL string
	// APIKey authenticates the app. It is sent in the Authorization header,
	// never in the URL.
	APIKey string
	// AllowControl lets the Cloud run state-changing actions such as
	// signals. When false the agent is read-only and answers every control
	// request with "forbidden".
	AllowControl bool
	// MetadataOnly strips payloads, results, errors and step data from every
	// response, whatever mode the Cloud asks for.
	MetadataOnly bool
	// MaxConcurrentRequests bounds requests served at once (default 16).
	// Requests beyond it are answered "unavailable" instead of queueing.
	MaxConcurrentRequests int
	// RequestTimeout bounds one request (default 30s).
	RequestTimeout time.Duration
	// MinReconnectDelay and MaxReconnectDelay bound the reconnect backoff
	// (defaults 1s and 30s). Each wait is jittered by ±50%.
	MinReconnectDelay time.Duration
	MaxReconnectDelay time.Duration
	Logger            *slog.Logger
}

func (c *Config) validate() (*url.URL, error) {
	if c.APIKey == "" {
		return nil, errors.New("conductor: Config.APIKey is required")
	}
	u, err := url.Parse(c.URL)
	if err != nil || c.URL == "" {
		return nil, fmt.Errorf("conductor: invalid Config.URL %q", c.URL)
	}
	switch u.Scheme {
	case "ws", "wss":
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return nil, fmt.Errorf("conductor: Config.URL scheme must be ws, wss, http or https, got %q", u.Scheme)
	}
	if !strings.HasSuffix(u.Path, agentPath) {
		u.Path = strings.TrimSuffix(u.Path, "/") + agentPath
	}
	if c.MaxConcurrentRequests <= 0 {
		c.MaxConcurrentRequests = 16
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 30 * time.Second
	}
	if c.MinReconnectDelay <= 0 {
		c.MinReconnectDelay = time.Second
	}
	if c.MaxReconnectDelay <= 0 {
		c.MaxReconnectDelay = 30 * time.Second
	}
	c.MaxReconnectDelay = max(c.MaxReconnectDelay, c.MinReconnectDelay)
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return u, nil
}

// Agent is a running connection to RunnerQ Cloud.
type Agent struct {
	cfg    Config
	url    string
	engine *runnerq.WorkerEngine
	h      *handlers
	table  map[string]handlerFunc
	sem    chan struct{}
	log    *slog.Logger
	cancel context.CancelFunc
	done   chan struct{}

	mu        sync.Mutex
	conn      *websocket.Conn
	sessionID string
	closing   bool

	connected atomic.Bool
}

// Start validates cfg and connects in the background. It returns without
// waiting for the Cloud: an unreachable Cloud never delays the worker. The
// agent stops when ctx ends or Close is called.
func Start(ctx context.Context, engine *runnerq.WorkerEngine, cfg Config) (*Agent, error) {
	if engine == nil {
		return nil, errors.New("conductor: engine is required")
	}
	u, err := cfg.validate()
	if err != nil {
		return nil, err
	}
	h := newHandlers(engine, cfg.AllowControl)
	ctx, cancel := context.WithCancel(ctx)
	a := &Agent{
		cfg:    cfg,
		url:    u.String(),
		engine: engine,
		h:      h,
		table:  h.table(),
		sem:    make(chan struct{}, cfg.MaxConcurrentRequests),
		log:    cfg.Logger.With("component", "runnerq-conductor"),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go a.run(ctx)
	return a, nil
}

// Connected reports whether the agent currently has a Cloud session.
func (a *Agent) Connected() bool { return a.connected.Load() }

// Close tells the Cloud this executor is shutting down (so it is recorded as
// a deploy, not a crash), closes the connection and waits for the agent to
// stop or ctx to end. Call it before stopping the engine.
func (a *Agent) Close(ctx context.Context) error {
	a.mu.Lock()
	a.closing = true
	conn := a.conn
	a.mu.Unlock()

	if conn != nil {
		sayGoodbye(ctx, conn)
	}
	a.cancel()
	select {
	case <-a.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sayGoodbye sends the graceful-shutdown event and closes conn normally.
func sayGoodbye(ctx context.Context, conn *websocket.Conn) {
	if evt, err := newEvent(typeGoodbye, goodbye{Reason: "shutdown"}); err == nil {
		wctx, cancel := context.WithTimeout(ctx, writeTimeout)
		_ = wsjson.Write(wctx, conn, evt)
		cancel()
	}
	_ = conn.Close(websocket.StatusNormalClosure, "shutdown")
}

// run keeps a session open until the agent stops.
func (a *Agent) run(ctx context.Context) {
	defer close(a.done)
	delay := a.cfg.MinReconnectDelay
	for {
		start := time.Now()
		err := a.session(ctx)
		if ctx.Err() != nil || a.isClosing() {
			return
		}

		wait := delay
		switch {
		case websocket.CloseStatus(err) == websocket.StatusGoingAway:
			// The gateway node is draining; reconnect to another one promptly.
			wait = a.cfg.MinReconnectDelay
			a.log.Info("Cloud gateway is restarting; reconnecting")
		case time.Since(start) > a.cfg.MaxReconnectDelay:
			// The session was healthy for a while; start the backoff over.
			delay, wait = a.cfg.MinReconnectDelay, a.cfg.MinReconnectDelay
			a.log.Warn("Lost connection to RunnerQ Cloud; reconnecting", "error", err)
		default:
			a.log.Warn("Could not connect to RunnerQ Cloud; retrying", "error", err, "retry_in", wait)
		}
		delay = min(delay*2, a.cfg.MaxReconnectDelay)

		t := time.NewTimer(jitter(wait))
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

func (a *Agent) isClosing() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.closing
}

// jitter spreads d uniformly over [d/2, 3d/2).
func jitter(d time.Duration) time.Duration {
	return d/2 + rand.N(d)
}

// session dials, completes the handshake and serves requests until the
// connection ends.
func (a *Agent) session(ctx context.Context) error {
	// The handshake is not cut short by Close: a Close that lands mid-way
	// waits (bounded by handshakeTimeout) and then says goodbye below, so the
	// Cloud never records a clean stop as a crash.
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), handshakeTimeout)
	defer cancel()
	conn, res, err := websocket.Dial(dctx, a.url, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": {"Bearer " + a.cfg.APIKey}},
	})
	if err != nil {
		if res != nil && res.StatusCode == http.StatusUnauthorized {
			return errors.New("the Cloud rejected the API key (401)")
		}
		return err
	}
	defer conn.CloseNow()
	conn.SetReadLimit(maxMessageBytes)

	w, err := a.handshake(dctx, conn)
	if err != nil {
		return err
	}
	a.h.setMetadataOnly(a.cfg.MetadataOnly || w.DataMode == dataModeMetadataOnly)

	a.mu.Lock()
	if a.closing {
		// Close ran during the handshake and found no connection to say
		// goodbye on; do it here so the Cloud still records a clean stop.
		a.mu.Unlock()
		sayGoodbye(context.WithoutCancel(ctx), conn)
		return nil
	}
	a.conn, a.sessionID = conn, w.SessionID
	a.mu.Unlock()
	a.connected.Store(true)
	a.log.Info("Connected to RunnerQ Cloud", "app", w.App, "session", w.SessionID, "data_mode", w.DataMode)
	defer func() {
		a.connected.Store(false)
		a.mu.Lock()
		a.conn = nil
		a.mu.Unlock()
	}()

	sctx, stop := context.WithCancel(ctx)
	defer stop()
	go a.pingLoop(sctx, conn)
	return a.readLoop(sctx, conn)
}

func (a *Agent) handshake(ctx context.Context, conn *websocket.Conn) (welcome, error) {
	host, _ := os.Hostname()
	req, err := newRequest("hello", typeHello, hello{
		ProtocolVersions: []int{protocolVersion},
		SDK:              sdkInfo{Name: "runnerq-go", Version: sdkVersion(), Language: "go"},
		ExecutorID:       a.engine.InstanceID(),
		Hostname:         host,
		ActivityTypes:    a.engine.ActivityTypes(),
		MaxWorkers:       a.engine.MaxConcurrentActivities(),
		Capabilities:     a.h.capabilities(),
	})
	if err != nil {
		return welcome{}, err
	}
	if err := wsjson.Write(ctx, conn, req); err != nil {
		return welcome{}, err
	}
	var res envelope
	if err := wsjson.Read(ctx, conn, &res); err != nil {
		return welcome{}, err
	}
	if res.Error != nil {
		return welcome{}, fmt.Errorf("the Cloud rejected the handshake: %w", res.Error)
	}
	var w welcome
	if res.Kind != kindResponse || res.Type != typeHello {
		return w, fmt.Errorf("unexpected handshake reply %s/%s", res.Kind, res.Type)
	}
	if err := json.Unmarshal(res.Data, &w); err != nil {
		return w, fmt.Errorf("decode welcome: %w", err)
	}
	if w.Version != protocolVersion {
		return w, fmt.Errorf("the Cloud chose protocol version %d; this SDK speaks %d", w.Version, protocolVersion)
	}
	return w, nil
}

// pingLoop detects a dead gateway: the connection is closed when a ping goes
// unanswered.
func (a *Agent) pingLoop(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pctx, cancel := context.WithTimeout(ctx, pingInterval)
			err := conn.Ping(pctx)
			cancel()
			if err != nil && ctx.Err() == nil {
				conn.CloseNow()
				return
			}
		}
	}
}

func (a *Agent) readLoop(ctx context.Context, conn *websocket.Conn) error {
	var wg sync.WaitGroup
	defer wg.Wait()
	// In-flight requests are abandoned once the connection is gone: nobody
	// is left to read their responses.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for {
		var req envelope
		if err := wsjson.Read(ctx, conn, &req); err != nil {
			return err
		}
		if req.Kind != kindRequest {
			continue // the gateway sends no events yet
		}
		select {
		case a.sem <- struct{}{}:
		default:
			a.reply(ctx, conn, errorResponse(req, errorf(codeUnavailable, "agent is at its request limit")))
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-a.sem }()
			a.reply(ctx, conn, a.serve(ctx, req))
		}()
	}
}

// serve runs one request and builds its response. A panicking handler is
// reported as an internal error; it never takes down the worker.
func (a *Agent) serve(ctx context.Context, req envelope) (res envelope) {
	defer func() {
		if p := recover(); p != nil {
			a.log.Error("Conductor request handler panicked", "type", req.Type, "panic", p)
			res = errorResponse(req, errorf(codeInternal, "handler panicked"))
		}
	}()
	h, ok := a.table[req.Type]
	if !ok {
		return errorResponse(req, errorf(codeUnsupported, "this agent does not serve %q", req.Type))
	}
	rctx, cancel := context.WithTimeout(ctx, a.cfg.RequestTimeout)
	defer cancel()
	out, err := h(rctx, req.Data)
	if err != nil {
		var we *wireError
		if !errors.As(err, &we) {
			we = errorf(codeInternal, "%s", errorMessage(err))
		}
		return errorResponse(req, we)
	}
	data, err := json.Marshal(out)
	if err != nil {
		return errorResponse(req, errorf(codeInternal, "encode response: %v", err))
	}
	return envelope{V: req.V, Kind: kindResponse, ID: req.ID, Type: req.Type, Data: data}
}

func (a *Agent) reply(ctx context.Context, conn *websocket.Conn, res envelope) {
	wctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	if err := wsjson.Write(wctx, conn, res); err != nil && ctx.Err() == nil {
		a.log.Debug("Could not send Conductor response", "type", res.Type, "error", err)
	}
}

func newRequest(id, msgType string, data any) (envelope, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return envelope{}, err
	}
	return envelope{V: protocolVersion, Kind: kindRequest, ID: id, Type: msgType, Data: raw}, nil
}

func newEvent(msgType string, data any) (envelope, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return envelope{}, err
	}
	return envelope{V: protocolVersion, Kind: kindEvent, Type: msgType, Data: raw}, nil
}

func errorResponse(req envelope, e *wireError) envelope {
	return envelope{V: req.V, Kind: kindResponse, ID: req.ID, Type: req.Type, Error: e}
}

// sdkVersion reports the runnerq-go module version compiled into the binary.
func sdkVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	if info.Main.Path == sdkModulePath {
		return info.Main.Version
	}
	for _, dep := range info.Deps {
		if dep.Path == sdkModulePath {
			if dep.Replace != nil {
				return dep.Replace.Version
			}
			return dep.Version
		}
	}
	return "unknown"
}

// SessionID is the current Cloud session id, or "" while disconnected.
func (a *Agent) SessionID() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.conn == nil {
		return ""
	}
	return a.sessionID
}
