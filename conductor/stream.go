package conductor

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
)

// Event streams tail the event log through QueryStorage and push batches to
// the Cloud as stream.events. They are resumable: a subscription starts
// after a cursor, so when the Cloud moves the stream to another executor it
// loses nothing.
//
// Event ids grow in insertion order but a transaction can commit late,
// surfacing an event below ids already delivered. Each poll therefore
// rescans the streamRescan ids below the cursor and sends only ids it has
// not sent. A subscription resuming after a cursor treats that window as
// already sent, so a failover does not replay it. Delivery is at least once
// in edge cases, so consumers dedupe by event id.

const (
	maxSubscriptions   = 4
	defaultStreamBatch = 200
	maxStreamBatch     = 1000
	defaultStreamDelay = 500 * time.Millisecond
	minStreamDelay     = 50 * time.Millisecond
	// streamRescan is how many ids below the cursor each poll looks at
	// again for late commits.
	streamRescan = 256
)

// streams holds one session's subscriptions; they end with the session.
type streams struct {
	a    *Agent
	conn *websocket.Conn

	mu   sync.Mutex
	subs map[string]context.CancelFunc
	wg   sync.WaitGroup
}

func newStreams(a *Agent, conn *websocket.Conn) *streams {
	return &streams{a: a, conn: conn, subs: map[string]context.CancelFunc{}}
}

// close stops every subscription and waits for them.
func (s *streams) close() {
	s.mu.Lock()
	for _, cancel := range s.subs {
		cancel()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *streams) subscribe(ctx context.Context, data json.RawMessage) (any, error) {
	qs := s.a.h.qs
	if qs == nil {
		return nil, errorf(codeUnsupported, "this agent's storage cannot stream events")
	}
	req, err := decode[subscribeRequest](data)
	if err != nil {
		return nil, err
	}
	filter, err := toStorageFilter(req.Filter)
	if err != nil {
		return nil, err
	}
	batch := req.MaxBatch
	if batch <= 0 {
		batch = defaultStreamBatch
	}
	batch = min(batch, maxStreamBatch)
	delay := time.Duration(req.MaxDelayMS) * time.Millisecond
	if delay <= 0 {
		delay = defaultStreamDelay
	}
	delay = max(delay, minStreamDelay)

	var cursor int64
	gap := false
	if req.AfterCursor != "" {
		c, err := strconv.ParseInt(req.AfterCursor, 10, 64)
		if err != nil || c < 0 {
			// Not a cursor this backend issued: start from the end and say so.
			gap = true
		} else {
			cursor = c
		}
	}
	if req.AfterCursor == "" || gap {
		// Validate the filter and find the log's end in one query.
		last, err := qs.QueryEvents(ctx, storage.EventQuery{Filter: filter, Desc: true, Limit: 1})
		if err != nil {
			return nil, err
		}
		if len(last.Items) > 0 {
			cursor = last.Items[0].ID
		}
	} else if _, err := qs.QueryEvents(ctx, storage.EventQuery{Filter: filter, Limit: 1}); err != nil {
		return nil, err // an invalid filter fails the subscribe, not the stream
	}

	s.mu.Lock()
	if len(s.subs) >= maxSubscriptions {
		s.mu.Unlock()
		return nil, errorf(codeResourceExhausted, "at most %d event subscriptions per executor", maxSubscriptions)
	}
	id := "sub_" + uuid.NewString()
	sctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s.subs[id] = cancel
	s.wg.Add(1)
	s.mu.Unlock()

	t := &tailer{s: s, id: id, filter: filter, batch: batch, delay: delay, cursor: cursor, sent: map[int64]bool{}}
	// What is already in the window below the start was delivered before (or
	// predates the subscription): only late commits into it are new.
	if seen, err := t.window(ctx, false); err == nil {
		for _, ev := range seen {
			t.sent[ev.ID] = true
		}
	}
	go func() {
		defer s.wg.Done()
		if gap {
			t.push(sctx, typeStreamGap, streamGap{SubscriptionID: id, SinceCursor: req.AfterCursor})
		}
		t.run(sctx)
	}()
	return subscription{SubscriptionID: id, Cursor: strconv.FormatInt(cursor, 10)}, nil
}

func (s *streams) unsubscribe(_ context.Context, data json.RawMessage) (any, error) {
	req, err := decode[subscription](data)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	cancel, ok := s.subs[req.SubscriptionID]
	delete(s.subs, req.SubscriptionID)
	s.mu.Unlock()
	if !ok {
		return nil, errorf(codeNotFound, "no subscription %q", req.SubscriptionID)
	}
	cancel()
	return struct{}{}, nil
}

// tailer is one subscription's poll loop.
type tailer struct {
	s      *streams
	id     string
	filter *storage.QueryFilter
	batch  int
	delay  time.Duration
	cursor int64
	// sent holds ids delivered within the rescan window, so a rescan never
	// repeats them.
	sent map[int64]bool
}

func (t *tailer) run(ctx context.Context) {
	defer func() {
		t.s.mu.Lock()
		delete(t.s.subs, t.id)
		t.s.mu.Unlock()
	}()
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		full, err := t.poll(ctx)
		if err != nil && ctx.Err() == nil {
			slog.Warn("Conductor event stream poll failed; retrying", "subscription", t.id, "error", err)
		}
		next := t.delay
		if full {
			next = 0 // catching up: keep reading
		}
		timer.Reset(next)
	}
}

// query reads events in (after, upTo] (upTo 0: unbounded), oldest first.
func (t *tailer) query(ctx context.Context, after, upTo int64, limit int, detail bool) ([]storage.EventRecord, error) {
	terms := []storage.QueryFilter{{Field: "seq", Op: storage.OpGt, Value: float64(after)}}
	if upTo > 0 {
		terms = append(terms, storage.QueryFilter{Field: "seq", Op: storage.OpLte, Value: float64(upTo)})
	}
	if t.filter != nil {
		terms = append(terms, *t.filter)
	}
	page, err := t.s.a.h.qs.QueryEvents(ctx, storage.EventQuery{
		Filter: &storage.QueryFilter{And: terms}, Limit: limit, IncludeDetail: detail && !t.s.a.h.metadataOnly(),
	})
	if err != nil {
		return nil, err
	}
	return page.Items, nil
}

// window reads the rescan window below the cursor.
func (t *tailer) window(ctx context.Context, detail bool) ([]storage.EventRecord, error) {
	if t.cursor <= 0 {
		return nil, nil
	}
	return t.query(ctx, max(t.cursor-streamRescan, 0), t.cursor, streamRescan, detail)
}

// poll sends late commits found in the rescan window and the next batch
// past the cursor, and reports whether that batch was full.
func (t *tailer) poll(ctx context.Context) (bool, error) {
	late, err := t.window(ctx, true)
	if err != nil {
		return false, err
	}
	fresh, err := t.query(ctx, t.cursor, 0, t.batch, true)
	if err != nil {
		return false, err
	}
	var pending []storage.EventRecord
	for _, ev := range append(late, fresh...) {
		if !t.sent[ev.ID] {
			pending = append(pending, ev)
		}
	}
	if err := t.send(ctx, pending); err != nil {
		return false, err
	}
	for id := range t.sent {
		if id <= t.cursor-streamRescan {
			delete(t.sent, id)
		}
	}
	return len(fresh) == t.batch, nil
}

// send pushes events as stream.events frames that each fit the Cloud's frame
// limit (an oversized frame would drop the session, and the resumed stream
// would read the same batch again). An event counts as sent, and moves the
// cursor, only once its frame is written, so each frame's cursor covers what
// the Cloud has received. An event too large for a frame by itself goes
// without its detail.
func (t *tailer) send(ctx context.Context, events []storage.EventRecord) error {
	budget := int(t.s.a.peerFrameLimit.Load()) - 1024 // the margin serve leaves too
	var (
		items []eventView
		ids   []int64
		size  int
	)
	flush := func() error {
		if len(items) == 0 {
			return nil
		}
		cursor := t.cursor
		for _, id := range ids {
			cursor = max(cursor, id)
		}
		if err := t.push(ctx, typeStreamEvents, streamEvents{SubscriptionID: t.id, Items: items, Cursor: strconv.FormatInt(cursor, 10)}); err != nil {
			return err
		}
		for _, id := range ids {
			t.sent[id] = true
		}
		t.cursor = cursor
		items, ids, size = nil, nil, 0
		return nil
	}
	for _, ev := range events {
		item := toEvent(ev)
		n := encodedSize(item)
		if budget > 0 && n > budget && item.Detail != nil {
			slog.Warn("Conductor event detail is over the frame limit; streaming the event without it",
				"subscription", t.id, "event", item.ID, "bytes", n, "limit", budget)
			item.Detail = nil
			n = encodedSize(item)
		}
		if budget > 0 && len(items) > 0 && size+n+1 > budget {
			if err := flush(); err != nil {
				return err
			}
		}
		items = append(items, item)
		ids = append(ids, ev.ID)
		size += n + 1
	}
	return flush()
}

func encodedSize(v any) int {
	b, err := json.Marshal(v)
	if err != nil {
		return 0
	}
	return len(b)
}

func (t *tailer) push(ctx context.Context, msgType string, data any) error {
	evt, err := newEvent(msgType, data)
	if err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	return wsjson.Write(wctx, t.s.conn, evt)
}
