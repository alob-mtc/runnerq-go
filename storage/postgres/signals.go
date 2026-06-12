package postgres

// Wake-up signalling between RunnerQ processes, built on LISTEN/NOTIFY with
// the table data as the source of truth.
//
// Design constraints this file exists to satisfy:
//
//  1. NOTIFY must never run inside a data transaction. Postgres serializes
//     the commit of every NOTIFY-carrying transaction on a single global
//     queue lock, so per-transition in-transaction notifications cap
//     cluster-wide commit throughput regardless of hardware. Instead, hot
//     paths report what happened AFTER their transaction commits (see
//     signaler), and a single goroutine per backend flushes tiny batched
//     notifications from outside any transaction.
//
//  2. Waiters can live in a different process than the worker that produces
//     what they're waiting for. A future's GetResult is routinely called
//     from a separate server process that only holds a backend handle, so
//     all wake-up state lives here in the storage backend (one LISTEN
//     connection + in-process fan-out per backend instance), never in the
//     engine.
//
//  3. Notifications are hints, not deliveries. LISTEN connections drop, and
//     a signal can be lost between a waiter's last check and its park. Every
//     wait path therefore re-checks the tables on a slow fallback interval;
//     correctness never depends on a notification arriving.
//
// Channels (queue names are capped at 48 chars, so these stay under the
// 63-byte identifier limit):
//
//	rq_w_<queue> — edge trigger: runnable work was committed
//	rq_r_<queue> — payload: comma-joined activity IDs whose results committed
//	rq_e_<queue> — edge trigger: new runnerq_events rows committed

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/alob-mtc/runnerq-go/storage"
)

const (
	// signalFlushInterval batches post-commit signals before notifying.
	// Bounds the per-process NOTIFY rate at ~20/sec/channel no matter the
	// activity throughput, and bounds the added wake-up latency.
	signalFlushInterval = 50 * time.Millisecond

	// workWaitProbe is how often a parked Dequeue re-probes the table while
	// blocked, covering lost work signals and scheduled/retrying rows coming
	// due (which emit no signal at their due time).
	workWaitProbe = 2 * time.Second

	// resultWaitFallback is how often a parked WaitForResult re-checks the
	// results table, covering lost result signals.
	resultWaitFallback = 5 * time.Second

	// eventTailInterval is the fallback cadence of the event tailer while
	// subscribers exist.
	eventTailInterval = time.Second

	// eventTailBatch caps rows read per tail query.
	eventTailBatch = 500

	// resultIDsPerNotify keeps result notification payloads under the 8KB
	// NOTIFY limit (36-byte UUIDs plus separators).
	resultIDsPerNotify = 200
)

func (b *PostgresBackend) workChannel() string   { return "rq_w_" + b.queueName }
func (b *PostgresBackend) resultChannel() string { return "rq_r_" + b.queueName }
func (b *PostgresBackend) eventChannel() string  { return "rq_e_" + b.queueName }

// ---------------------------------------------------------------------------
// Send side: post-commit batched signaler
// ---------------------------------------------------------------------------

type signalKind int

const (
	sigWork signalKind = iota
	sigEvent
	sigResult
)

type signalMsg struct {
	kind signalKind
	id   uuid.UUID // set for sigResult only
}

type signaler struct {
	b      *PostgresBackend
	ch     chan signalMsg
	cancel context.CancelFunc
	done   chan struct{}
}

func newSignaler(b *PostgresBackend) *signaler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &signaler{
		b:      b,
		ch:     make(chan signalMsg, 4096),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go s.run(ctx)
	return s
}

func (s *signaler) stop() {
	s.cancel()
	<-s.done
}

// send never blocks the hot path: if the buffer is full the signal is
// dropped, and receivers recover via their fallback probes.
func (s *signaler) send(m signalMsg) {
	select {
	case s.ch <- m:
	default:
	}
}

func (s *signaler) run(ctx context.Context) {
	defer close(s.done)
	ticker := time.NewTicker(signalFlushInterval)
	defer ticker.Stop()

	var workPending, eventPending bool
	resultIDs := make(map[uuid.UUID]struct{})

	flush := func() {
		if !workPending && !eventPending && len(resultIDs) == 0 {
			return
		}
		batch := &pgx.Batch{}
		if workPending {
			batch.Queue(`SELECT pg_notify($1, '')`, s.b.workChannel())
		}
		if eventPending {
			batch.Queue(`SELECT pg_notify($1, '')`, s.b.eventChannel())
		}
		if len(resultIDs) > 0 {
			ids := make([]string, 0, len(resultIDs))
			for id := range resultIDs {
				ids = append(ids, id.String())
			}
			for start := 0; start < len(ids); start += resultIDsPerNotify {
				end := min(start+resultIDsPerNotify, len(ids))
				batch.Queue(`SELECT pg_notify($1, $2)`, s.b.resultChannel(), strings.Join(ids[start:end], ","))
			}
		}
		// Not the caller's ctx: flushes happen on the signaler's own clock,
		// and a failed flush is harmless (receivers re-probe).
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := s.b.pool.SendBatch(flushCtx, batch).Close()
		cancel()
		if err != nil {
			slog.Debug("runnerq: signal flush failed; receivers fall back to polling", "error", err)
		}
		workPending, eventPending = false, false
		clear(resultIDs)
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case m := <-s.ch:
			switch m.kind {
			case sigWork:
				workPending = true
			case sigEvent:
				eventPending = true
			case sigResult:
				resultIDs[m.id] = struct{}{}
			}
		case <-ticker.C:
			flush()
		}
	}
}

// Post-commit signal helpers — call ONLY after the relevant transaction has
// committed, so a woken waiter's re-query is guaranteed to see the data.
func (b *PostgresBackend) signalWork()  { b.sig.send(signalMsg{kind: sigWork}) }
func (b *PostgresBackend) signalEvent() { b.sig.send(signalMsg{kind: sigEvent}) }
func (b *PostgresBackend) signalResult(id uuid.UUID) {
	b.sig.send(signalMsg{kind: sigResult, id: id})
}

// ---------------------------------------------------------------------------
// Receive side: shared watcher (one LISTEN connection per backend instance)
// ---------------------------------------------------------------------------

type eventSub struct {
	ch     chan storage.ActivityEvent
	closed bool
}

type watcher struct {
	b      *PostgresBackend
	cancel context.CancelFunc
	done   chan struct{}

	mu            sync.Mutex
	workWaiters   map[chan struct{}]struct{}
	resultWaiters map[uuid.UUID]map[chan struct{}]struct{}
	eventSubs     map[*eventSub]struct{}

	eventTailKick chan struct{}
}

// getWatcher lazily starts the watcher on first use, so backends that never
// block (pure producers, inspectors) don't pay for a LISTEN connection.
func (b *PostgresBackend) getWatcher() *watcher {
	b.watcherMu.Lock()
	defer b.watcherMu.Unlock()
	if b.watch == nil {
		ctx, cancel := context.WithCancel(context.Background())
		w := &watcher{
			b:             b,
			cancel:        cancel,
			done:          make(chan struct{}),
			workWaiters:   make(map[chan struct{}]struct{}),
			resultWaiters: make(map[uuid.UUID]map[chan struct{}]struct{}),
			eventSubs:     make(map[*eventSub]struct{}),
			eventTailKick: make(chan struct{}, 1),
		}
		go w.run(ctx)
		b.watch = w
	}
	return b.watch
}

func (w *watcher) stop() {
	w.cancel()
	<-w.done
}

func (w *watcher) run(ctx context.Context) {
	defer close(w.done)

	tailDone := make(chan struct{})
	go func() {
		defer close(tailDone)
		w.eventTailLoop(ctx)
	}()

	for ctx.Err() == nil {
		if err := w.listenOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("runnerq: notification listener error; reconnecting", "error", err)
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
			}
		}
	}
	<-tailDone
}

// listenOnce holds one pooled connection in LISTEN mode until it errors or
// the watcher stops. Waiters parked during an outage are woken by their own
// fallback probes, so a dropped connection degrades latency, not correctness.
func (w *watcher) listenOnce(ctx context.Context) error {
	conn, err := w.b.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	for _, ch := range []string{w.b.workChannel(), w.b.resultChannel(), w.b.eventChannel()} {
		if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{ch}.Sanitize()); err != nil {
			return err
		}
	}

	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		switch n.Channel {
		case w.b.workChannel():
			w.wakeWorkWaiters()
		case w.b.resultChannel():
			w.wakeResultWaiters(n.Payload)
		case w.b.eventChannel():
			w.kickEventTail()
		}
	}
}

// ---- work waiters (blocking Dequeue) ----

func (w *watcher) registerWork() chan struct{} {
	ch := make(chan struct{}, 1)
	w.mu.Lock()
	w.workWaiters[ch] = struct{}{}
	w.mu.Unlock()
	return ch
}

func (w *watcher) unregisterWork(ch chan struct{}) {
	w.mu.Lock()
	delete(w.workWaiters, ch)
	w.mu.Unlock()
}

func (w *watcher) wakeWorkWaiters() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for ch := range w.workWaiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// ---- result waiters (WaitForResult) ----

func (w *watcher) registerResult(id uuid.UUID) chan struct{} {
	ch := make(chan struct{}, 1)
	w.mu.Lock()
	m := w.resultWaiters[id]
	if m == nil {
		m = make(map[chan struct{}]struct{})
		w.resultWaiters[id] = m
	}
	m[ch] = struct{}{}
	w.mu.Unlock()
	return ch
}

func (w *watcher) unregisterResult(id uuid.UUID, ch chan struct{}) {
	w.mu.Lock()
	if m := w.resultWaiters[id]; m != nil {
		delete(m, ch)
		if len(m) == 0 {
			delete(w.resultWaiters, id)
		}
	}
	w.mu.Unlock()
}

func (w *watcher) wakeResultWaiters(payload string) {
	if payload == "" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.resultWaiters) == 0 {
		return
	}
	for _, part := range strings.Split(payload, ",") {
		id, err := uuid.Parse(part)
		if err != nil {
			continue
		}
		for ch := range w.resultWaiters[id] {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
}

// ---- event stream (console SSE) ----

func (w *watcher) kickEventTail() {
	select {
	case w.eventTailKick <- struct{}{}:
	default:
	}
}

func (w *watcher) subscribeEvents(ctx context.Context) <-chan storage.ActivityEvent {
	sub := &eventSub{ch: make(chan storage.ActivityEvent, 100)}
	w.mu.Lock()
	w.eventSubs[sub] = struct{}{}
	w.mu.Unlock()
	w.kickEventTail() // prompt the tailer to initialize its cursor

	go func() {
		<-ctx.Done()
		w.mu.Lock()
		delete(w.eventSubs, sub)
		sub.closed = true
		close(sub.ch)
		w.mu.Unlock()
	}()
	return sub.ch
}

func (w *watcher) hasEventSubs() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.eventSubs) > 0
}

func (w *watcher) fanOutEvents(events []storage.ActivityEvent) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for sub := range w.eventSubs {
		if sub.closed {
			continue
		}
		for _, ev := range events {
			// Non-blocking: a slow SSE subscriber drops events rather than
			// stalling the tailer; the console refetches state on demand and
			// per-activity timelines read the table directly.
			select {
			case sub.ch <- ev:
			default:
			}
		}
	}
}

// eventTailLoop streams runnerq_events rows to subscribers by tailing the id
// sequence. Unlike the previous design (full event JSON inside pg_notify),
// rows are read from the table after commit, so payloads aren't size-limited
// and writers don't pay any notification cost in their transactions.
//
// The cursor starts at MAX(id) when the first subscriber appears — matching
// the old LISTEN semantics of "events from subscription time onward". A row
// from a transaction that commits late (its id below an already-seen id) can
// be skipped; for a live dashboard feed that trade-off is acceptable, and
// the authoritative per-activity timeline reads the table directly.
func (w *watcher) eventTailLoop(ctx context.Context) {
	var cursor int64 = -1 // -1: uninitialized (no subscribers yet)
	ticker := time.NewTicker(eventTailInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.eventTailKick:
		case <-ticker.C:
		}

		if !w.hasEventSubs() {
			cursor = -1 // re-anchor at subscribe time for the next subscriber
			continue
		}

		if cursor < 0 {
			var maxID *int64
			err := w.b.pool.QueryRow(ctx,
				`SELECT MAX(id) FROM runnerq_events WHERE queue_name = $1`,
				w.b.queueName).Scan(&maxID)
			if err != nil {
				if ctx.Err() == nil {
					slog.Warn("runnerq: event tail cursor init failed", "error", err)
				}
				continue
			}
			cursor = 0
			if maxID != nil {
				cursor = *maxID
			}
		}

		for {
			events, lastID, err := w.b.readEventsAfter(ctx, cursor, eventTailBatch)
			if err != nil {
				if ctx.Err() == nil {
					slog.Warn("runnerq: event tail read failed", "error", err)
				}
				break
			}
			if len(events) == 0 {
				break
			}
			cursor = lastID
			w.fanOutEvents(events)
			if len(events) < eventTailBatch {
				break
			}
		}
	}
}

func (b *PostgresBackend) readEventsAfter(ctx context.Context, after int64, limit int) ([]storage.ActivityEvent, int64, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT id, activity_id, event_type, worker_id, detail, created_at
		FROM runnerq_events
		WHERE queue_name = $1 AND id > $2
		ORDER BY id ASC
		LIMIT $3`,
		b.queueName, after, limit)
	if err != nil {
		return nil, after, err
	}
	defer rows.Close()

	var events []storage.ActivityEvent
	lastID := after
	for rows.Next() {
		var ev storage.ActivityEvent
		if err := rows.Scan(&lastID, &ev.ActivityID, &ev.EventType, &ev.WorkerID, &ev.Detail, &ev.Timestamp); err != nil {
			return nil, after, err
		}
		events = append(events, ev)
	}
	return events, lastID, rows.Err()
}

// ---------------------------------------------------------------------------
// Public wait APIs
// ---------------------------------------------------------------------------

// WaitForResult blocks until the activity's result exists, the context is
// cancelled, or a storage error occurs. Implements storage.ResultWaiter.
//
// Works across processes: the producing worker and the waiting caller only
// need backend instances pointed at the same database — wake-ups travel via
// LISTEN/NOTIFY, with a slow table re-check as the lossy-notification
// fallback. Idle cost per waiting process is one parked channel per waiter
// plus one point-SELECT per waiter per resultWaitFallback, instead of the
// previous 10 queries/sec/waiter.
func (b *PostgresBackend) WaitForResult(ctx context.Context, activityID uuid.UUID) (*storage.ActivityResult, error) {
	w := b.getWatcher()
	ch := w.registerResult(activityID)
	defer w.unregisterResult(activityID, ch)

	for {
		// Check after registering so a result committed between the check and
		// the park can't be missed: its signal lands on ch.
		res, err := b.GetResult(ctx, activityID)
		if err != nil {
			return nil, err
		}
		if res != nil {
			return res, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
		case <-time.After(resultWaitFallback):
		}
	}
}
