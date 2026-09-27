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

	// resultIDsPerNotify keeps result notification payloads under the 8KB
	// NOTIFY limit (36-byte UUIDs plus separators).
	resultIDsPerNotify = 200
)

func (b *PostgresBackend) workChannel() string   { return "rq_w_" + b.queueName }
func (b *PostgresBackend) resultChannel() string { return "rq_r_" + b.queueName }

// ---------------------------------------------------------------------------
// Send side: post-commit batched signaler
// ---------------------------------------------------------------------------

type signalKind int

const (
	sigWork signalKind = iota
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

	var workPending bool
	resultIDs := make(map[uuid.UUID]struct{})

	flush := func() {
		if !workPending && len(resultIDs) == 0 {
			return
		}
		batch := &pgx.Batch{}
		if workPending {
			batch.Queue(`SELECT pg_notify($1, '')`, s.b.workChannel())
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
		workPending = false
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
func (b *PostgresBackend) signalWork() { b.sig.send(signalMsg{kind: sigWork}) }
func (b *PostgresBackend) signalResult(id uuid.UUID) {
	b.sig.send(signalMsg{kind: sigResult, id: id})
}

// ---------------------------------------------------------------------------
// Receive side: shared watcher (one LISTEN connection per backend instance)
// ---------------------------------------------------------------------------

type watcher struct {
	b      *PostgresBackend
	cancel context.CancelFunc
	done   chan struct{}

	mu            sync.Mutex
	workWaiters   map[chan struct{}]struct{}
	resultWaiters map[uuid.UUID]map[chan struct{}]struct{}
}

// getWatcher lazily starts the watcher on first use, so backends that never
// block (pure producers, read-only tools) don't pay for a LISTEN connection.
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

	for ctx.Err() == nil {
		if err := w.listenOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("runnerq: notification listener error; reconnecting", "error", err)
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
			}
		}
	}
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

	for _, ch := range []string{w.b.workChannel(), w.b.resultChannel()} {
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
	for part := range strings.SplitSeq(payload, ",") {
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
