package runnerq

import (
	"context"
	"sync/atomic"
)

// suspendKey is the context key under which a slotHolder is plumbed from
// processActivity down into ActivityFuture.GetResult. Untyped struct is the
// idiomatic pattern so external code can't collide.
type suspendKey struct{}

// slotHolder owns this activity's seat in the engine's worker semaphore.
// release() returns the seat so another activity can dispatch onto it;
// reacquire() blocks until a seat is available again. An atomic guards
// against double-release / double-reacquire so callers don't need to track
// held state themselves.
//
// Lease note: the engine's dequeue SQL already sets the row's
// lease_deadline_ms to cover the activity's whole declared timeout
// (lease = now + max(defaultLeaseMS, (timeout_seconds+10)*1000)), and the
// handler's context is bounded by that same timeout. A parent that
// release()s its slot and waits on children stays well within both
// budgets, so no separate lease-extension loop is required at this layer.
//
// Concurrency: a slotHolder represents one logical seat. If a handler
// fans out concurrent goroutines that all call GetResult on the same ctx,
// they share that single seat — release/reacquire is serialized by the
// CAS in each method, so the worst case is one goroutine winning the
// reacquire race and the others no-op'ing. Don't rely on per-goroutine
// slot accounting in that pattern.
type slotHolder struct {
	sem  chan struct{}
	held atomic.Bool
}

func newSlotHolder(sem chan struct{}) *slotHolder {
	h := &slotHolder{sem: sem}
	h.held.Store(true)
	return h
}

// release the slot back to the pool. Safe to call multiple times.
func (h *slotHolder) release() {
	if h.held.CompareAndSwap(true, false) {
		<-h.sem
	}
}

// reacquire blocks (respecting ctx) until a slot is free, then takes it.
// Safe to call when already held or when racing with another reacquire on
// the same holder — no-op in those cases. The CAS-then-send order matters:
// claim ownership of held first so concurrent callers no-op rather than
// both succeeding at the send and double-consuming semaphore tokens.
func (h *slotHolder) reacquire(ctx context.Context) error {
	if !h.held.CompareAndSwap(false, true) {
		return nil
	}
	select {
	case h.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		// Couldn't take the slot we'd claimed responsibility for — give the
		// claim back so a future reacquire can try again.
		h.held.Store(false)
		return ctx.Err()
	}
}

// suspendFromContext extracts the slotHolder plumbed by processActivity.
// Returns nil when GetResult is called outside a handler context, or when
// SuspendOnAwait is off (engine doesn't install the holder).
func suspendFromContext(ctx context.Context) *slotHolder {
	v := ctx.Value(suspendKey{})
	if v == nil {
		return nil
	}
	h, _ := v.(*slotHolder)
	return h
}

func withSuspendSlot(ctx context.Context, h *slotHolder) context.Context {
	return context.WithValue(ctx, suspendKey{}, h)
}
