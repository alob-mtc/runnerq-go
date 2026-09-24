// Package storagetest is the conformance suite for storage.Storage backends.
//
// The durable-execution guarantees RunnerQ makes — claimed once, fenced
// acknowledgements, lease recovery, checkpoints that survive replay, parents
// that wake when awaited results arrive, atomic idempotency — are all promises the
// storage backend keeps; the engine only composes them. This suite states
// those promises as tests, so a backend that passes it can be swapped in for
// another without the engine noticing.
//
// A backend's own test file runs the suite through a Harness:
//
//	func TestConformance(t *testing.T) {
//		storagetest.Run(t, myHarness{})
//	}
//
// Every test opens a fresh backend on its own queue name and drives it through
// storage.Storage and the optional interfaces alone. The harness supplies
// only what those interfaces cannot express: opening a backend on a named
// queue, and moving a lease into the past.
//
// The optional interfaces durable execution depends on — CheckpointStorage,
// DependencyStorage, AttemptLeaseStorage, SpawnStorage — are required: their
// tests fail when the backend lacks them. BatchQueueStorage and ResultWaiter
// are optimisations the engine can do without; their tests are skipped.
package storagetest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
)

// Harness adapts one backend implementation to the suite.
type Harness interface {
	// Open returns a backend serving queue, closed by t.Cleanup. Two calls
	// with the same queue must return independent handles that share the
	// store — the suite uses that to stand in for two processes.
	Open(t *testing.T, queue string) storage.Storage
	// ExpireLease moves the lease of a processing activity into the past, as
	// the passage of time would, so the backend's own RequeueExpired sees it.
	ExpireLease(ctx context.Context, b storage.Storage, activityID uuid.UUID) error
}

// wait is the suite's bound on operations that block on another party:
// blocking claims, result waits, event delivery. Generous enough for a slow
// CI database, short enough that a missed wake fails the run instead of
// hanging it.
const wait = 10 * time.Second

// Run executes the whole suite against h.
func Run(t *testing.T, h Harness) {
	t.Helper()
	for _, g := range []struct {
		name  string
		tests []conformanceTest
	}{
		{"Claim", claimTests},
		{"Acknowledge", ackTests},
		{"LeaseRecovery", leaseTests},
		{"Durable", durableTests},
		{"Idempotency", idempotencyTests},
		{"Retention", retentionTests},
		{"Inspection", inspectionTests},
	} {
		t.Run(g.name, func(t *testing.T) {
			for _, tc := range g.tests {
				t.Run(tc.name, func(t *testing.T) { tc.fn(t, h) })
			}
		})
	}
}

type conformanceTest struct {
	name string
	fn   func(t *testing.T, h Harness)
}

// suite is the per-test environment: a queue name and the primary handle.
type suite struct {
	t     *testing.T
	h     Harness
	queue string
	b     storage.Storage
	ctx   context.Context
}

func newSuite(t *testing.T, h Harness) *suite {
	t.Helper()
	queue := "ct_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
	return &suite{t: t, h: h, queue: queue, b: h.Open(t, queue), ctx: context.Background()}
}

// another opens a second handle on the suite's queue: a separate process.
func (s *suite) another() storage.Storage {
	s.t.Helper()
	return s.h.Open(s.t, s.queue)
}

// activity is a runnable activity with the suite's defaults; options adjust
// it. The 30s timeout keeps leases comfortably in the future.
func activity(opts ...func(*storage.QueuedActivity)) storage.QueuedActivity {
	a := storage.QueuedActivity{
		ID:             uuid.New(),
		ActivityType:   "conformance",
		Payload:        json.RawMessage(`{"k":"v"}`),
		Priority:       storage.PriorityNormal,
		MaxRetries:     3,
		TimeoutSeconds: 30,
		CreatedAt:      time.Now().UTC(),
		Metadata:       map[string]string{},
	}
	for _, o := range opts {
		o(&a)
	}
	return a
}

func withType(typ string) func(*storage.QueuedActivity) {
	return func(a *storage.QueuedActivity) { a.ActivityType = typ }
}
func withPriority(p storage.ActivityPriority) func(*storage.QueuedActivity) {
	return func(a *storage.QueuedActivity) { a.Priority = p }
}
func withMaxRetries(n uint32) func(*storage.QueuedActivity) {
	return func(a *storage.QueuedActivity) { a.MaxRetries = n }
}
func withRetryDelay(base, cap uint64) func(*storage.QueuedActivity) {
	return func(a *storage.QueuedActivity) { a.RetryDelaySeconds, a.MaxRetryDelaySeconds = base, cap }
}
func withParent(parent storage.QueuedActivity) func(*storage.QueuedActivity) {
	return func(a *storage.QueuedActivity) {
		p := parent.ID
		a.ParentActivityID = &p
		a.RootActivityID = parent.RootActivityID
		if a.RootActivityID == uuid.Nil {
			a.RootActivityID = parent.ID
		}
		a.Depth = parent.Depth + 1
	}
}
func withKey(key string, behavior storage.IdempotencyBehavior) func(*storage.QueuedActivity) {
	return func(a *storage.QueuedActivity) {
		a.IdempotencyKey = &storage.IdempotencyKeyConfig{Key: key, Behavior: behavior}
	}
}
func withScheduledAt(at time.Time) func(*storage.QueuedActivity) {
	return func(a *storage.QueuedActivity) { a.ScheduledAt = &at }
}
func withCreatedAt(at time.Time) func(*storage.QueuedActivity) {
	return func(a *storage.QueuedActivity) { a.CreatedAt = at }
}

func (s *suite) enqueue(a storage.QueuedActivity) storage.QueuedActivity {
	s.t.Helper()
	if err := s.b.Enqueue(s.ctx, a); err != nil {
		s.t.Fatalf("enqueue %s: %v", a.ID, err)
	}
	return a
}

// promote runs the scheduled-activity processor for backends that need it,
// so due scheduled/retrying rows become claimable through Dequeue.
func (s *suite) promote() {
	s.t.Helper()
	if s.b.SchedulesNatively() {
		return
	}
	if _, err := s.b.ProcessScheduled(s.ctx); err != nil {
		s.t.Fatalf("process scheduled: %v", err)
	}
}

// tryClaim is one non-blocking claim; nil when nothing is claimable.
func (s *suite) tryClaim(worker string, types ...string) *storage.QueuedActivity {
	s.t.Helper()
	s.promote()
	got, err := s.b.Dequeue(s.ctx, worker, 0, types)
	if err != nil {
		s.t.Fatalf("dequeue as %s: %v", worker, err)
	}
	return got
}

// claim claims exactly want under worker's token and fails otherwise.
func (s *suite) claim(worker string, want storage.QueuedActivity, types ...string) {
	s.t.Helper()
	got := s.tryClaim(worker, types...)
	if got == nil || got.ID != want.ID {
		s.t.Fatalf("dequeue as %s: got %v, want %s", worker, got, want.ID)
	}
}

// claimNothing asserts the queue has nothing claimable right now.
func (s *suite) claimNothing(types ...string) {
	s.t.Helper()
	if got := s.tryClaim("nobody", types...); got != nil {
		s.t.Fatalf("claimed %s (%s), want nothing claimable", got.ID, got.ActivityType)
	}
}

// enqueueClaimed enqueues a and claims it as worker.
func (s *suite) enqueueClaimed(worker string, a storage.QueuedActivity) storage.QueuedActivity {
	s.t.Helper()
	s.enqueue(a)
	s.claim(worker, a, a.ActivityType)
	return a
}

func (s *suite) snapshot(id uuid.UUID) storage.ActivitySnapshot {
	s.t.Helper()
	snap, err := s.b.GetActivity(s.ctx, id)
	if err != nil {
		s.t.Fatalf("get activity %s: %v", id, err)
	}
	if snap == nil {
		s.t.Fatalf("activity %s not found", id)
	}
	return *snap
}

// snapshotStatus maps a row's status — the vocabulary of the status filters
// on ListRecentRoots/ListRecentActivities — to the display name
// ActivitySnapshot.Status carries. Both are part of the contract: the
// console filters by the former and renders the latter.
var snapshotStatus = map[string]string{
	"pending":     "Pending",
	"processing":  "Running",
	"scheduled":   "Scheduled",
	"retrying":    "Retrying",
	"waiting":     "Waiting",
	"completed":   "Completed",
	"failed":      "Failed",
	"dead_letter": "DeadLetter",
}

// status returns the activity's row status (the filter vocabulary), derived
// from its snapshot's display name.
func (s *suite) status(id uuid.UUID) string {
	s.t.Helper()
	display := s.snapshot(id).Status
	for raw, d := range snapshotStatus {
		if d == display {
			return raw
		}
	}
	s.t.Fatalf("activity %s has unknown snapshot status %q", id, display)
	return ""
}

func (s *suite) wantStatus(id uuid.UUID, want string) {
	s.t.Helper()
	if got := s.status(id); got != want {
		s.t.Fatalf("activity %s status = %q, want %q", id, got, want)
	}
}

func (s *suite) result(id uuid.UUID) *storage.ActivityResult {
	s.t.Helper()
	res, err := s.b.GetResult(s.ctx, id)
	if err != nil {
		s.t.Fatalf("get result %s: %v", id, err)
	}
	return res
}

func (s *suite) wantResult(id uuid.UUID, state storage.ResultState) *storage.ActivityResult {
	s.t.Helper()
	res := s.result(id)
	if res == nil || res.State != state {
		s.t.Fatalf("result of %s = %+v, want state %v", id, res, state)
	}
	return res
}

func (s *suite) events(id uuid.UUID) []storage.ActivityEvent {
	s.t.Helper()
	evs, err := s.b.GetActivityEvents(s.ctx, id, 100)
	if err != nil {
		s.t.Fatalf("events of %s: %v", id, err)
	}
	return evs
}

func (s *suite) hasEvent(id uuid.UUID, typ storage.ActivityEventType) bool {
	s.t.Helper()
	for _, ev := range s.events(id) {
		if ev.EventType == typ {
			return true
		}
	}
	return false
}

func (s *suite) wantEvent(id uuid.UUID, typ storage.ActivityEventType) {
	s.t.Helper()
	if !s.hasEvent(id, typ) {
		s.t.Fatalf("activity %s has no %s event", id, typ)
	}
}

func (s *suite) expire(id uuid.UUID) {
	s.t.Helper()
	if err := s.h.ExpireLease(s.ctx, s.b, id); err != nil {
		s.t.Fatalf("expire lease of %s: %v", id, err)
	}
}

// reap runs the reaper and asserts how many rows it recovered.
func (s *suite) reap(want uint64) {
	s.t.Helper()
	n, err := s.b.RequeueExpired(s.ctx, 100)
	if err != nil || n != want {
		s.t.Fatalf("requeue expired: n=%d err=%v, want %d", n, err, want)
	}
}

// wantKind asserts err is a storage error of the given kind — the engine
// classifies outcomes by kind, so the kind is part of the contract.
func wantKind(t *testing.T, err error, kind storage.StorageErrorKind, what string) {
	t.Helper()
	se, ok := storage.IsStorageError(err)
	if !ok || se.Kind != kind {
		t.Fatalf("%s: err = %v, want storage error of kind %v", what, err, kind)
	}
}

// need returns the optional interface a durable-execution feature relies on,
// failing the test when the backend lacks it: without these the engine still
// runs, but not with the guarantees this suite certifies.
func need[T any](t *testing.T, b storage.Storage) T {
	t.Helper()
	x, ok := b.(T)
	if !ok {
		var zero T
		t.Fatalf("backend does not implement %T, which durable execution relies on", &zero)
	}
	return x
}

// optional returns an optional interface, skipping the test when absent —
// for capabilities the engine has a fallback for (batch claims, result waits).
func optional[T any](t *testing.T, b storage.Storage) T {
	t.Helper()
	x, ok := b.(T)
	if !ok {
		var zero T
		t.Skipf("backend does not implement optional %T", &zero)
	}
	return x
}

// receive waits for one value with the suite's timeout.
func receive[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(wait):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func raw(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("storagetest: marshal %v: %v", v, err))
	}
	return data
}
