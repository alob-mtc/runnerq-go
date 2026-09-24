package storagetest

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
)

// Inspection: the read surface the console and inspector build on. Status
// names, lineage queries, event history, the live event stream, and
// worker-pool capacity.
var inspectionTests = []conformanceTest{
	{"StatusListsAndStatsAgree", testStatusListsAndStats},
	{"LineageQueries", testLineage},
	{"EventsAreOrderedHistory", testEventHistory},
	{"EventStreamDeliversCommittedEvents", testEventStream},
	{"WorkerPoolsCountTowardCapacity", testWorkerPools},
}

// stats reads through a fresh handle so a per-handle cache cannot serve a
// stale snapshot.
func (s *suite) stats() storage.QueueStats {
	s.t.Helper()
	st, err := s.another().Stats(s.ctx)
	if err != nil || st == nil {
		s.t.Fatalf("stats: %+v %v", st, err)
	}
	return *st
}

func ids(snaps []storage.ActivitySnapshot) map[uuid.UUID]bool {
	out := make(map[uuid.UUID]bool, len(snaps))
	for _, sn := range snaps {
		out[sn.ID] = true
	}
	return out
}

func testStatusListsAndStats(t *testing.T, h Harness) {
	s := newSuite(t, h)
	pending := s.enqueue(activity(withType("pending")))
	scheduled := s.enqueue(activity(withType("scheduled"), withScheduledAt(time.Now().UTC().Add(time.Hour))))
	processing := s.enqueueClaimed("p", activity(withType("processing")))
	waiting := s.enqueueClaimed("w", activity(withType("waiting")))
	if err := s.b.Yield(s.ctx, waiting.ID, time.Now().UTC().Add(time.Hour), "w", "sleep", "nap"); err != nil {
		t.Fatal(err)
	}
	retrying := s.enqueueClaimed("r", activity(withType("retrying"), withRetryDelay(3600, 0)))
	if _, err := s.b.AckFailure(s.ctx, retrying.ID, storage.NewRetryableFailure("later"), "r"); err != nil {
		t.Fatal(err)
	}
	completed := s.enqueueClaimed("c", activity(withType("completed")))
	if err := s.b.AckSuccess(s.ctx, completed.ID, nil, "c"); err != nil {
		t.Fatal(err)
	}
	failed := s.enqueueClaimed("f", activity(withType("failed")))
	if _, err := s.b.AckFailure(s.ctx, failed.ID, storage.NewNonRetryableFailure("no"), "f"); err != nil {
		t.Fatal(err)
	}
	dead := s.enqueueClaimed("d", activity(withType("dead"), withMaxRetries(1)))
	if _, err := s.b.AckFailure(s.ctx, dead.ID, storage.NewRetryableFailure("no"), "d"); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		id     uuid.UUID
		status string
	}{
		{pending.ID, "pending"}, {scheduled.ID, "scheduled"}, {processing.ID, "processing"}, {waiting.ID, "waiting"},
		{retrying.ID, "retrying"}, {completed.ID, "completed"}, {failed.ID, "failed"}, {dead.ID, "dead_letter"},
	} {
		s.wantStatus(tc.id, tc.status)
		roots, err := s.b.ListRecentRoots(s.ctx, tc.status, 0, 10)
		if err != nil || len(roots) != 1 || roots[0].ID != tc.id {
			t.Fatalf("roots with status %s = %v err=%v, want just %s", tc.status, ids(roots), err, tc.id)
		}
		all, err := s.b.ListRecentActivities(s.ctx, tc.status, 0, 10)
		if err != nil || len(all) != 1 || all[0].ID != tc.id {
			t.Fatalf("activities with status %s = %v err=%v", tc.status, ids(all), err)
		}
	}
	if roots, err := s.b.ListRecentRoots(s.ctx, "", 0, 100); err != nil || len(roots) != 8 {
		t.Fatalf("all roots = %d err=%v, want 8", len(roots), err)
	}
	for _, tc := range []struct {
		name string
		list func() ([]storage.ActivitySnapshot, error)
		want uuid.UUID
	}{
		{"pending", func() ([]storage.ActivitySnapshot, error) { return s.b.ListPending(s.ctx, 0, 10) }, pending.ID},
		{"processing", func() ([]storage.ActivitySnapshot, error) { return s.b.ListProcessing(s.ctx, 0, 10) }, processing.ID},
		{"scheduled", func() ([]storage.ActivitySnapshot, error) { return s.b.ListScheduled(s.ctx, 0, 10) }, scheduled.ID},
		{"completed", func() ([]storage.ActivitySnapshot, error) { return s.b.ListCompleted(s.ctx, 0, 10) }, completed.ID},
	} {
		got, err := tc.list()
		if err != nil || !ids(got)[tc.want] {
			t.Fatalf("list %s = %v err=%v, want it to include %s", tc.name, ids(got), err, tc.want)
		}
	}
	if records, err := s.b.ListDeadLetter(s.ctx, 0, 10); err != nil || len(records) != 1 || records[0].Activity.ID != dead.ID {
		t.Fatalf("dead letter list = %+v err=%v", records, err)
	}

	st := s.stats()
	if st.Pending != 1 || st.Processing != 1 || st.Scheduled != 1 || st.Waiting != 1 || st.Retrying != 1 || st.Failed != 1 || st.DeadLetter != 1 {
		t.Fatalf("stats = %+v", st)
	}
	if st.ActiveWorkers != 1 || st.Roots.Completed != 1 || st.Roots.Pending != 1 || st.ByPriority.Normal == 0 {
		t.Fatalf("stats = %+v", st)
	}
	if snap, err := s.b.GetActivity(s.ctx, uuid.New()); err != nil || snap != nil {
		t.Fatalf("unknown activity: %+v %v", snap, err)
	}
}

// Children and subtrees follow parent_activity_id / root_activity_id.
func testLineage(t *testing.T, h Harness) {
	s := newSuite(t, h)
	root := s.enqueue(activity(withType("root")))
	child := s.enqueue(activity(withType("child"), withParent(root)))
	grandchild := s.enqueue(activity(withType("grandchild"), withParent(child)))
	other := s.enqueue(activity(withType("other")))

	snap := s.snapshot(grandchild.ID)
	if snap.ParentActivityID == nil || *snap.ParentActivityID != child.ID || snap.RootActivityID == nil || *snap.RootActivityID != root.ID || snap.Depth != 2 {
		t.Fatalf("grandchild lineage: %+v", snap)
	}
	children, err := s.b.GetChildren(s.ctx, root.ID, 0, 10)
	if err != nil || len(children) != 1 || children[0].ID != child.ID {
		t.Fatalf("children of root = %v err=%v", ids(children), err)
	}
	sub, err := s.b.GetSubtree(s.ctx, root.ID)
	if err != nil || len(sub) != 3 || !ids(sub)[root.ID] || !ids(sub)[child.ID] || !ids(sub)[grandchild.ID] {
		t.Fatalf("subtree of root = %v err=%v", ids(sub), err)
	}
	roots, err := s.b.ListRecentRoots(s.ctx, "", 0, 10)
	if err != nil || len(roots) != 2 || !ids(roots)[root.ID] || !ids(roots)[other.ID] {
		t.Fatalf("roots = %v err=%v", ids(roots), err)
	}
	if err := s.b.RecordSpawnLinked(s.ctx, child.ID, other.ID); err != nil {
		t.Fatal(err)
	}
	s.wantEvent(child.ID, storage.EventSpawnLinked)
}

// The event history of an activity is its lifecycle in order, with the
// acting worker on the events an execution produces.
func testEventHistory(t *testing.T, h Harness) {
	s := newSuite(t, h)
	a := s.enqueueClaimed("w1", activity(withRetryDelay(0, 0)))
	if _, err := s.b.AckFailure(s.ctx, a.ID, storage.NewRetryableFailure("again"), "w1"); err != nil {
		t.Fatal(err)
	}
	s.claim("w2", a)
	if err := s.b.AckSuccess(s.ctx, a.ID, nil, "w2"); err != nil {
		t.Fatal(err)
	}
	evs := s.events(a.ID)
	want := []storage.ActivityEventType{storage.EventEnqueued, storage.EventDequeued, storage.EventRetrying, storage.EventDequeued, storage.EventCompleted}
	if len(evs) != len(want) {
		t.Fatalf("events = %+v, want %v", evs, want)
	}
	for i, ev := range evs {
		if ev.EventType != want[i] || ev.ActivityID != a.ID {
			t.Fatalf("event %d = %+v, want %s", i, ev, want[i])
		}
		if i > 0 && ev.Timestamp.Before(evs[i-1].Timestamp) {
			t.Fatalf("events out of order: %+v", evs)
		}
	}
	if evs[4].WorkerID == nil || *evs[4].WorkerID != "w2" || evs[2].WorkerID == nil || *evs[2].WorkerID != "w1" {
		t.Fatalf("acting workers not recorded: %+v", evs)
	}
	if evs, err := s.b.GetActivityEvents(s.ctx, a.ID, 2); err != nil || len(evs) != 2 {
		t.Fatalf("limited events = %d err=%v", len(evs), err)
	}
}

// Events committed after a subscription are delivered to it, from another
// process, and the stream ends when the context does.
func testEventStream(t *testing.T, h Harness) {
	s := newSuite(t, h)
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	events, err := s.another().EventStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	a := s.enqueue(activity())
	deadline := time.After(wait)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("event stream closed")
			}
			if ev.ActivityID == a.ID && ev.EventType == storage.EventEnqueued {
				cancel()
				closed := make(chan struct{})
				go func() {
					defer close(closed)
					for range events {
					}
				}()
				receive(t, closed, "event stream to close after cancellation")
				return
			}
		case <-deadline:
			t.Fatal("Enqueued event never arrived")
		}
	}
}

// Registered pools add up to the cluster's MaxWorkers until deregistered.
func testWorkerPools(t *testing.T, h Harness) {
	s := newSuite(t, h)
	one, two := uuid.New(), uuid.New()
	for _, p := range []storage.WorkerPoolInfo{
		{PoolID: one, QueueName: s.queue, MaxWorkers: 4, ActivityTypes: []string{"a"}},
		{PoolID: two, QueueName: s.queue, MaxWorkers: 2},
	} {
		if err := s.b.RegisterWorkerPool(s.ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.b.HeartbeatWorkerPool(s.ctx, one); err != nil {
		t.Fatal(err)
	}
	if st := s.stats(); st.MaxWorkers == nil || *st.MaxWorkers != 6 {
		t.Fatalf("max workers = %v, want 6", st.MaxWorkers)
	}
	if err := s.b.DeregisterWorkerPool(s.ctx, one); err != nil {
		t.Fatal(err)
	}
	if st := s.stats(); st.MaxWorkers == nil || *st.MaxWorkers != 2 {
		t.Fatalf("max workers after deregister = %v, want 2", st.MaxWorkers)
	}
}
