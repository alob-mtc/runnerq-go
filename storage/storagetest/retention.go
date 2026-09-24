package storagetest

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
)

// Retention: terminal workflow trees older than their TTL are deleted whole
// — activities, events, results and checkpoints, idempotency keys — and
// nothing else is.
var retentionTests = []conformanceTest{
	{"SweepsWholeTerminalTrees", testRetentionSweepsTrees},
	{"KeepsTreesWithLiveDescendants", testRetentionKeepsLiveTrees},
	{"CompletedAndFailedAgeSeparately", testRetentionSeparateTTLs},
	{"RespectsBatchSize", testRetentionBatchSize},
	{"ConcurrentSweepersDoNotDoubleCount", testRetentionConcurrentSweepers},
}

// retentionTTL is the shortest TTL the suite relies on. Backends may resolve
// TTLs to whole seconds, so trees are aged past it with a real wait.
const retentionTTL = time.Second

type tree struct {
	root, child, checkpoint uuid.UUID
	key                     string
}

// finishTree runs a two-activity workflow to its end through the public
// surface: the root spawns a keyed child, parks on it, the child stores a
// checkpoint and completes, the root wakes and ends with rootOutcome
// ("completed" or "failed").
func (s *suite) finishTree(rootOutcome string) tree {
	s.t.Helper()
	root := s.enqueueClaimed("r1", activity(withType("root")))
	tr := tree{root: root.ID, checkpoint: uuid.New(), key: "step:" + root.ID.String()}
	child := activity(withType("child"), withParent(root), withKey(tr.key, storage.BehaviorReturnExisting))
	if existing, err := s.b.EnqueueIdempotent(s.ctx, &child); err != nil || existing != nil {
		s.t.Fatalf("spawn child: %v %v", existing, err)
	}
	tr.child = child.ID
	if err := need[storage.DependencyStorage](s.t, s.b).YieldForResult(s.ctx, root.ID, child.ID, &child.ID, time.Now().UTC().Add(time.Hour), "r1", "await", "child"); err != nil {
		s.t.Fatal(err)
	}
	s.claim("c", child, "child")
	if err := s.b.StoreResult(s.ctx, tr.checkpoint, child.ID, storage.ActivityResult{Data: raw(1), State: storage.ResultOk}, "run:step"); err != nil {
		s.t.Fatal(err)
	}
	if err := s.b.AckSuccess(s.ctx, child.ID, raw("child"), "c"); err != nil {
		s.t.Fatal(err)
	}
	s.claim("r2", root, "root")
	switch rootOutcome {
	case "completed":
		if err := s.b.AckSuccess(s.ctx, root.ID, raw("root"), "r2"); err != nil {
			s.t.Fatal(err)
		}
	case "failed":
		if _, err := s.b.AckFailure(s.ctx, root.ID, storage.NewNonRetryableFailure("no"), "r2"); err != nil {
			s.t.Fatal(err)
		}
	}
	s.wantStatus(root.ID, rootOutcome)
	return tr
}

func (s *suite) age() {
	time.Sleep(retentionTTL + 500*time.Millisecond)
}

func (s *suite) sweep(policy storage.RetentionPolicy, batch int, want uint64) {
	s.t.Helper()
	n, err := s.b.CleanupExpired(s.ctx, policy, batch)
	if err != nil || n != want {
		s.t.Fatalf("cleanup: n=%d err=%v, want %d", n, err, want)
	}
}

func (s *suite) wantTree(tr tree, present bool) {
	s.t.Helper()
	for _, id := range []uuid.UUID{tr.root, tr.child} {
		snap, err := s.b.GetActivity(s.ctx, id)
		if err != nil {
			s.t.Fatal(err)
		}
		if (snap != nil) != present {
			s.t.Fatalf("activity %s present=%v, want %v", id, snap != nil, present)
		}
		if evs := s.events(id); (len(evs) > 0) != present {
			s.t.Fatalf("activity %s has %d events, want present=%v", id, len(evs), present)
		}
	}
	for _, id := range []uuid.UUID{tr.child, tr.checkpoint} {
		if res := s.result(id); (res != nil) != present {
			s.t.Fatalf("result %s present=%v, want %v", id, res != nil, present)
		}
	}
	if evs := s.events(tr.checkpoint); (len(evs) > 0) != present {
		s.t.Fatalf("checkpoint %s has %d events, want present=%v", tr.checkpoint, len(evs), present)
	}
	_, err := s.b.LookupIdempotencyActivityID(s.ctx, tr.key)
	if (err == nil) != present {
		s.t.Fatalf("key %q lookup err=%v, want present=%v", tr.key, err, present)
	}
	if sub, err := s.b.GetSubtree(s.ctx, tr.root); err != nil || (len(sub) > 0) != present {
		s.t.Fatalf("subtree of %s has %d rows (%v), want present=%v", tr.root, len(sub), err, present)
	}
}

func testRetentionSweepsTrees(t *testing.T, h Harness) {
	s := newSuite(t, h)
	old := s.finishTree("completed")
	s.wantTree(old, true)
	s.sweep(storage.RetentionPolicy{}, 100, 0) // zero TTLs keep everything
	s.age()
	fresh := s.finishTree("completed")
	s.sweep(storage.RetentionPolicy{Completed: retentionTTL}, 100, 1)
	s.wantTree(old, false)
	s.wantTree(fresh, true)
	s.sweep(storage.RetentionPolicy{Completed: retentionTTL}, 100, 0)
}

// A root that is terminal while a descendant is not keeps the whole tree.
func testRetentionKeepsLiveTrees(t *testing.T, h Harness) {
	s := newSuite(t, h)
	root := s.enqueueClaimed("r", activity(withType("root")))
	child := s.enqueue(activity(withType("child"), withParent(root)))
	if err := s.b.AckSuccess(s.ctx, root.ID, nil, "r"); err != nil {
		t.Fatal(err)
	}
	s.age()
	s.sweep(storage.RetentionPolicy{Completed: retentionTTL}, 100, 0)
	if snap, err := s.b.GetActivity(s.ctx, root.ID); err != nil || snap == nil {
		t.Fatalf("root with a live child was swept: %v", err)
	}
	s.claim("c", child, "child")
	if err := s.b.AckSuccess(s.ctx, child.ID, nil, "c"); err != nil {
		t.Fatal(err)
	}
	s.sweep(storage.RetentionPolicy{Completed: retentionTTL}, 100, 1)
}

func testRetentionSeparateTTLs(t *testing.T, h Harness) {
	s := newSuite(t, h)
	completed := s.finishTree("completed")
	failed := s.finishTree("failed")
	s.age()
	s.sweep(storage.RetentionPolicy{Failed: retentionTTL}, 100, 1)
	s.wantTree(failed, false)
	s.wantTree(completed, true)
	s.sweep(storage.RetentionPolicy{Completed: retentionTTL}, 100, 1)
	s.wantTree(completed, false)
}

func testRetentionBatchSize(t *testing.T, h Harness) {
	s := newSuite(t, h)
	for range 3 {
		s.finishTree("completed")
	}
	s.age()
	total := uint64(0)
	for range 5 {
		n, err := s.b.CleanupExpired(s.ctx, storage.RetentionPolicy{Completed: retentionTTL}, 2)
		if err != nil || n > 2 {
			t.Fatalf("batch: n=%d err=%v, cap was 2", n, err)
		}
		total += n
		if n == 0 {
			break
		}
	}
	if total != 3 {
		t.Fatalf("drained %d trees, want 3", total)
	}
}

// Sweepers on two handles never delete or count the same tree twice.
func testRetentionConcurrentSweepers(t *testing.T, h Harness) {
	s := newSuite(t, h)
	const trees = 3
	for range trees {
		s.finishTree("completed")
	}
	s.age()
	handles := []storage.Storage{s.b, s.another()}
	counts := make(chan uint64, len(handles))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, b := range handles {
		wg.Go(func() {
			<-start
			n, err := b.CleanupExpired(s.ctx, storage.RetentionPolicy{Completed: retentionTTL}, 100)
			if err != nil {
				t.Errorf("sweep: %v", err)
			}
			counts <- n
		})
	}
	close(start)
	wg.Wait()
	close(counts)
	total := uint64(0)
	for n := range counts {
		total += n
	}
	if total != trees {
		t.Fatalf("sweepers counted %d trees, want %d", total, trees)
	}
}
