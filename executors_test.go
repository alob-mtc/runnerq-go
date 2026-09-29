package runnerq

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
)

// reportingBackend is a lifecycle backend that records executor reports.
type reportingBackend struct {
	*lifecycleBackend
	mu      sync.Mutex
	started []storage.ExecutorInfo
	state   func() storage.ExecutorState
	stopped chan string
}

func (b *reportingBackend) ExecutorStarted(info storage.ExecutorInfo, state func() storage.ExecutorState) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.started = append(b.started, info)
	b.state = state
}

func (b *reportingBackend) ExecutorStopped(id string) { b.stopped <- id }

func TestEngineReportsItselfToItsBackend(t *testing.T) {
	b := &reportingBackend{lifecycleBackend: newLifecycleBackend(), stopped: make(chan string, 1)}
	cfg := DefaultWorkerConfig()
	cfg.QueueName = "payments"
	cfg.MaxConcurrentActivities = 3
	e := NewWorkerEngineWithBackend(b, cfg)
	entered, release := make(chan struct{}), make(chan struct{})
	e.RegisterActivityWithName("charge", &funcHandler{fn: func(c ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
		close(entered)
		<-release
		return json.RawMessage(`true`), nil
	}})
	e.RegisterActivityWithName("refund", &funcHandler{})
	id := uuid.New()
	b.claims <- storage.QueuedActivity{ID: id, ActivityType: "charge", TimeoutSeconds: 30}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Start(ctx) }()
	receive(t, entered)

	b.mu.Lock()
	started, state := b.started, b.state
	b.mu.Unlock()
	if len(started) != 1 {
		t.Fatalf("started reported %d times", len(started))
	}
	info := started[0]
	if info.ID != e.InstanceID() || info.Queue != "payments" || info.MaxConcurrency != 3 ||
		len(info.ActivityTypes) != 2 || info.ActivityTypes[0] != "charge" || info.StartedAt.IsZero() {
		t.Fatalf("info: %+v", info)
	}
	st := state()
	if st.Draining || len(st.Running) != 1 || st.Running[0].ID != id || st.Running[0].Type != "charge" {
		t.Fatalf("state while running: %+v", st)
	}

	cancel()
	deadline := time.Now().Add(4 * time.Second)
	for !state().Draining {
		if time.Now().After(deadline) {
			t.Fatal("not draining after the stop began")
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(release)
	receive(t, b.ack)
	if got := receive(t, b.stopped); got != e.InstanceID() {
		t.Fatalf("stopped %q", got)
	}
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
}
