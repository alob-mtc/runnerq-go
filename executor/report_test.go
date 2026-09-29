package executor

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type notifier struct{ Signal }

func (*notifier) Snapshot() Snapshot { return Snapshot{} }

func TestSignal(t *testing.T) {
	var s Signal
	s.Notify() // nobody waiting
	a, b := s.Changed(), s.Changed()
	if a != b {
		t.Fatal("waiters before a change share its channel")
	}
	s.Notify()
	for _, ch := range []<-chan struct{}{a, b} {
		select {
		case <-ch:
		default:
			t.Fatal("not woken")
		}
	}
	select {
	case <-s.Changed():
		t.Fatal("a new wait is woken by an earlier change")
	default:
	}
}

func TestReport(t *testing.T) {
	src := &notifier{}
	var sends atomic.Int32
	sent := make(chan time.Time, 16)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		Report(ctx, src, func() time.Duration { return time.Hour }, 100*time.Millisecond, func() {
			sends.Add(1)
			sent <- time.Now()
		})
	}()
	first := <-sent

	// A change is reported without waiting for the interval, but not
	// within the gap; a burst of changes goes out as one report.
	for range 5 {
		src.Notify()
		time.Sleep(5 * time.Millisecond)
	}
	second := <-sent
	if gap := second.Sub(first); gap < 100*time.Millisecond || gap > time.Second {
		t.Fatalf("second report %v after the first", gap)
	}
	time.Sleep(300 * time.Millisecond)
	if n := sends.Load(); n != 2 {
		t.Fatalf("%d reports for one burst of changes", n)
	}
	cancel()
	<-done
}

func TestReportWithoutNotifier(t *testing.T) {
	var sends atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	Report(ctx, plain{}, func() time.Duration { return 100 * time.Millisecond }, time.Second, func() { sends.Add(1) })
	if n := sends.Load(); n != 3 {
		t.Fatalf("%d reports in 250ms at a 100ms interval", n)
	}
}

type plain struct{}

func (plain) Snapshot() Snapshot { return Snapshot{} }
