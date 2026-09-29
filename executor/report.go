package executor

import (
	"context"
	"sync"
	"time"
)

// Notifier is implemented by a Source (such as the engine) that signals
// state changes: an activity starts or finishes, or a drain begins. Changed
// returns a channel closed at the next change after the call.
type Notifier interface {
	Changed() <-chan struct{}
}

// Signal implements Notifier for any number of waiters. The zero value is
// ready to use.
type Signal struct {
	mu sync.Mutex
	ch chan struct{}
}

// Changed returns a channel closed by the next Notify.
func (s *Signal) Changed() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ch == nil {
		s.ch = make(chan struct{})
	}
	return s.ch
}

// Notify wakes everyone waiting on Changed.
func (s *Signal) Notify() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ch != nil {
		close(s.ch)
		s.ch = nil
	}
}

// Report calls send now and then every interval() until ctx ends. When src
// is a Notifier it also sends after each change, but no sooner than minGap
// after the previous send, so bursts coalesce. interval is re-read each
// round, so it may change.
func Report(ctx context.Context, src Source, interval func() time.Duration, minGap time.Duration, send func()) {
	n, _ := src.(Notifier)
	timer := time.NewTimer(0)
	timer.Stop()
	defer timer.Stop()
	for {
		// Taken before send so a change during it isn't missed.
		var changed <-chan struct{}
		if n != nil {
			changed = n.Changed()
		}
		send()
		sent := time.Now()
		timer.Reset(interval())
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-changed:
			timer.Reset(time.Until(sent.Add(minGap)))
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
		}
	}
}
