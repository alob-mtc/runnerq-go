package executor

import (
	"context"
	"sync"
	"time"
)

// Notifier is a Source that says when its state changes: an activity
// starts or finishes, or a drain begins. Changed returns a channel that is
// closed at the next change after the call, so a reporter can report soon
// after a change rather than at its next interval. The engine is one.
type Notifier interface {
	Changed() <-chan struct{}
}

// Signal tells any number of waiters about changes. The zero value is
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

// Report calls send now, then every interval, until ctx ends. When src is
// a Notifier it also sends soon after each change, but never sooner than
// minGap after the previous send: changes in the meantime go out together
// in the next report. interval is read before each wait, so it may change.
func Report(ctx context.Context, src Source, interval func() time.Duration, minGap time.Duration, send func()) {
	n, _ := src.(Notifier)
	timer := time.NewTimer(0)
	timer.Stop()
	defer timer.Stop()
	for {
		// Taken before the send, so a change during it isn't missed.
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
