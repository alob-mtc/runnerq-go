package runnerq

import (
	"sync"
	"time"

	"github.com/alob-mtc/runnerq-go/executor"
)

// MetricsSink allows collecting metrics about activity processing.
// Implement this interface to integrate with your preferred metrics system.
type MetricsSink interface {
	IncCounter(name string, value uint64)
	ObserveDuration(name string, dur time.Duration)
}

// NoopMetrics is the default metrics sink that discards all metrics.
type NoopMetrics struct{}

func (NoopMetrics) IncCounter(_ string, _ uint64)             {}
func (NoopMetrics) ObserveDuration(_ string, _ time.Duration) {}

// countingMetrics counts every metric the engine emits, for its executor
// snapshots, and passes each on to the configured sink unchanged.
type countingMetrics struct {
	sink MetricsSink

	mu     sync.Mutex
	counts map[string]uint64
	last   map[string]time.Duration
}

func newCountingMetrics(sink MetricsSink) *countingMetrics {
	return &countingMetrics{sink: sink, counts: map[string]uint64{}, last: map[string]time.Duration{}}
}

func (m *countingMetrics) IncCounter(name string, value uint64) {
	m.mu.Lock()
	m.counts[name] += value
	m.mu.Unlock()
	m.sink.IncCounter(name, value)
}

func (m *countingMetrics) ObserveDuration(name string, dur time.Duration) {
	m.mu.Lock()
	m.last[name] = dur
	m.mu.Unlock()
	m.sink.ObserveDuration(name, dur)
}

func (m *countingMetrics) counters() executor.Counters {
	m.mu.Lock()
	defer m.mu.Unlock()
	return executor.Counters{
		Claimed:           m.counts[metricStarted],
		Succeeded:         m.counts["activity_completed"],
		Retried:           m.counts["activity_retry"],
		Failed:            m.counts["activity_failed_non_retry"],
		TimedOut:          m.counts["activity_timeout"],
		DeadLettered:      m.counts[metricDeadLettered],
		ClaimsLost:        m.counts["activity_claim_lost"],
		HeartbeatFailures: m.counts["activity_heartbeat_failed"],
		LastClaimLag:      m.last[metricClaimLag],
	}
}

// Metrics the engine emits for its snapshots (and any sink).
const (
	// metricStarted: an activity started executing here.
	metricStarted = "activity_started"
	// metricDeadLettered: an activity ran out of attempts.
	metricDeadLettered = "activity_dead_lettered"
	// metricClaimLag: how long an activity waited to be claimed after it
	// was due.
	metricClaimLag = "activity_claim_lag"
)
