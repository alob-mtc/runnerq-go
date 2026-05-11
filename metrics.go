package runnerq

import "time"

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
