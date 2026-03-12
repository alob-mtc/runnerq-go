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

func (NoopMetrics) IncCounter(_ string, _ uint64)          {}
func (NoopMetrics) ObserveDuration(_ string, _ time.Duration) {}

// Metric name constants for activity lifecycle counters.
const (
	MetricActivityEnqueued       = "runnerq_activity_enqueued_total"
	MetricActivityStarted        = "runnerq_activity_started_total"
	MetricActivityCompleted      = "runnerq_activity_completed_total"
	MetricActivityRetry          = "runnerq_activity_retry_total"
	MetricActivityFailedNonRetry = "runnerq_activity_failed_non_retry_total"
	MetricActivityTimeout        = "runnerq_activity_timeout_total"
	MetricActivityDeadLettered   = "runnerq_activity_dead_lettered_total"
	MetricActivityPanic          = "runnerq_activity_panic_total"
	MetricActivityHandlerNotFound = "runnerq_activity_handler_not_found_total"

	MetricActivityEnqueueError = "runnerq_activity_enqueue_error_total"
	MetricActivityDequeueError = "runnerq_activity_dequeue_error_total"
	MetricResultStoreError     = "runnerq_activity_result_store_error_total"
	MetricBackendError         = "runnerq_backend_error_total"

	MetricWorkerStarted             = "runnerq_worker_started_total"
	MetricWorkerStopped             = "runnerq_worker_stopped_total"
	MetricWorkerConcurrencySaturated = "runnerq_worker_concurrency_saturated_total"

	MetricSchedulerTick      = "runnerq_scheduler_tick_total"
	MetricSchedulerPollError = "runnerq_scheduler_poll_error_total"
	MetricReaperTick         = "runnerq_reaper_tick_total"
	MetricReaperError        = "runnerq_reaper_error_total"

	MetricActivityExecutionDuration = "runnerq_activity_execution_seconds"
	MetricActivityQueueLatency      = "runnerq_activity_queue_latency_seconds"
	MetricDequeueRoundtripDuration  = "runnerq_dequeue_roundtrip_seconds"
)
