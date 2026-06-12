package runnerq

import "time"

// RetentionConfig opts the engine into deleting old terminal workflow trees.
// Without it, activities, events, results, and idempotency keys are kept
// forever. The deletion unit is a whole tree (terminal root with no
// non-terminal descendants), so retries can never find their children's
// results missing. One engine per queue sweeps at a time (advisory-lock
// leadership in the backend); running it on every engine is safe.
type RetentionConfig struct {
	// Completed is how long trees whose root completed successfully are kept.
	// Zero keeps them forever.
	Completed time.Duration `json:"completed,omitempty"`
	// Failed is how long trees whose root is failed or dead_letter are kept —
	// a separate clock so failures can be held longer for inspection.
	// Zero keeps them forever.
	Failed time.Duration `json:"failed,omitempty"`
	// Interval is the sweep cadence. Defaults to 10 minutes.
	Interval time.Duration `json:"interval,omitempty"`
	// BatchSize is the max root trees deleted per sweep transaction.
	// Defaults to 100.
	BatchSize int `json:"batch_size,omitempty"`
}

// WorkerConfig controls queue behavior and resource usage.
type WorkerConfig struct {
	// QueueName is used as a prefix to avoid conflicts between different applications.
	QueueName string `json:"queue_name"`

	// MaxConcurrentActivities is the maximum number of activities processed concurrently.
	MaxConcurrentActivities int `json:"max_concurrent_activities"`

	// SchedulePollIntervalSeconds is the interval for polling scheduled activities.
	// When nil, defaults to 5 seconds.
	// Only effective for backends that don't handle scheduling natively in Dequeue().
	SchedulePollIntervalSeconds *uint64 `json:"schedule_poll_interval_seconds,omitempty"`

	// LeaseMS is the lease duration in milliseconds for claimed activities.
	// Defaults to 60000 ms (60s).
	LeaseMS *uint64 `json:"lease_ms,omitempty"`

	// ReaperIntervalSeconds is how often the reaper scans for expired leases.
	// Defaults to 5 seconds.
	ReaperIntervalSeconds *uint64 `json:"reaper_interval_seconds,omitempty"`

	// ReaperBatchSize is the max number of expired items to requeue per reaper tick.
	// Defaults to 100.
	ReaperBatchSize *int `json:"reaper_batch_size,omitempty"`

	// ActivityTypes restricts this engine to only dequeue specific activity types.
	// When nil, workers dequeue all activity types.
	ActivityTypes []string `json:"activity_types,omitempty"`

	// MaxActivityDepth caps how deep the parent/child activity tree can grow.
	// A handler attempting to spawn a child beyond this depth will receive ErrDepthExceeded.
	// When zero, defaults to 32.
	MaxActivityDepth uint16 `json:"max_activity_depth,omitempty"`

	// SuspendOnAwait, when true, switches the engine to a semaphore-based
	// concurrency model where ActivityFuture.GetResult releases this
	// activity's worker slot while it waits for the child future to resolve.
	//
	// Deprecated: in-handler GetResult now yield-parks the parent after a
	// short in-process grace, which frees the whole worker (goroutine and
	// all), not just a slot — the parent-blocking-on-children starvation
	// pattern this mode existed for self-resolves within the grace window in
	// the default fixed-pool model. SuspendOnAwait remains functional but
	// buys little; it and SuspendLeafActivityTypes/SuspendLeavesReserved may
	// be removed in a future release.
	SuspendOnAwait bool `json:"suspend_on_await,omitempty"`

	// SuspendLeafActivityTypes, when non-empty together with SuspendOnAwait,
	// reserves SuspendLeavesReserved slots for these "leaf" types — i.e. when
	// the free-slot count drops to the reservation, the dispatcher only
	// dequeues leaf types. Prevents the wake-up deadlock where every freed
	// slot is immediately taken by another awakening parent and no leaf can
	// run. Mirrors the parent/leaf split from the WORKER_MODE example
	// pattern, but in-process.
	SuspendLeafActivityTypes []string `json:"suspend_leaf_activity_types,omitempty"`

	// SuspendLeavesReserved is the number of slots set aside for leaf
	// activity types when at-pressure. Only meaningful with SuspendOnAwait
	// and SuspendLeafActivityTypes set. Default 0 (no reservation).
	SuspendLeavesReserved int `json:"suspend_leaves_reserved,omitempty"`

	// Retention enables deletion of old terminal workflow trees. Nil (the
	// default) keeps everything forever.
	Retention *RetentionConfig `json:"retention,omitempty"`

	// ShutdownGraceSeconds bounds the entire shutdown drain — worker loops,
	// dispatchers, in-flight activity goroutines, result-storage goroutines,
	// and worker-pool deregistration all run in parallel under this single
	// budget. When the budget expires, Start() returns even if some
	// goroutines are still in flight (those are then orphaned for the
	// remaining process lifetime, which is fine on a SIGTERM). Default 30s.
	ShutdownGraceSeconds *uint64 `json:"shutdown_grace_seconds,omitempty"`
}

// DefaultMaxActivityDepth is the default cap when MaxActivityDepth is unset.
const DefaultMaxActivityDepth uint16 = 32

// DefaultWorkerConfig returns a WorkerConfig with sensible defaults.
func DefaultWorkerConfig() WorkerConfig {
	leaseMS := uint64(60_000)
	reaperInterval := uint64(5)
	reaperBatch := 100
	return WorkerConfig{
		QueueName:               "default",
		MaxConcurrentActivities: 10,
		LeaseMS:                 &leaseMS,
		ReaperIntervalSeconds:   &reaperInterval,
		ReaperBatchSize:         &reaperBatch,
	}
}
