package runnerq

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
}

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
