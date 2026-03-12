package postgres

const schemaSql = `
-- Activities table (permanent - no TTL)
CREATE TABLE IF NOT EXISTS runnerq_activities (
    id UUID PRIMARY KEY,
    queue_name TEXT NOT NULL,
    activity_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    priority INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    scheduled_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    lease_deadline_ms BIGINT,
    current_worker_id TEXT,
    last_worker_id TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    timeout_seconds BIGINT NOT NULL DEFAULT 300,
    retry_delay_seconds BIGINT NOT NULL DEFAULT 60,
    last_error TEXT,
    last_error_at TIMESTAMPTZ,
    metadata JSONB,
    idempotency_key TEXT
);

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_runnerq_dequeue_effective
    ON runnerq_activities (
        queue_name,
        activity_type,
        priority DESC,
        retry_count DESC,
        COALESCE(scheduled_at, created_at) ASC
    )
    WHERE status IN ('pending', 'scheduled', 'retrying');
CREATE INDEX IF NOT EXISTS idx_runnerq_activities_processing
    ON runnerq_activities(queue_name, lease_deadline_ms)
    WHERE status = 'processing';

-- Idempotency keys table
CREATE TABLE IF NOT EXISTS runnerq_idempotency (
    queue_name TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    activity_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (queue_name, idempotency_key)
);

-- Events table (permanent - full history)
CREATE TABLE IF NOT EXISTS runnerq_events (
    id BIGSERIAL PRIMARY KEY,
    activity_id UUID NOT NULL,
    queue_name TEXT NOT NULL,
    event_type TEXT NOT NULL,
    worker_id TEXT,
    detail JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_runnerq_events_activity
    ON runnerq_events(activity_id, created_at DESC);

-- Results table (permanent)
CREATE TABLE IF NOT EXISTS runnerq_results (
    activity_id UUID PRIMARY KEY,
    queue_name TEXT NOT NULL,
    state TEXT NOT NULL,
    data JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`
