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
    max_retry_delay_seconds BIGINT NOT NULL DEFAULT 0,
    last_error TEXT,
    last_error_at TIMESTAMPTZ,
    metadata JSONB,
    idempotency_key TEXT
);

-- Indexes for efficient queries
--
-- idx_runnerq_dequeue_effective serves the single-type dequeue form: with
-- activity_type pinned by equality, the remaining key columns match the
-- dequeue ORDER BY exactly, so the claim is an index walk that stops at the
-- first unlocked row.
CREATE INDEX IF NOT EXISTS idx_runnerq_dequeue_effective
    ON runnerq_activities (
        queue_name,
        activity_type,
        priority DESC,
        retry_count DESC,
        COALESCE(scheduled_at, created_at) ASC
    )
    WHERE status IN ('pending', 'scheduled', 'retrying');
-- idx_runnerq_dequeue_order serves the untyped and multi-type dequeue forms.
-- Its key order IS the dequeue ORDER BY, so Postgres never has to sort the
-- whole eligible backlog to find the top row — without it, every claim is a
-- top-1 sort over every live row in the queue, i.e. O(backlog) per dequeue
-- per worker, which degrades superlinearly exactly when a backlog builds.
CREATE INDEX IF NOT EXISTS idx_runnerq_dequeue_order
    ON runnerq_activities (
        queue_name,
        priority DESC,
        retry_count DESC,
        COALESCE(scheduled_at, created_at) ASC
    )
    WHERE status IN ('pending', 'scheduled', 'retrying');
CREATE INDEX IF NOT EXISTS idx_runnerq_activities_processing
    ON runnerq_activities(queue_name, lease_deadline_ms)
    WHERE status = 'processing';
CREATE INDEX IF NOT EXISTS idx_runnerq_completed_non_cron
    ON runnerq_activities(queue_name, completed_at DESC, created_at DESC)
    WHERE status IN ('completed', 'failed')
      AND (metadata->>'source') IS DISTINCT FROM 'cron';
CREATE INDEX IF NOT EXISTS idx_runnerq_completed_cron
    ON runnerq_activities(queue_name, completed_at DESC, created_at DESC)
    WHERE status IN ('completed', 'failed')
      AND metadata->>'source' = 'cron';
CREATE INDEX IF NOT EXISTS idx_runnerq_dead_letter
    ON runnerq_activities(queue_name, completed_at DESC)
    WHERE status = 'dead_letter';

-- Migration: add column for existing deployments (idempotent, metadata-only on PG 11+).
ALTER TABLE runnerq_activities
    ADD COLUMN IF NOT EXISTS max_retry_delay_seconds BIGINT NOT NULL DEFAULT 0;

-- Lineage columns for parent/child activity tracking.
ALTER TABLE runnerq_activities
    ADD COLUMN IF NOT EXISTS parent_activity_id UUID,
    ADD COLUMN IF NOT EXISTS root_activity_id UUID,
    ADD COLUMN IF NOT EXISTS depth SMALLINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_runnerq_parent_id
    ON runnerq_activities(parent_activity_id)
    WHERE parent_activity_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_runnerq_root_id
    ON runnerq_activities(root_activity_id)
    WHERE root_activity_id IS NOT NULL;
-- Partial index for the workflows-list view (parent IS NULL = roots only),
-- ordered for the typical "newest first" query.
CREATE INDEX IF NOT EXISTS idx_runnerq_root_only
    ON runnerq_activities(queue_name, created_at DESC)
    WHERE parent_activity_id IS NULL;
CREATE INDEX IF NOT EXISTS idx_runnerq_root_status
    ON runnerq_activities(queue_name, status)
    WHERE parent_activity_id IS NULL;

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
-- Cursor tailing for the live event stream (EventStream reads id > cursor).
CREATE INDEX IF NOT EXISTS idx_runnerq_events_queue_seq
    ON runnerq_events(queue_name, id);

-- Results table (permanent)
CREATE TABLE IF NOT EXISTS runnerq_results (
    activity_id UUID PRIMARY KEY,
    queue_name TEXT NOT NULL,
    state TEXT NOT NULL,
    data JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Worker pools: one row per live engine instance. Heartbeats let us tell
-- which pools are still alive for cluster-wide capacity reporting.
CREATE TABLE IF NOT EXISTS runnerq_worker_pools (
    pool_id        UUID PRIMARY KEY,
    queue_name     TEXT NOT NULL,
    max_workers    INTEGER NOT NULL,
    activity_types TEXT[],
    started_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_runnerq_worker_pools_queue_alive
    ON runnerq_worker_pools(queue_name, last_seen_at);
`
