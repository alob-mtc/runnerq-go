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
-- NOTE: the two hot dequeue indexes (idx_runnerq_dequeue_effective_v2,
-- idx_runnerq_dequeue_order_v2) are NOT created here. They are versioned
-- (_v2: their partial predicates gained the 'waiting' status) and migrated
-- by ensureDequeueIndexes in Go using CREATE INDEX CONCURRENTLY, because a
-- plain CREATE INDEX takes a SHARE lock that blocks all writes on
-- runnerq_activities for the duration of the build — at boot, on a hot
-- queue, that stalls every enqueue/dequeue/ack in the cluster. The v1 names
-- are dropped only after the v2 indexes are built and valid, so there is
-- never a window with no dequeue index. See ensureDequeueIndexes.
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

-- Results table. Rows live until the retention sweeper deletes their
-- workflow tree (or forever when retention is not configured).
CREATE TABLE IF NOT EXISTS runnerq_results (
    activity_id UUID PRIMARY KEY,
    queue_name TEXT NOT NULL,
    state TEXT NOT NULL,
    data JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- owner_activity_id ties every result row to the activity whose lifetime
-- governs it. For normal activity results it equals activity_id; for Run/
-- Sleep checkpoints (whose activity_id is a synthetic UUIDv5 matching no
-- activity row) it is the handler's activity — without it, checkpoint rows
-- could never be garbage-collected. NULL on legacy rows (never swept).
ALTER TABLE runnerq_results
    ADD COLUMN IF NOT EXISTS owner_activity_id UUID;
CREATE INDEX IF NOT EXISTS idx_runnerq_results_owner
    ON runnerq_results(queue_name, owner_activity_id)
    WHERE owner_activity_id IS NOT NULL;

-- step is the human identity of a checkpoint row, "kind:name" (e.g.
-- "run:create-transfer", "sleep:retry-backoff") — the same string the checkpoint
-- ID is derived from, persisted so the console can show a workflow's durable
-- step history (the checkpoint ID itself is a one-way hash). NULL for an
-- activity's own final result and for legacy rows. No dedicated index: read only
-- by owner (already indexed above) on the console path, never on a hot path.
ALTER TABLE runnerq_results
    ADD COLUMN IF NOT EXISTS step TEXT;

-- Retention sweep: find roots that have been terminal longer than the TTL.
CREATE INDEX IF NOT EXISTS idx_runnerq_root_terminal_age
    ON runnerq_activities(queue_name, status, completed_at)
    WHERE parent_activity_id IS NULL
      AND status IN ('completed', 'failed', 'dead_letter');

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
