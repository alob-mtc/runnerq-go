# Throughput & Observability Fix Plan

Working notes for backlog-drain investigation on the lineage_tree example
running across 11 processes (1 spawner + 10 worker-only), each with
`MaxWorkers(50)` and a Postgres backend.

Status: **draft for review — nothing applied yet.**

## 1. Observed symptoms

- 11 processes × `MaxWorkers(50)` configured. Console dashboard reports
  `WORKERS 0 / 50` (denominator wrong; should be 550).
- Backlog grew unboundedly under load: `RUNNING 499 / PENDING 3101 /
  DEAD_LETTER 1365` mid-run.
- Logs full of `activity timed out worker_id=N activity_type=... timeout=1m0s`,
  almost exclusively on the parent activity types (`research_workflow`,
  `analyze_data`, `draft_report`).
- Once the spawner stopped firing new roots, the backlog drained to zero. DLQ
  climbed to `1551` during drain.
- Earlier in the session: Postgres returned `FATAL: sorry, too many clients
  already (SQLSTATE 53300)` when bringing all 11 processes up.

## 2. Root cause: parent-blocking-on-children starvation

`examples/observability/lineage_tree/main.go` defines a recursive fan-out:

```
research_workflow (d=0)        ─ awaits 90s on 3 children
  fetch_documents (d=1, leaf)
  analyze_data    (d=1)        ─ awaits 60s on 2 children
    extract_facts  (d=2, leaf)
    verify_sources (d=2, leaf)
  draft_report    (d=1)        ─ awaits 60s on 2 children
    outline    (d=2, leaf)
    write_intro (d=2, leaf)
```

When a parent calls `fut.GetResult(awaitCtx)` (lineage_tree/main.go:88, 91, 94,
140, 141, 203, 204), the parent's goroutine **holds its worker slot doing
nothing** until its children resolve. With 50 workers per process and deep
fan-out:

- One root tree pins ≥ 3 worker slots concurrently (root + 2 mid-level
  parents) before any leaf can run.
- Roughly 16 concurrent roots saturate all 50 workers in the awaiting state.
- Leaves can't be dequeued because every worker is parked on `GetResult`.
- The parent's `awaitCtx` (60–90s) fires → parent times out → retry → re-spawn
  children → state degrades further.

The 1m0s timeouts in the logs are almost all parent `awaitCtx` deadlines, not
activity-level timeouts. The cluster recovered as soon as new spawning
stopped because in-flight parents could finally receive their children's
results.

The lease/heartbeat story is **not** the bottleneck: dequeue SQL already sets
`lease_deadline_ms = now + max(defaultLeaseMS, (timeout_seconds + 10) * 1000)`
(postgres.go:293), so leases auto-extend to cover any per-activity timeout.

## 3. Connection pool sizing (real, separate issue)

`postgres.New()` defaults `poolSize=10` (postgres.go:31). Across 11 processes
that's 110 backend connections, exceeding the typical PG `max_connections=100`
default — explaining the earlier 53300 error. Stats(), reaper, scheduled
processor, and worker dequeues all share this pool, so per-process pressure is
also higher than just `MaxWorkers` would suggest.

## 4. Workers KPI is per-process, not cluster-wide

Trace of `WORKERS 0 / 50`:

- Numerator (`active_workers`) is cluster-wide: `COUNT(DISTINCT
  current_worker_id) WHERE status='processing'` (postgres.go:831). Correct.
- Denominator (`max_workers`) comes from
  `QueueInspector.WithMaxWorkers(engine.MaxConcurrentActivities())`
  (inspector.go:28-32, 326-329). It's a literal of whichever process happens to
  run the observability inspector. The backend's `QueueStats.MaxWorkers` field
  exists (storage.go:118) but **postgres `Stats()` never populates it**.

Result: in any multi-process deployment the denominator is meaningless. With
11 × 50 = 550 configured slots, the dashboard reports 50.

Fix requires per-instance registration: each engine announces its capacity to a
shared table, heartbeats periodically, and `Stats()` sums across live rows.

## 5. Fix plan

Tiered so each tier can land independently and be tested in isolation.

### Tier 1 — Trivial corrections (low risk, apply first)

**T1a. Default Postgres pool size 10 → 25.**

`storage/postgres/postgres.go:31`

```go
func New(ctx context.Context, databaseURL, queueName string) (*PostgresBackend, error) {
    return WithConfig(ctx, databaseURL, queueName, 30_000, 25)
}
```

Eliminates 53300 errors on multi-process bring-up at the cost of ~165 extra
backend slots in the worst-case 11-process deployment. Operators using
`WithConfig` aren't affected. Safer pattern would be to derive from a
`MaxWorkers` hint, but that requires API change; defer.

**T1b. Cache `Stats()` results behind a 1s TTL.**

`storage/postgres/postgres.go:817-888` runs 14 COUNT subqueries per call. SSE
ticks every 5s and amplifies per connected client. A 1s in-process cache cuts
redundant load without a noticeable freshness regression in the UI.

```go
type PostgresBackend struct {
    pool           *pgxpool.Pool
    queueName      string
    defaultLeaseMS int64

    statsMu     sync.Mutex
    statsCache  *storage.QueueStats
    statsExpiry time.Time
}

const statsCacheTTL = time.Second

func (b *PostgresBackend) Stats(ctx context.Context) (*storage.QueueStats, error) {
    b.statsMu.Lock()
    if b.statsCache != nil && time.Now().Before(b.statsExpiry) {
        s := *b.statsCache
        b.statsMu.Unlock()
        return &s, nil
    }
    b.statsMu.Unlock()

    // ... existing query + scan ...

    b.statsMu.Lock()
    b.statsCache = stats
    b.statsExpiry = time.Now().Add(statsCacheTTL)
    b.statsMu.Unlock()
    return stats, nil
}
```

### Tier 2 — Split parent vs leaf workers in the demo (config-only)

The engine already supports per-instance type filtering via
`Builder.ActivityTypes(...)`. The demo doesn't use it, so every process
competes for every activity type. Drive role from an env var so the same
binary can be deployed in two modes.

`examples/observability/lineage_tree/main.go`

```go
var (
    parentTypes = []string{"research_workflow", "analyze_data", "draft_report"}
    leafTypes   = []string{
        "fetch_documents", "extract_facts", "verify_sources",
        "outline", "write_intro", "audit_log",
    }
)

mode := os.Getenv("WORKER_MODE") // "spawner" | "leaves" | "" (all)
builder := runnerq.Builder().
    Backend(backend).
    QueueName("lineage_demo").
    MaxWorkers(50)

switch mode {
case "spawner":
    builder = builder.ActivityTypes(parentTypes)
case "leaves":
    builder = builder.ActivityTypes(leafTypes)
}

engine, err := builder.Build()
```

Deployment recipe:
- 1 process: `WORKER_MODE=spawner ./lineage_tree` — 50 slots, only ever holds
  awaiting parents.
- 10 processes: `WORKER_MODE=leaves ./lineage_tree` — 500 slots, dedicated to
  leaves.

Capacity ratio 1:10 matches the per-tree fan-out (~7 leaves per root).

### Tier 3 — Cluster-aware worker registration (fixes the `0 / 50` KPI)

Schema addition (idempotent):

```sql
CREATE TABLE IF NOT EXISTS runnerq_worker_pools (
    pool_id      UUID PRIMARY KEY,
    queue_name   TEXT NOT NULL,
    max_workers  INTEGER NOT NULL,
    activity_types TEXT[],            -- NULL = all types
    started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_runnerq_worker_pools_queue_alive
    ON runnerq_worker_pools(queue_name, last_seen_at);
```

Engine wiring:

- On `Start()`, generate `poolID := uuid.New()`, INSERT row with
  `max_workers = config.MaxConcurrentActivities` and `activity_types =
  config.ActivityTypes`.
- Heartbeat goroutine: every 10s, `UPDATE ... SET last_seen_at=NOW() WHERE
  pool_id=$1`.
- On graceful shutdown, `DELETE WHERE pool_id=$1` (best-effort).
- Reaper view: pools with `last_seen_at < NOW() - INTERVAL '60s'` are
  considered dead — exclude from aggregates.

Backend `Stats()` adds:

```go
var totalMax int
err := b.pool.QueryRow(ctx, `
    SELECT COALESCE(SUM(max_workers), 0)::int
    FROM runnerq_worker_pools
    WHERE queue_name = $1 AND last_seen_at > NOW() - INTERVAL '60 seconds'`,
    b.queueName).Scan(&totalMax)
if totalMax > 0 {
    stats.MaxWorkers = &totalMax
}
```

`QueueInspector.WithMaxWorkers()` becomes a fallback for backends that don't
implement registration. Existing `WithMaxWorkers` callers continue to work but
are now generally unnecessary.

Post-fix dashboard: `WORKERS 17 / 550` accurately reflects 11 live pools each
contributing 50 capacity.

### Tier 4 — Engine-level per-type concurrency caps (deferred)

Replace process-splitting with in-engine reservation:

```go
runnerq.Builder().
    MaxWorkers(50).
    MaxWorkersPerType(map[string]int{
        "research_workflow": 5,
        "analyze_data":      5,
        "draft_report":      5,
        // unspecified types share remaining 35 slots
    })
```

Implementation: per-type semaphores, per-type dequeue queries. Not in this PR.
Captured here so it isn't lost.

### Tier 5 — Continuation / parent suspension (architectural, separate design)

The Temporal-style fix: when a parent calls `GetResult` on an unresolved
future, persist a continuation, free the worker slot, and re-enter the parent
when its children complete (signal-driven). Eliminates the starvation pattern
entirely instead of working around it.

Significant change: requires a workflow-state representation, child-completion
signalling, and state-machine resumption. Worth a dedicated design doc. Not
in scope for this plan.

## 6. Recommended apply order

1. T1a + T1b (small, isolated; merges the Postgres pool/cache pain).
2. T3 (worker registration; required to validate downstream KPI changes).
3. T2 (demo config; lets us actually drain a load test without DLQ growth).
4. T4 (engine API change; design + iterate).
5. T5 (architecture; design doc first).

## 7. Validation

For each tier, before/after metrics on a fixed load (e.g. 100 root spawns over
60s, then idle):

| Metric | Before | T1 | T2 | T3 |
| --- | --- | --- | --- | --- |
| Backlog peak | 3101 | ~similar | ≪ | unchanged |
| DLQ delta | +186 | unchanged | ~0 | unchanged |
| 53300 errors | yes | none | none | none |
| `Stats()` p95 | high | low | low | low |
| `WORKERS` denom | 50 | 50 | 50 | 550 |

## 8. Out of scope

- Postgres `max_connections` tuning (operator concern).
- pg_notify storm reduction beyond Tier 1 caching.
- Adding PgBouncer / external pooler.
- Metric backend (Prometheus exposition, etc.).
