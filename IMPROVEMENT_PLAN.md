# RunnerQ Improvement Plan

Audit date: 2026-06-12. Full-codebase review (~5,100 lines core + storage + console) against the goal:
**production-grade durable execution engine at massive scale.**

## Verdict

RunnerQ today is a well-built Postgres task queue with parent/child futures — not yet a durable
execution engine. Solid foundations: correct `FOR UPDATE SKIP LOCKED` claims, fenced acks, SSE
console, good release hygiene. But the category-defining guarantee — a workflow survives process
death without redoing completed work — is absent, several correctness bugs can permanently wedge
workflows, throughput is capped well below "massive scale" by per-transition NOTIFY serialization
and polling loops, and the repo has effectively no tests (one 30-line unit test).

Reference comparison: Temporal, Inngest, Restate, DBOS, Azure Durable Functions.

---

## Tier 0 — Correctness bugs that wedge workflows (fix first; days)

### 0.1 Result storage is non-atomic with completion (critical)
- `MarkCompleted` calls `AckSuccess(ctx, id, nil, …)` with a **nil** result (queue.go:179); the
  real result is stored later by a separate async goroutine in a second transaction
  (engine.go:576-583).
- Crash or `StoreResult` failure between the two: activity is `completed` forever, result row never
  exists, parent's `GetResult` polls until its own timeout → parent retried → re-spawns subtree.
  This is a direct input to the DLQ death spiral documented in PERF_FIX_PLAN.md.
- The backend already supports atomic result-with-ack (postgres.go:400-405) — the engine just
  doesn't use it. Fix: pass the result through `AckSuccess`; delete the async success-path
  `StoreResult`. Also remove the double-store on the non-retryable failure path (in-tx at
  postgres.go:471-474 and again async at engine.go:620-632).

### 0.2 Worker identity is not unique — stale acks land (critical)
- Acks fence on `current_worker_id = $2` (postgres.go:388-390, 441) but IDs are `"worker-%d"`
  (engine.go:365) and the constants `"dispatcher"` / `"dispatcher-leaf"` (engine.go:232, 243) —
  identical across every engine process and attempt.
- Sequence: engine A claims as "dispatcher" → lease expires → reaper requeues → engine B claims,
  also as "dispatcher" → A's slow handler finishes → A's `AckSuccess` matches B's claim and marks
  it completed while B is still running.
- Fix: make worker IDs unique per process (pool UUID exists at engine.go:189, unused for claims) —
  ideally a per-attempt fencing token returned by `Dequeue`.

### 0.3 Idempotency claim and enqueue are non-atomic (critical)
- `EvaluateIdempotencyRule` claims the key in one autocommit op (executor.go:286;
  postgres.go:755-759), then `Enqueue` runs in its own transaction (executor.go:312).
- Crash between them permanently bricks the key: `ReturnExisting` forever returns a future for an
  activity that doesn't exist (LEFT JOIN status NULL, postgres.go:773-778, unchecked at
  postgres.go:799-801); `AllowReuseOnFailure` errors forever (postgres.go:814-837).
- Related race: `BehaviorAllowReuse` does an unguarded read-then-write update
  (postgres.go:803-812).
- Fix: claim + enqueue in one transaction, or detect/repair orphaned keys in
  `resolveIdempotencyBehavior`.

### 0.4 Reaper never increments retry_count — poison pills loop forever (high)
- `RequeueExpired` flips expired rows straight back to `pending` (postgres.go:598-611): no
  `retry_count` bump, no `max_retries` check, no event recorded.
- An activity whose handler crashes the process (OOM, segfault) is re-claimed and re-crashes
  workers forever, never reaches the DLQ, and is invisible in the console timeline.
- Fix: increment `retry_count` on requeue, route to dead-letter when exhausted, record a
  `Requeued` event.

### 0.5 Leases use application clocks (high)
- Lease deadline = caller's `time.Now()` (postgres.go:289-290, 313); reaper compares against its
  own `time.Now()` (postgres.go:590, 609); `scheduled_at <= $6` likewise (postgres.go:319).
- One fast-clocked node in a cluster prematurely reclaims every other node's leases → systematic
  double execution. Fix: compute deadlines and comparisons server-side with the Postgres clock.

### 0.6 Graceful shutdown can't complete in-flight work; Start/Stop races (high/medium)
- `stop()` cancels `engineCtx` immediately (engine.go:269-270, 353-355); in-flight handlers that
  finish during the drain ack with a cancelled context → `activity_lost_completion` → guaranteed
  rerun (engine.go:562-570). The drain (engine.go:286-338) waits for goroutines whose acks are
  already doomed.
- Fix: two-phase shutdown — stop dequeueing, let in-flight handlers finish and ack on a live
  context, then cancel.
- Also: `cancelFunc`/`shutdownCh` written in `Start` (engine.go:183-184) and read/closed in
  `stop()` (engine.go:353-360) with no synchronization; the `select`/`default`/`close` idiom can
  double-close → panic on concurrent `Stop()`.

### Other Tier-0-adjacent correctness issues
- Engines without an `ActivityTypes` filter mark unregistered types failed **non-retryably**
  (`handler_not_found`, engine.go:489-493) — destroying work a sibling engine could process.
- Handler success exactly at the deadline is discarded and retried (engine.go:517-523); timeout is
  only checked after the handler returns, so a ctx-ignoring handler blocks a slot forever.
- `canRetry` off-by-one: `(retry_count+1) < max_retries` (postgres.go:487-509) — `MaxRetries=3`
  yields only 2 retries.
- `RegisterActivity` after `Start` races the unsynchronized handlers map (engine.go:108-110).
- Library installs process-wide SIGINT/SIGTERM handlers inside `Start` (engine.go:257-258).

---

## Tier 1 — Earn the "durable execution" label (headline feature; weeks)

### Current reality
- No event-sourced history, no replay, no checkpointing. A handler is one function invocation per
  attempt (engine.go:481-540); `runnerq_events` is observability-only.
- Parent crash/timeout → lease expires → reaper requeues → handler **restarts from line 1**,
  minting fresh child UUIDs (activity.go:159-161) → **already-completed children re-execute as
  duplicates** unless the user hand-rolled idempotency keys (no example does this correctly;
  examples/basic/main.go:128 uses `uuid.New()` as the key, defeating dedup entirely).
- `SuspendOnAwait` persists **nothing** — it only releases the in-process semaphore slot
  (suspend.go:32-68). A "workflow" cannot outlive one process's single handler invocation bounded
  by `timeout_seconds`. The defining durable-execution capability (months-long workflows surviving
  deploys) is absent. PERF_FIX_PLAN.md Tier 5 acknowledges this as unbuilt.

### Recommended direction: Inngest/DBOS-style step memoization
Not Temporal-style full event-sourced replay — that replaces the current core (a rewrite, months+).
Step memoization delivers ~90% of user-visible durability semantics on top of building blocks that
already exist (idempotency table, permanent results table, lineage columns):

1. ✅ **DONE — memoized step spawns.** `ActivityBuilder.Step(name)` derives the idempotency key
   `(root_activity_id, parent_activity_id, name)` with `ReturnExisting`: a retried parent
   re-issues identical spawns, each reattaches to the existing child, and `GetResult` returns
   the memoized result instantly — the parent fast-forwards through completed steps.
   Determinism contract is the mild Inngest-style one (stable step names across retries, unique
   per parent), not full replay determinism. (Step is opt-in per spawn; making lineage-keyed
   dedup the default for ALL spawns remains open.)
2. ✅ **DONE — `ctx.Run(name, fn)`.** Checkpoints local side effects into the results table
   under a UUIDv5 of (activity ID, step name): success and permanent failure are both stored
   and replayed; retryable failures re-run. At-least-once with at-most-once-per-recorded-success
   (a crash between fn and the checkpoint commit re-runs fn — documented).
3. ✅ **DONE — `ctx.Sleep(name, d)`.** Durable timer: the wake deadline persists as a checkpoint,
   so a crashed/redeployed handler resumes with only the remainder. Sleeps that fit the handler's
   timeout budget wait in-process (releasing the suspend slot); longer sleeps YIELD — a new
   fenced `Yield` storage op parks the row as scheduled until the wake time WITHOUT consuming
   retry_count, and the handler resumes after the wake (records an `EventYielded`).
4. ✅ **DONE — signals.** `ctx.WaitForSignal(name, timeout)` blocks (in-process when it fits the
   handler budget, otherwise yield-parked — no worker held, no retry consumed) until
   `WorkerEngine.Signal` / `runnerq.SignalActivity` delivers a payload from any process sharing
   the database. Signals are buffered (delivery before the wait — or before the activity runs —
   resolves instantly, including on replay), wait deadlines persist across parks, timeouts
   surface as a typed non-retryable error (`IsSignalTimeout`), and signal rows are
   retention-collected with their tree. Caveat (documented): delivery wakes ANY scheduled row
   early, including Delay-scheduled ones. Per-name semantics are last-write-wins (DBOS
   setEvent-style), not a message queue.
5. ✅ **DONE — parent re-dispatch.** In-handler `GetResult` waits in-process for a 2s grace,
   then yield-parks the parent as a dedicated `waiting` status row — no goroutine, no lease, no
   retry consumed; the child's terminal ack (success, failure, dead-letter — including reaper
   dead-letters) atomically wakes the parked parent, which replays and fast-forwards. Workflows
   now outlive handler invocations and deploys. Also landed with this: the `waiting` status
   fixes the signals early-wake caveat (Delay-scheduled rows are no longer woken by delivery);
   a post-park recheck closes the park race for ALL durable waits (a result committing between
   the handler's final check and the park landing previously produced no wake — a latent
   forever-park bug for unbounded signal waits); `SuspendOnAwait` has been removed entirely (the starvation
   pattern it existed for self-resolves within the await grace; the dispatcher, slotHolder
   machinery, config fields, and stress example are deleted); `FutureFor`/`ActivityID`/
   `WaitAll` ship future rehydration. Item "auto-key every spawn" is closed as won't-do:
   sequence-derived keys are fail-wrong under non-deterministic replay and payload-hash keys
   silently degrade — explicit `Step` names are the model (same conclusion as Inngest).

### Also required for the category
- **Cancellation API** — none exists at any layer (storage, inspector, engine). Add cancel with
  best-effort propagation to descendants via `root_activity_id`.
- **`ctx.Heartbeat()` — deliberately deferred.** Dequeue already sets the lease to
  `max(default, timeout+10s)`, so the lease never expires before the activity timeout and a
  heartbeat that only extends the lease buys nothing today. It becomes meaningful only with a
  rethought long-activity timeout model (heartbeat-refreshed deadlines instead of one fixed
  timeout); do it then, with a worker-fence guard added to ExtendLease's WHERE clause.
- **Engine-native cron/schedules** — the console Schedules tab only lists rows the user tagged
  `metadata.source='cron'` (postgres.go:1255-1274); nothing in core creates them.
- ✅ **DONE — future rehydration**: `FutureFor(backend, id)`, `fut.ActivityID()`, and `WaitAll`.
- ✅ **DONE — workflow versioning guidance**: README "Versioning workflows across deploys" —
  name-based step keys make reorder/add safe; renaming live step names is the one hazard.

---

## Tier 2 — Scale ceilings (blocks 10k/sec regardless of Tier 1)

1. ✅ **DONE — `pg_notify` + event INSERT inside every lifecycle transaction.** Postgres
   serializes NOTIFY-ing commits on a global queue lock — a hard cluster-wide commit-rate cap,
   paid even with zero listeners. Fixed: transactions now only insert rows; a per-backend
   signaler batches post-commit signals (~50ms windows) into tiny notifications on three
   channels — work edge, event edge, and batched result activity-IDs (storage/postgres/
   signals.go). The console event stream is fed by a cursor tailer over `runnerq_events`
   (full rows — also removes the 8KB NOTIFY truncation), with one shared LISTEN connection
   per process instead of one per SSE subscriber.
2. ✅ **DONE — Dequeue ORDER BY cannot use the index.** Fixed: new
   `idx_runnerq_dequeue_order` keyed exactly like the ORDER BY; the query is split into three
   static forms (untyped / single-type / multi-type) replacing the
   `($7 IS NULL OR …)` OR-pattern, and the eligibility predicate is restructured as
   `status IN (...) AND (status='pending' OR scheduled_at <= NOW())` so the planner can prove
   the partial-index predicate for ORDERED scans (verified: 0.2ms index walk vs external merge
   sort on a 200k-row backlog). Residual (acceptable for now): future-scheduled rows still sit
   in the same index and are filtered in-scan.
3. ✅ **DONE — Polling everywhere despite NOTIFY firing on every transition.** Fixed:
   `Dequeue` is now genuinely blocking (parks on the work signal, re-probes every 2s as
   fallback, honors the timeout param); engine loops dropped their idle backoff. Futures use
   `WaitForResult` — notification-driven with a 5s table re-check fallback — exposed as the
   optional `storage.ResultWaiter` capability and **working across processes** (the awaiting
   server process and the producing worker only share the database; verified by integration
   tests with two separate backend instances: ~100-200ms wakes). Custom backends without the
   capability fall back to the legacy 100ms poll.
4. **No batching.** Enqueue is one transaction per child (postgres.go:218-286 — a fanout-100
   handler does 100 sequential round-trips); dequeue claims one row per round-trip → O(workers²)
   skip-locked scanning; no batch ack. ~20 statements / 4 transactions per successful activity
   today. Add batch claim, batch enqueue, batch ack.
5. ◐ **PARTIALLY DONE — unbounded table growth.** Retention sweeper shipped: opt-in
   `RetentionConfig{Completed, Failed TTLs}` on the engine; the backend's `CleanupExpired`
   deletes whole terminal workflow trees (activities + events + results — including Run/Sleep
   checkpoint rows via the new `owner_activity_id` column — + idempotency keys) in batched
   transactions, with per-queue advisory-lock leadership so only one engine sweeps. Tree-level
   deletion (terminal root, no non-terminal descendants) guarantees retries never find their
   children's results missing. Caveat: result rows written before the owner column existed have
   a NULL owner_activity_id and are never swept — pre-upgrade checkpoint rows must be cleaned
   manually if reclaiming that space matters. Still open: time-based partitioning for very high
   volumes, and
   fillfactor/autovacuum tuning for UPDATE-churn bloat (status flips across 5 partial indexes
   are never HOT).
6. **Suspend-mode dispatcher is single-threaded** (engine.go:421-457): serial
   acquire→dequeue→spawn caps the engine at roughly 200-500 dispatches/sec regardless of
   `MaxConcurrentActivities`.
7. **Every engine runs its own reaper** every 5s against the same rows (engine.go:703-729) — no
   leader election; duplicated cluster work.
8. **Connection pool sizing is uncoordinated.** Default 25 conns/process (postgres.go:46-52),
   never validated against worker count or suspend-mode poll load; the PERF_FIX_PLAN "too many
   clients" failure is structural. Each SSE consumer also pins a pool connection for its lifetime
   (postgres.go:1297-1312). Validate pool vs workers at Build; document PgBouncer.

---

## Tier 3 — Production operability

### Metrics (high)
- Total emitted surface today: **5 counters** (engine.go:569-636); `ObserveDuration` is declared
  (metrics.go:9) and never called anywhere; no labels; no shipped sink (no Prometheus/OTel dep).
- Add: handler duration, queue-wait latency, dequeue/enqueue latency + counters, in-flight and
  suspended-waiter gauges, backlog/DLQ depth, reaper requeue count, dead-letter counter, pool
  saturation — all labeled by activity type. Ship a Prometheus implementation.

### Console security (high)
- Console + API are completely unauthenticated (routes.go:19-52) and the SSE stream sets
  `Access-Control-Allow-Origin: *` (routes.go:339) — any website in an operator's browser can read
  the live event stream of an intranet console, including full payloads. Add an auth seam
  (middleware hook + token) before stabilizing the HTTP API.

### Console functionality & query cost
- No mutation endpoints: operators can't retry/cancel/requeue DLQ items from the console.
- `ListRecentRoots`/`ListRecentActivities` sort on a computed CASE/COALESCE expression no index can
  serve (postgres.go:1190-1253) — top-N sort over the whole queue on every SSE-triggered refetch.
- `ListCronActivities` (postgres.go:1255-1274) matches no index — full scan.
- `Stats()` runs 14 scalar COUNT(*)s, several over all historical rows (postgres.go:851-899), with
  a 1s cache and no singleflight.
- `GetSubtree` is unbounded (postgres.go:1276-1294); search is client-side over the current page
  only; pagination is OFFSET-based with no totals.

### Misc
- No OpenTelemetry trace propagation across the enqueue→dequeue boundary.
- No injectable logger — everything uses the global slog default.
- `GetActivityEvents`/`GetResult` don't filter by queue_name (postgres.go:709, 1122-1128).

---

## Tier 4 — Engineering hygiene (cheap; prerequisite for everything above)

- **One 30-line test in the entire repo** (storage/postgres/postgres_test.go — a status-string
  mapping test). Zero coverage of dequeue SQL, SKIP LOCKED concurrency, lease/reaper recovery,
  idempotency races, retry math, ack fencing, shutdown. Add a testcontainers-based Postgres
  integration suite with crash-recovery invariant tests; every Tier 0 fix lands with a regression
  test.
- CI runs `go test` without `-race` despite heavy mutex/channel code; add `-race`.
- No golangci-lint config; CI is build + vet + gofmt only.
- No benchmarks; the PERF_FIX_PLAN meltdown was discovered by hand. Promote
  examples/perf/suspend_stress into a repeatable benchmark harness.

---

## Suggested order

1. **Tier 4 harness + Tier 0 fixes together** — each Tier 0 item is small and needs a regression
   test; the harness pays for itself immediately.
2. **Tier 2 items 1-3** (NOTIFY cost, dequeue index, event-driven wakeups) — they make the current
   model viable at scale and are prerequisites for Tier 1's re-dispatch design.
3. **Tier 1 step memoization** — the headline feature that earns "durable execution".
4. **Tier 3** alongside, with console auth before any API stabilization.
