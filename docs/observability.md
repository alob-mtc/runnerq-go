# Observability

RunnerQ's console is RunnerQ Cloud: the `conductor` agent connects your
workers to it, and it reads from your own database through them. From code,
query the backend directly, and forward counters to your metrics system.

## RunnerQ Cloud (Conductor agent)

The `conductor` package connects worker processes to RunnerQ Cloud. The agent
dials out over a WebSocket and answers the Cloud's queries from your own
database, so the Cloud never connects into your network or holds database
credentials.

```go
import "github.com/alob-mtc/runnerq-go/conductor"

agent, err := conductor.Start(ctx, engine, conductor.Config{
    URL:          "wss://cloud.runnerq.dev",
    APIKey:       os.Getenv("RUNNERQ_CONDUCTOR_KEY"),
    AllowControl: true, // let operators run commands; omit for read-only
})
if err != nil {
    log.Fatal(err)
}
defer agent.Close(context.Background()) // before the engine stops
engine.Start(ctx)
```

- **Workers only.** Start one agent per engine. Each engine appears in the
  Cloud as an executor, identified by `engine.InstanceID()`, with its live
  in-flight activities and counters pushed as reports: on the interval the
  Cloud sets, and within about a second of a change (an activity starting or
  finishing, or a drain beginning).
- **Labels.** Tag the worker with `WorkerConfig.Labels` (region, deploy
  version); the Cloud shows them whichever way the worker reports.
  `conductor.Config.Labels` adds to them and wins on a clash.
- **Queries, not views.** The Cloud reads through the backend's
  `storage.QueryStorage` (filters, keyset paging, counts, aggregates, events,
  steps, trees). Backends without it serve only live executor state.
- **Off the execution path.** `Start` returns immediately. If the Cloud is
  unreachable the agent retries with backoff (1s → 30s, jittered) and your
  activities are unaffected.
- **Clean shutdowns.** `Close` tells the Cloud the executor is stopping, so a
  deploy isn't reported as a crash.
- **Commands are opt-in.** With `AllowControl: true` (and a backend
  implementing `storage.CommandStorage`) operators can cancel, retry, run
  now, reschedule, reprioritise, delete and signal activities from the Cloud.
  Every command is idempotent and audited. Cancelling a running activity
  stops its handler (immediately when it runs on the executor that received
  the command, otherwise at the next claim heartbeat); anything awaiting it
  receives a cancellation error. Without `AllowControl` the agent is
  read-only and advertises no commands.
- **Live events.** The Cloud subscribes to one executor per app and streams
  its event log (`events.subscribe`), resuming from the last cursor on
  another executor if that one goes away. Events whose transaction commits
  late are caught by rescanning just below the cursor.
- **Metadata-only mode.** Set per app in the Cloud (applied live), or forced
  locally with `MetadataOnly: true`, which the Cloud cannot relax. Payloads,
  results, errors and event details are never sent.
- **Bounded load.** At most `MaxConcurrentRequests` (default 16) requests run
  at once, each limited by `RequestTimeout` (default 30s) or the Cloud's
  deadline, whichever is sooner.

## Reading activities from code

Backends that implement `storage.QueryStorage` (the built-in Postgres backend
does) answer the same queries the Cloud uses: filtered and paged activity
lists, counts, aggregates, events, steps and trees. For example, how many
activities are pending, running, waiting or dead-lettered right now:

```go
qs, ok := backend.(storage.QueryStorage)
if !ok {
    return errors.New("this backend doesn't support queries")
}
rows, err := qs.AggregateActivities(ctx, storage.AggregateQuery{
    Filter:  &storage.QueryFilter{Field: "status", Op: "in", Value: []any{"pending", "running", "waiting", "dead_letter"}},
    GroupBy: []string{"status"},
    Count:   true,
})
if err != nil {
    return err
}
for _, r := range rows.Rows {
    fmt.Println(r.Key["status"], r.Count)
}
```

## Metrics

Provide a `MetricsSink` to forward counters into Prometheus/StatsD/etc.:

```go
type MetricsSink interface {
    IncCounter(name string, value uint64)
    ObserveDuration(name string, dur time.Duration)
}

engine, _ := runnerq.Builder().Backend(backend).Metrics(&MyMetrics{}).Build()
```

The default is `runnerq.NoopMetrics`. Counters currently emitted:

| Counter | Meaning |
|---|---|
| `activity_started` | claimed, and its handler started here |
| `activity_completed` | completed successfully |
| `activity_retry` | requested a retry |
| `activity_failed_non_retry` | failed permanently |
| `activity_timeout` | exceeded its timeout |
| `activity_dead_lettered` | ran out of attempts (after a retry request or a timeout) |
| `activity_claim_lost` | the execution lost its claim — cancelled mid-handler by the heartbeat, or its ack was rejected by the fence (the replacement execution owns the outcome) |
| `activity_heartbeat_failed` | a claim renewal failed and will be retried on the next beat |
| `activity_yielded` | parked for a durable wait (sleep/signal/await) |
| `activity_trees_swept` | workflow trees deleted by retention |

Durations go to `ObserveDuration`: `activity_claim_lag` is how long an
activity waited, from when it was due, until a worker started it, and
`activity_completion_persistence` how long recording a completion took.

> Gauges (queue depth) and per-type labels are not wired yet. A richer
> Prometheus-shaped surface is on the roadmap.

## Executor snapshots

`engine.Snapshot()` is the engine as it is now: its identity (id, queue,
activity types, concurrency, host, SDK version, labels), what it's running,
whether it's draining, and counters since it was built (claimed, succeeded,
retried, failed, timed out, dead-lettered, claims lost, heartbeat failures,
last claim lag). The counters come from the same metrics the sink sees.

Everything that reports a worker reads it: the Cloud agent for its hello and
reports, and a storage backend that implements `executor.Observer`, which the
engine tells when it starts and stops (the RunnerQ Cloud storage adapter
reports hosted workers this way). Attach your own with `engine.Observe(o)`
before `Start`.

The engine is also an `executor.Notifier`: `engine.Changed()` returns a
channel closed at its next change (an activity starting or finishing, or a
drain beginning). `executor.Report` reports on an interval and soon after
changes, spaced by a minimum gap so a busy worker doesn't flood its
destination. The Cloud agent reports through it, and an `executor.Observer`
can too.
