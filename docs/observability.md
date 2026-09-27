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
    Labels:       map[string]string{"region": "eu-west-1"},
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
  in-flight activities pushed as periodic reports.
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
| `activity_completed` | completed successfully |
| `activity_retry` | requested a retry |
| `activity_failed_non_retry` | failed permanently |
| `activity_timeout` | exceeded its timeout |
| `activity_claim_lost` | the execution lost its claim — cancelled mid-handler by the heartbeat, or its ack was rejected by the fence (the replacement execution owns the outcome) |
| `activity_heartbeat_failed` | a claim renewal failed and will be retried on the next beat |
| `activity_yielded` | parked for a durable wait (sleep/signal/await) |
| `activity_trees_swept` | workflow trees deleted by retention |

> Duration/gauge instrumentation (`ObserveDuration`, queue-depth gauges,
> per-type labels) is not yet wired — the metrics surface is counters only for
> now. A richer Prometheus-shaped surface is on the roadmap.
