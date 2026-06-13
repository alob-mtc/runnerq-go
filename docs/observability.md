# Observability

## The web console

RunnerQ ships an embedded web console — live queue stats, activity browsing,
result inspection, and a lifecycle event timeline — with no build step (the
HTML/JS is embedded; no npm).

```go
import (
    "net/http"
    "github.com/alob-mtc/runnerq-go/observability"
    "github.com/alob-mtc/runnerq-go/observability/ui"
)

inspector := observability.NewQueueInspector(backend).
    WithMaxWorkers(engine.MaxConcurrentActivities())

mux := http.NewServeMux()
mux.Handle("/console/", http.StripPrefix("/console", ui.RunnerQUI(inspector)))
http.ListenAndServe(":8081", mux)   // → http://localhost:8081/console/
```

Mount it on your existing mux alongside your own routes. For a custom UI,
serve just the JSON API:

```go
mux.Handle("/api/observability/", http.StripPrefix("/api/observability", ui.ObservabilityAPI(inspector)))
```

> **Security:** the console and API are unauthenticated and the SSE stream
> sends `Access-Control-Allow-Origin: *`. Mount them behind your own auth
> middleware and don't expose them publicly.

### SSE fan-out

Live updates use Server-Sent Events. All subscribers share a **single** backend
`EventStream` connection via a fan-out hub inside `QueueInspector`: the first
subscriber lazily starts one Postgres `LISTEN`, additional subscribers get
buffered channels fed by non-blocking broadcast (a slow client drops its own
events, never blocking others), and the connection is released when the last
subscriber leaves. The HTTP layer caps concurrent SSE connections (default 64),
returning `503` beyond that. So 100 dashboard tabs cost one database
connection, not 100. Custom backends get this for free — the hub works on any
`InspectionStorage.EventStream()` channel.

## Queue stats from code

```go
inspector := observability.NewQueueInspector(backend).
    WithMaxWorkers(engine.MaxConcurrentActivities())

stats, _ := inspector.Stats(ctx)
fmt.Printf("pending=%d processing=%d scheduled=%d waiting=%d dead_letter=%d\n",
    stats.PendingActivities, stats.ProcessingActivities, stats.ScheduledActivities,
    stats.WaitingActivities, stats.DeadLetterActivities)
```

The inspector also lists activities by status, fetches a single activity and
its event timeline, walks a workflow's children/subtree, and streams events —
see `observability.QueueInspector`.

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
| `activity_lost_completion` | finished but the lease had moved on (work will rerun) |
| `activity_yielded` | parked for a durable wait (sleep/signal/await) |
| `activity_trees_swept` | workflow trees deleted by retention |

> Duration/gauge instrumentation (`ObserveDuration`, queue-depth gauges,
> per-type labels) is not yet wired — the metrics surface is counters only for
> now. A richer Prometheus-shaped surface is on the roadmap.
