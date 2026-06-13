# 11 — Retention

By default RunnerQ keeps every activity, event, result, and idempotency key
forever. Retention deletes whole terminal workflow trees once they age past a
TTL — in the background, safe to enable on every engine in a cluster.

## What this shows

A quick workflow runs to completion, then the program watches the retention
sweeper delete it once it's older than the (deliberately tiny) TTL. In
production these TTLs are days, not seconds.

## Run it

```bash
docker compose up -d        # from the examples/ directory, if not already up
go run .
```

## Expected output

```
workflow <id> completed; waiting for retention to sweep it...
✓ swept — the completed workflow tree was deleted by retention.
```

## The key idea

```go
engine, _ := runnerq.Builder().Backend(backend).
    Retention(runnerq.RetentionConfig{
        Completed: 7 * 24 * time.Hour,    // completed trees kept 7 days
        Failed:    30 * 24 * time.Hour,   // failures kept longer for inspection
        Interval:  10 * time.Minute,      // sweep cadence (default)
    }).Build()
```

The deletion unit is a whole **workflow tree** — a root that's been terminal
past its TTL and has no still-running descendants. Its activities, events,
results (including `ctx.Run`/`ctx.Sleep` checkpoints), and idempotency keys go
together, so a retried handler never finds a checkpoint missing. `Completed`
and `Failed` are separate clocks; zero means keep forever. Run it on every
engine — the backend elects one sweeper per queue via an advisory lock. See
[docs/configuration.md](../../docs/configuration.md#retention).

Next: [12 — Workload Isolation](../12-workload-isolation/).
