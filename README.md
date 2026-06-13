# RunnerQ

**Durable workflows and queues for Go, backed by Postgres.**

Add crash-proof background jobs and multi-step workflows to your Go app in a
few lines — no separate orchestrator to run, no new infrastructure. RunnerQ is
a library: import it, point it at the Postgres you already have, and write
workflows as ordinary Go functions. When a process crashes mid-workflow, it
resumes from where it left off without redoing completed work.

```go
func (h *Checkout) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
    // Each step is checkpointed in Postgres. Crash after the charge and
    // restart — the charge is NOT repeated; the workflow resumes at shipping.
    receipt, err := ctx.Run("charge-card", func() (json.RawMessage, error) {
        return chargeCard(payload)   // runs exactly once
    })
    if err != nil {
        return nil, err
    }

    ship, err := ctx.ActivityExecutor.Activity("ship").Step("ship").Payload(payload).Execute(ctx.Ctx)
    if err != nil {
        return nil, err
    }
    return ship.GetResult(ctx.Ctx)   // parent parks here — frees the worker, survives deploys
}
```

```bash
go get github.com/alob-mtc/runnerq-go
```

> See it for real: [`examples/02-crash-and-resume`](examples/02-crash-and-resume/)
> charges an order, lets you `Ctrl-C` it, and resumes on restart without
> double-charging. That demo is the whole pitch in 30 seconds.

## Why RunnerQ

Background work that touches the real world — charging cards, sending email,
calling APIs, orchestrating multi-step jobs — has to survive crashes,
deploys, and retries without doing things twice. The usual options are a heavy
workflow server (a cluster to operate) or a plain task queue (no durability —
you hand-roll idempotency and recovery yourself).

RunnerQ gives you durable execution as a **library**:

- **A workflow is just a Go function.** Orchestration is normal control flow —
  loops, conditionals, error handling — not a DSL or a DAG.
- **Steps are checkpointed.** Completed work is skipped on replay, so retries
  and restarts are cheap and side effects don't repeat.
- **Workflows pause for free.** Sleep for days or wait for a webhook while
  holding zero workers — paused workflows are just rows in Postgres.
- **Only Postgres.** No orchestrator cluster, no broker, no control-plane
  bill. Scale by adding stateless worker processes.

### When to use it

- **Use RunnerQ** when you want durable, crash-safe workflows or queues with
  minimal new infrastructure, and you're already running Postgres.
- **Reach for a dedicated workflow server** (Temporal, Cadence) if you need
  many SDK languages or want workflow orchestration decoupled from your app
  and your database.
- **A plain queue** (River, Asynq, SQS) is enough if you only need
  fire-and-forget tasks and don't need workflows, steps, signals, or durable
  timers.

## Features

### Durable workflows that survive crashes

A handler that calls `ctx.Run` steps is a durable workflow. Each step's result
is checkpointed; if the process dies, the handler replays and completed steps
return their stored results instead of re-executing.

```go
receipt, err := ctx.Run("charge-card", func() (json.RawMessage, error) {
    return chargeCard(payload)   // at most once, even across retries and restarts
})
```

### Orchestration that fast-forwards on retry

Spawn child activities with `.Step(name)`. A retried parent reattaches to the
children it already launched instead of duplicating them, and parents that
await children **park** in the database — no goroutine, no lease, no retry
burned while they wait.

```go
fut, _ := ctx.ActivityExecutor.Activity("reserve").Step("reserve").Payload(p).Execute(ctx.Ctx)
reserved, _ := fut.GetResult(ctx.Ctx)   // memoized on replay
```

### Durable timers

Sleep inside a workflow for seconds or weeks. The deadline is persisted —
a restart resumes the remainder — and long sleeps hold no worker.

```go
ctx.Sleep("cooling-off", 24 * time.Hour)   // survives restarts; frees the worker
```

### Signals — pause for the outside world

Wait for an approval, a webhook, or a payment confirmation. The workflow parks
until a signal arrives, delivered from anywhere — even another process.

```go
decision, err := ctx.WaitForSignal("approval", 48*time.Hour)   // parks, holds no worker
// from an HTTP handler, a Slack action, a Kafka consumer:
engine.Signal(ctx, activityID, "approval", payload)
```

### Exactly-once enqueue

Idempotency keys make enqueuing exactly-once — dedupe webhooks and event
handlers with one option.

```go
executor.Activity("process_event").
    Payload(p).
    IdempotencyKeyOption(eventID, runnerq.ReturnExisting).   // duplicate deliveries collapse to one
    Execute(ctx)
```

### A real queue underneath

Priorities, exponential-backoff retries, a dead-letter queue, scheduling, and
worker-level type filtering so slow jobs can't starve latency-sensitive ones.

```go
executor.Activity("send_email").
    Payload(p).
    Priority(runnerq.PriorityHigh).
    MaxRetries(5).
    Timeout(2 * time.Minute).
    Delay(30 * time.Second).
    Execute(ctx)
```

### A built-in console

An embedded web dashboard (no npm, no build step) with live queue stats,
activity browsing, results, and lifecycle timelines over SSE.

```go
mux.Handle("/console/", http.StripPrefix("/console", ui.RunnerQUI(inspector)))
```

## How it compares

| | RunnerQ | Temporal / Cadence | Plain task queue (River, Asynq, …) |
|---|---|---|---|
| Durable workflows | ✅ | ✅ | ❌ |
| Deploy model | a Go library | a server cluster to operate | a library |
| Infrastructure | Postgres you already run | server + its own datastore | Postgres or Redis |
| Workflow code | plain Go, step-memoized | plain code, full replay-determinism | n/a |
| Durable timers / signals | ✅ | ✅ | ❌ |
| Scale | add stateless worker processes | scale the cluster | add workers |

**The economics:** RunnerQ adds one dependency you almost certainly already
have — Postgres. There's no orchestrator cluster to size, secure, and pay for,
and no separate control plane. Workers are stateless; scale out by running
more of your own binary. The trade-off versus a dedicated server is
deliberate: durable execution that fits *inside* your app and your database,
rather than a system you operate alongside it.

## Quick start

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"

    "github.com/alob-mtc/runnerq-go"
    "github.com/alob-mtc/runnerq-go/storage/postgres"
)

type Greeting struct{ runnerq.DefaultDeadLetterHandler }

func (h *Greeting) ActivityType() string { return "greeting" }

func (h *Greeting) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
    greeting, err := ctx.Run("compose", func() (json.RawMessage, error) {
        return json.Marshal("hello, " + string(payload))
    })
    return greeting, err
}

func main() {
    ctx := context.Background()

    backend, err := postgres.New(ctx, "postgres://postgres:runnerq@localhost:5432/runnerq", "my_app")
    if err != nil {
        log.Fatal(err)
    }
    defer backend.Close()

    engine, err := runnerq.Builder().Backend(backend).MaxWorkers(8).Build()
    if err != nil {
        log.Fatal(err)
    }
    engine.RegisterActivity("greeting", &Greeting{})
    go engine.Start(ctx)

    future, _ := engine.GetActivityExecutor().
        Activity("greeting").
        Payload(json.RawMessage(`"world"`)).
        Execute(ctx)

    result, _ := future.GetResult(ctx)
    fmt.Println(string(result))   // "hello, world"
}
```

## Examples

Runnable, realistic, copy-pasteable — each in [`examples/`](examples/) with its
own README. `docker compose up -d` once, then `go run .` in any of them.

| # | Example | Shows |
|---|---------|-------|
| 01 | [hello-workflow](examples/01-hello-workflow/) | a durable two-step workflow |
| 02 | [crash-and-resume](examples/02-crash-and-resume/) | **kill it mid-flight; it resumes without redoing work** |
| 03 | [steps-and-retries](examples/03-steps-and-retries/) | child activities memoized across a retry |
| 04 | [checkpoint-side-effects](examples/04-checkpoint-side-effects/) | charge once, even when the handler retries |
| 05 | [durable-sleep](examples/05-durable-sleep/) | timers that survive restarts |
| 06 | [human-approval](examples/06-human-approval/) | pause for an approval, delivered over HTTP |
| 07 | [fan-out](examples/07-fan-out/) | spawn many children, run them in parallel |
| 08 | [exactly-once-webhook](examples/08-exactly-once-webhook/) | idempotency keys collapse duplicate deliveries |
| 09 | [cross-process-futures](examples/09-cross-process-futures/) | enqueue in a web tier, await by ID anywhere |
| 10 | [retries-and-dead-letter](examples/10-retries-and-dead-letter/) | backoff, the dead-letter queue, `OnDeadLetter` |
| 11 | [retention](examples/11-retention/) | TTL cleanup of completed workflows |
| 12 | [workload-isolation](examples/12-workload-isolation/) | many worker fleets sharing one queue |
| 13 | [console](examples/13-console/) | the built-in observability dashboard |

## Documentation

Full reference in [`docs/`](docs/):

- [Getting Started](docs/getting-started.md)
- [Workflows & Activities](docs/workflows.md)
- [Durable Execution](docs/durable-execution.md) — `Step`, `Run`, `Sleep`, signals, versioning
- [Retries & Dead Letter](docs/retries-and-dead-letter.md)
- [Configuration](docs/configuration.md) — tuning, scheduling, workload isolation, retention
- [Observability](docs/observability.md) — console, stats, metrics
- [Storage Backends](docs/storage-backends.md)

## Status

RunnerQ is pre-1.0. The durable-execution core (steps, checkpoints, signals,
durable sleep, crash recovery) is built and integration-tested against
Postgres, but APIs may still change before 1.0. See [RELEASING.md](RELEASING.md)
for the stability policy.

## License

MIT — see [LICENSE](LICENSE).
