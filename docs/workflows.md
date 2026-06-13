# Workflows & Activities

In RunnerQ an **activity** is a unit of work; a **workflow** is just an
activity handler that orchestrates other activities. There's no separate
"workflow" type to learn — orchestration is plain Go control flow inside a
handler.

## The handler interface

```go
type ActivityHandler interface {
    ActivityType() string
    Handle(ctx ActivityContext, payload json.RawMessage) (json.RawMessage, error)
    OnDeadLetter(ctx ActivityContext, payload json.RawMessage, errorMsg string)
}
```

Embed `runnerq.DefaultDeadLetterHandler` for a no-op `OnDeadLetter`. Handlers
must be safe for concurrent use — the engine runs many at once.

A handler returns:

- `(result, nil)` — success; `result` may be nil.
- `(nil, runnerq.NewRetryError(msg))` — retry with backoff.
- `(nil, runnerq.NewNonRetryError(msg))` — fail permanently (no retry).
- any other non-nil `error` — treated as retryable.

See [Retries & Dead Letter](retries-and-dead-letter.md) for the failure model.

## The activity context

`ActivityContext` carries everything a handler needs:

| Field / method | Purpose |
|---|---|
| `ActivityID` | this activity's UUID |
| `ActivityType` | the registered type string |
| `RetryCount` | attempt number (0 on first run) |
| `Metadata` | `map[string]string` set at enqueue time |
| `Ctx` | a `context.Context` bounded by the activity timeout |
| `ActivityExecutor` | spawn child activities (tagged as children of this one) |
| `ParentActivityID`, `RootActivityID`, `Depth` | lineage |
| `Run`, `Sleep`, `WaitForSignal` | durable primitives — see [Durable Execution](durable-execution.md) |

```go
func (h *MyHandler) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
    if id, ok := ctx.Metadata["correlation_id"]; ok {
        // ...
    }
    if ctx.Ctx.Err() != nil {
        return nil, runnerq.NewNonRetryError("cancelled")
    }
    // ...
}
```

## Enqueuing activities

Get an executor from the engine (`engine.GetActivityExecutor()`) or from
`ctx.ActivityExecutor` inside a handler. Both use the same fluent builder:

```go
future, err := executor.Activity("send_email").
    Payload(payload).
    Priority(runnerq.PriorityHigh).   // Critical > High > Normal (default) > Low
    MaxRetries(5).                     // default 3; 0 = unlimited
    Timeout(2 * time.Minute).          // default 300s
    MaxRetryDelay(10 * time.Minute).   // cap on backoff; default 1h
    Delay(30 * time.Second).           // run no earlier than now+delay
    Metadata("tenant", "acme").
    IdempotencyKeyOption("order-42", runnerq.ReturnExisting).
    Execute(ctx)
```

`Payload` is required. Everything else is optional. `Execute` returns an
`*ActivityFuture`.

### Idempotency keys

`IdempotencyKeyOption(key, behavior)` makes an enqueue exactly-once for that
key. Behaviors:

- `runnerq.ReturnExisting` — if the key exists, return a future for the
  existing activity (don't enqueue a duplicate). The common choice.
- `runnerq.AllowReuse` — repoint the key at a new activity each time.
- `runnerq.AllowReuseOnFailure` — reuse only if the prior activity failed.
- `runnerq.NoReuse` — error if the key already exists.

Keys are scoped per queue. The claim-and-enqueue is a single atomic operation,
so a crash can never leave a key pointing at an activity that was never
created.

## Orchestration: spawning children

Inside a handler, `ctx.ActivityExecutor` spawns child activities tagged with
this activity's lineage. This is how you build multi-step workflows:

```go
func (h *Order) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
    pay, err := ctx.ActivityExecutor.Activity("charge").Step("charge").Payload(payload).Execute(ctx.Ctx)
    if err != nil { return nil, err }
    if _, err := pay.GetResult(ctx.Ctx); err != nil { return nil, err }

    ship, err := ctx.ActivityExecutor.Activity("ship").Step("ship").Payload(payload).Execute(ctx.Ctx)
    if err != nil { return nil, err }
    return ship.GetResult(ctx.Ctx)
}
```

Use `.Step(name)` for spawns inside a handler — it makes the workflow
crash-safe (see [Durable Execution](durable-execution.md)). `MaxActivityDepth`
(default 32) caps recursion depth. `.AsRoot()` detaches a spawn from its
parent's lineage for fire-and-forget side jobs.

## Getting results

`future.GetResult(ctx)` returns the activity's result, blocking until it's
available or `ctx` is done.

- **Outside a handler** (e.g. an HTTP server awaiting a workflow) it simply
  blocks — notification-driven, with a slow poll fallback, and works even if a
  different process produced the result.
- **Inside a handler**, awaiting a child parks the parent durably after a
  short grace — see [Durable Execution](durable-execution.md).

A failed activity surfaces as a `*WorkerError`:

```go
result, err := future.GetResult(ctx)
if err != nil {
    if we, ok := runnerq.IsWorkerError(err); ok {
        log.Printf("failed (retryable=%v): %v", we.IsRetryable(), we)
    }
    return
}
```

## Awaiting across processes

A future is just an activity ID plus a backend handle, so it's rehydratable —
enqueue a workflow in one process and await its result in another:

```go
// process A:
id := future.ActivityID()                    // persist this handle somewhere

// process B (only has a backend):
fut := runnerq.FutureFor(backend, id)
result, err := fut.GetResult(ctx)
```

`runnerq.WaitAll(ctx, futs...)` awaits several futures and returns their
results in order — handy for fan-out (see
[example 07](../examples/07-fan-out/)).
