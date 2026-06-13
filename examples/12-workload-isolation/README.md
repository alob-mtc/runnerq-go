# 12 — Workload Isolation

Many worker fleets, one queue. By default an engine dequeues every activity
type; `.ActivityTypes(...)` restricts it, so slow jobs get their own workers
and can't starve latency-sensitive ones — no separate queues, brokers, or
binaries.

## What this shows

Three engines share one queue, each restricted to its own types: a
notifications fleet (`send_email`, `send_sms`), a slow reports fleet
(`generate_report`), and a finance fleet (`reconcile_ledger`). Per-fleet
counters prove each engine only ran the types it owns.

## Run it

```bash
docker compose up -d        # from the examples/ directory, if not already up
go run .
```

## Expected output

```text
enqueuing a mix of 12 activities across 4 types...
✓ handled — notifications fleet: 6, reports fleet: 3, finance fleet: 3
  each fleet only ran the activity types it was assigned.
```

## The key idea

```go
// Each engine claims only its declared types from the shared queue.
notif, _   := runnerq.Builder().Backend(backend).QueueName("q").
                  ActivityTypes([]string{"send_email", "send_sms"}).Build()
reports, _ := runnerq.Builder().Backend(backend).QueueName("q").
                  ActivityTypes([]string{"generate_report"}).Build()
```

These engines can live in one process or many — they coordinate only through
Postgres. An engine whose filter names a type with no registered handler
panics at `Start()` with a clear message.

### The catch-all variation

An engine with **no** `ActivityTypes` filter dequeues *every* type — useful as
overflow capacity. But because it can claim any type, it must register a
handler for **every** type in the queue; otherwise it claims an activity it
can't run and fails it as `handler_not_found`, stealing work from the fleet
that could have handled it. Register all handlers on a catch-all engine, or
give every type a dedicated fleet (as this example does). See
[docs/configuration.md](../../docs/configuration.md#workload-isolation).

Next: [13 — The Console](../13-console/).
