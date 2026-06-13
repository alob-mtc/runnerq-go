# Storage Backends

RunnerQ persists everything through a storage interface. PostgreSQL is the
built-in, supported backend; the interface is exported so you *can* implement
another, though that's an advanced undertaking (see the caveat below).

## PostgreSQL

```go
import "github.com/alob-mtc/runnerq-go/storage/postgres"

backend, err := postgres.New(ctx, "postgres://user:pass@localhost/runnerq", "my_queue")
// or, with lease/pool tuning:
backend, err := postgres.WithConfig(ctx, dsn, "my_queue", /*leaseMS*/ 30_000, /*pool*/ 50)
defer backend.Close()
```

What the backend gives you:

- **Crash-safe claiming** — `FOR UPDATE SKIP LOCKED` dequeue with leases;
  expired leases are reaped and requeued.
- **Atomic durability** — results are stored in the same transaction as
  completion; idempotency claim and enqueue are one atomic step.
- **Event-driven wakeups** — `LISTEN/NOTIFY` drives blocking dequeue, future
  resolution, and the console stream, with table re-checks as a lossless
  fallback. No busy-polling.
- **Self-managing schema** — created idempotently on connect (advisory-locked
  so concurrent boots don't race; hot indexes built `CONCURRENTLY`).

### Schema

On first connect the backend creates:

| Table | Holds |
|---|---|
| `runnerq_activities` | activities and their lifecycle state |
| `runnerq_events` | append-only lifecycle event timeline |
| `runnerq_results` | activity results and `Run`/`Sleep` checkpoints |
| `runnerq_idempotency` | idempotency-key → activity mapping |
| `runnerq_worker_pools` | live engine registry for capacity reporting |

Without [retention](configuration.md#retention) configured, these grow
forever — turn it on for production.

## Implementing a custom backend

A custom backend must implement `storage.Storage`, which composes
`QueueStorage` (enqueue/dequeue/ack, leases, idempotency, results, signals,
yield, retention), `InspectionStorage` (stats, listings, event stream), and
`WorkerPoolStorage` (engine registry). It's a large surface — the durable
primitives in particular (`Yield`, `WakeWaiting`, `SignalActivity`,
`EnqueueIdempotent`, `CleanupExpired`, atomic `StoreResult` with an owner) each
carry correctness requirements documented on the interface methods in
[`storage/storage.go`](../storage/storage.go).

```go
type MyBackend struct{ /* ... */ }

func (b *MyBackend) Enqueue(ctx context.Context, a storage.QueuedActivity) error { /* ... */ }
func (b *MyBackend) Dequeue(ctx context.Context, workerID string, timeout time.Duration, types []string) (*storage.QueuedActivity, error) { /* ... */ }
func (b *MyBackend) AckSuccess(ctx context.Context, id uuid.UUID, result json.RawMessage, workerID string) error { /* ... */ }
// ... and the rest of QueueStorage + InspectionStorage + WorkerPoolStorage

engine, _ := runnerq.Builder().Backend(&MyBackend{}).Build()
```

If your backend can block efficiently until a result exists, also implement the
optional `storage.ResultWaiter` (`WaitForResult`) — futures use it instead of
polling, and it must work across processes. Without it, awaiting falls back to
a 100ms poll.

> **Caveat:** the interface — especially the parking/wakeup and atomicity
> contracts behind durable execution — is non-trivial to get right. Unless you
> have a strong reason to target another store, run the Postgres backend (or
> wrap it). Read the doc comments in `storage/storage.go` before starting, and
> lean on the integration tests in `storage/postgres` as a conformance
> reference.
