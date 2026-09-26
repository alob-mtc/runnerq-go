# Storage Backends

RunnerQ persists everything through a storage interface. PostgreSQL is the
built-in backend. The interface is exported so you *can* implement another,
and the conformance suite tells you when you have (see "Proving a backend
durable" below).

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

Two more optional interfaces bound what an execution that lost its lease can
do. `storage.AttemptLeaseStorage` (`ExtendLeaseForWorker`) lets the engine
heartbeat a running handler's claim and cancel the handler when the claim is
gone. `storage.SpawnStorage` (`EnqueueForWorker`, `EnqueueIdempotentForWorker`)
makes handler-issued spawns conditional on the claim, atomically with the
insert. Without them the engine falls back to lease sizing alone and unfenced
spawns.

### Proving a backend durable

`storage/storagetest` is the conformance suite: it states every behaviour the
engine relies on — claimed once under `SKIP LOCKED`-equivalent semantics,
fenced acknowledgements, lost-reply reconciliation, lease recovery, parked
parents woken by awaited results, checkpoints published by exactly one
owner, atomic idempotency, whole-tree retention — as tests driven only through the storage
interfaces. Run it from your backend's test file:

```go
type harness struct{}

func (harness) Open(t *testing.T, queue string) storage.Storage { /* fresh backend on queue */ }
func (harness) ExpireLease(ctx context.Context, b storage.Storage, id uuid.UUID) error { /* move the lease into the past */ }

func TestConformance(t *testing.T) { storagetest.Run(t, harness{}) }
```

The harness supplies only what the interface cannot express: opening a
backend on a named queue (two opens on one queue stand in for two processes)
and forcing a lease to expire. A backend that passes the suite can replace
Postgres without the engine noticing. The PostgreSQL backend runs it in CI.

> **Caveat:** the interface — especially the parking/wakeup and atomicity
> contracts behind durable execution — is non-trivial to get right. Read the
> doc comments in `storage/storage.go` before starting, use the PostgreSQL
> backend as a reference implementation, and treat a green conformance
> run as the bar for "done".
