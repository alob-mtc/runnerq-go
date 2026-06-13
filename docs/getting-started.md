# Getting Started

## Install

```bash
go get github.com/alob-mtc/runnerq-go
```

Requires Go 1.25+ and a PostgreSQL database (12+).

## Connect to Postgres

The Postgres backend creates its own schema on first connect (idempotently),
so there's no migration step:

```go
import "github.com/alob-mtc/runnerq-go/storage/postgres"

backend, err := postgres.New(ctx, "postgres://user:pass@localhost:5432/mydb", "my_app")
if err != nil {
    log.Fatal(err)
}
defer backend.Close()
```

The second argument is the **queue name** — a logical namespace stored as a
column, so many independent queues can share one database. For lease and pool
tuning, use `postgres.WithConfig` (see [Configuration](configuration.md)).

## Write a workflow

A workflow is an activity handler — a type implementing `ActivityHandler`.
Embed `DefaultDeadLetterHandler` unless you need a dead-letter callback.

```go
type Greeting struct {
    runnerq.DefaultDeadLetterHandler
}

func (h *Greeting) ActivityType() string { return "greeting" }

func (h *Greeting) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
    name := string(payload)
    return json.Marshal("hello, " + name)
}
```

## Build the engine and run it

```go
engine, err := runnerq.Builder().
    Backend(backend).
    MaxWorkers(8).
    Build()
if err != nil {
    log.Fatal(err)
}
engine.RegisterActivity("greeting", &Greeting{})

// Start blocks until the context is cancelled or a SIGINT/SIGTERM arrives.
go engine.Start(ctx)
```

## Enqueue and await

```go
future, err := engine.GetActivityExecutor().
    Activity("greeting").
    Payload(json.RawMessage(`"world"`)).
    Execute(ctx)
if err != nil {
    log.Fatal(err)
}

result, err := future.GetResult(ctx)   // blocks until the activity completes
fmt.Println(string(result))            // "hello, world"
```

## Shutdown

Stop the engine and wait for the graceful drain before closing the backend:

```go
done := make(chan struct{})
go func() { defer close(done); engine.Start(ctx) }()
// ... later ...
engine.Stop()   // stop intake, let in-flight handlers finish and ack
<-done
backend.Close()
```

## Next

- The [examples](../examples/) — runnable, realistic, copy-pasteable.
- [Durable Execution](durable-execution.md) — make workflows survive crashes.
