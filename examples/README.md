# RunnerQ Go Examples

All examples require a running PostgreSQL instance:

```bash
docker run -d --name runnerq-postgres \
    -e POSTGRES_PASSWORD=runnerq \
    -e POSTGRES_DB=runnerq \
    -p 5432:5432 \
    postgres:16
```

Set the connection string (or use the default `postgres://postgres:runnerq@localhost:5432/runnerq`):

```bash
export DATABASE_URL="postgres://postgres:runnerq@localhost:5432/runnerq"
```

## basic/

Getting started with RunnerQ and the PostgreSQL backend. Creates a worker engine, registers a test activity handler that retries on the first 2 attempts, enqueues activities periodically, and serves the console UI.

```bash
go run ./examples/basic
```

## observability/

### console_ui/

The simplest way to add the RunnerQ Console UI with real-time SSE updates. No extra configuration required.

```bash
go run ./examples/observability/console_ui
```

### sse_events/

Tests SSE event emission end-to-end. Enqueues activities that retry, so you can observe the full lifecycle (enqueued, dequeued, started, failed, retried, completed) in the browser DevTools console.

```bash
go run ./examples/observability/sse_events
```

## advanced/

### activity_filtering/

Runs multiple worker engines on the **same queue**, each filtering for different activity types. Demonstrates workload isolation without separate queues or binaries:

| Node          | Activity Types               |
|---------------|------------------------------|
| `email-node`  | `send_email`, `send_sms`     |
| `trade-node`  | `execute_trade`              |
| `catch-all`   | everything                   |

```bash
go run ./examples/advanced/activity_filtering
```
