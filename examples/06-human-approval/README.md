# 06 — Human-in-the-Loop with Signals

A workflow pauses — durably, holding no worker — until something external
happens: an approval click, a webhook, a payment confirmation. `WaitForSignal`
parks the workflow; `Signal` / `SignalActivity` delivers the payload from
anywhere, including another process.

## What this shows

An expense-approval workflow files a request, then waits for a decision. An
HTTP endpoint delivers it. While it waits, the workflow is just a row in
Postgres — no goroutine, no worker slot. The same workflow could wait for two
minutes or two weeks at the same (zero) cost.

## Run it

```bash
docker compose up -d        # from the examples/ directory, if not already up
go run .
```

It prints a waiting prompt. In another terminal:

```bash
curl -X POST localhost:8080/approve     # or /reject
```

## Expected output

```
workflow <id> is waiting.
  ⏸ awaiting a decision (POST /approve or /reject) — parked, holding no worker
  ✓ decision received: {"approved":true}
✓ resolved: {"approved":true}
```

## The key idea

```go
// In the workflow: park until a "decision" signal arrives (or 2 min passes).
decision, err := ctx.WaitForSignal("decision", 2*time.Minute)
if runnerq.IsSignalTimeout(err) { /* auto-reject */ }

// From anywhere with a backend handle — an HTTP handler, a Slack action,
// a Kafka consumer in a different process:
engine.Signal(ctx, activityID, "decision", payload)
// or, without an engine:
runnerq.SignalActivity(ctx, backend, activityID, "decision", payload)
```

Signals are **buffered**: if the decision arrives before the workflow reaches
`WaitForSignal` (or before it even starts), it's delivered the moment the wait
is reached. The activity's ID — `future.ActivityID()` — is the handle you
signal; persist it wherever your approval UI can find it.

Next: [07 — Fan-Out](../07-fan-out/).
