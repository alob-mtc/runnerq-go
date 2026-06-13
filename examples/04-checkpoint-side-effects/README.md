# 04 — Checkpoint Side Effects with `ctx.Run`

Make non-idempotent side effects — charges, emails, external API writes —
safe under retries. `ctx.Run` runs a function at most once per recorded
success and replays its stored result thereafter.

## What this shows

A billing workflow charges credits once, then sends a batch that fails twice
before succeeding. When the handler retries, the charge is **not** repeated —
only the failing send re-runs. Counters prove it: charge ran once, send ran
three times.

## Run it

```bash
docker compose up -d        # from the examples/ directory, if not already up
go run .
```

## Expected output

```
billing_run attempt #0
  ▶ charge-credits EXECUTING (run #1) — real money moves here
  ▶ send-batch EXECUTING (run #1)
billing_run attempt #1
  ▶ send-batch EXECUTING (run #2)         ← charge NOT re-run
billing_run attempt #2
  ▶ send-batch EXECUTING (run #3)
  ✓ batch delivered

✓ done: {"charge_id":"ch_42"}
  charge ran 1 time(s); send ran 3 time(s).
  → the charge happened exactly once despite the retries.
```

## The key idea

```go
receipt, _ := ctx.Run("charge-credits", func() (json.RawMessage, error) {
    return chargeCredits()    // stored on success → never runs again
})

_, err := ctx.Run("send-batch", func() (json.RawMessage, error) {
    return sendBatch()        // retryable failure → re-runs; charge stays put
})
```

`ctx.Run` semantics: a **success** is stored and replayed; a **`NonRetryError`**
is stored and replayed (the step stays failed); a **retryable error** is not
stored, so the step runs again next attempt. A crash between `fn` returning and
the checkpoint committing re-runs `fn` — so make `fn` as idempotent as the
external system allows (e.g. pass an idempotency key to your payment provider).

Next: [05 — Durable Sleep](../05-durable-sleep/).
