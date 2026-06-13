# RunnerQ Examples

Each example is a standalone, runnable program with its own README. They build
on each other — start at `01` and walk up.

## Setup (once)

Every example talks to Postgres. Start one here:

```bash
docker compose up -d      # Postgres on localhost:5432
```

Each example reads `DATABASE_URL` and falls back to
`postgres://postgres:runnerq@localhost:5432/runnerq`, so once Postgres is up
you can just `cd` into any example and `go run .`.

When you're done: `docker compose down -v`.

## The walkthrough

| # | Example | What it teaches |
|---|---------|-----------------|
| 01 | [hello-workflow](01-hello-workflow/) | A durable two-step workflow — the smallest useful program |
| 02 | [crash-and-resume](02-crash-and-resume/) | **The headline.** Kill it mid-flight; it resumes without redoing work |
| 03 | [steps-and-retries](03-steps-and-retries/) | `.Step()` — child activities that survive a retry without re-running |
| 04 | [checkpoint-side-effects](04-checkpoint-side-effects/) | `ctx.Run` — charge once, even when the handler retries |
| 05 | [durable-sleep](05-durable-sleep/) | `ctx.Sleep` — timers that survive restarts (a 24h wait holds no worker) |
| 06 | [human-approval](06-human-approval/) | `ctx.WaitForSignal` — pause for an approval/webhook, delivered over HTTP |
| 07 | [fan-out](07-fan-out/) | `WaitAll` — spawn many children, run them in parallel, collect results |

If you only run one, run **02** — it's the demo that shows what "durable" means.

## More examples

These predate the walkthrough above and are being reworked, but still run:

- [observability/](observability/) — the built-in web console (live activity view over SSE).
- [advanced/activity_filtering/](advanced/activity_filtering/) — multiple worker fleets sharing one queue, each handling different activity types.
