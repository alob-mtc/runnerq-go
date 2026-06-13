# RunnerQ Documentation

Reference docs for RunnerQ. New here? Start with the
[examples walkthrough](../examples/) — it's the fastest way in. These pages
are the deeper reference.

## Contents

- **[Getting Started](getting-started.md)** — install, connect to Postgres, write and run your first workflow.
- **[Workflows & Activities](workflows.md)** — handlers, the activity context, spawning child activities, getting results, awaiting across processes.
- **[Durable Execution](durable-execution.md)** — the core: `Step`, `ctx.Run`, `ctx.Sleep`, `WaitForSignal`, how parents park and resume, and versioning across deploys.
- **[Retries & Dead Letter](retries-and-dead-letter.md)** — error types, backoff, delivery semantics, lease recovery, the dead-letter queue.
- **[Configuration](configuration.md)** — builder options, defaults, lease/pool tuning, scheduling, workload isolation, retention.
- **[Observability](observability.md)** — the web console, the inspector API, queue stats, metrics.
- **[Storage Backends](storage-backends.md)** — the Postgres backend, the schema, implementing your own.

## The one-paragraph mental model

A **workflow** is an ordinary Go function (an activity handler). Inside it you
call **steps** — `ctx.Run` for local work, `.Step(...)` spawns for child
activities — each of which is checkpointed in Postgres. If the process
crashes, the handler re-runs from the top, but completed steps return their
stored results instead of re-executing, so the workflow fast-forwards to where
it left off. `ctx.Sleep` and `ctx.WaitForSignal` let a workflow pause for
hours or days without holding any worker. That's the whole idea; everything
else is configuration.
