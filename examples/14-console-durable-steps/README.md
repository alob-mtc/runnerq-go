# 14 — Console: durable step history & "blocked on"

A focused visual test for the console's durable-execution views (the Steps tab
and the "Blocked on" banner).

Each `process_payment` workflow walks through every durable primitive:

| Step | Primitive | What you see in the console |
|------|-----------|------------------------------|
| `authorize` | `ctx.Run` | Steps tab → `run:authorize` (Ok) with stored result |
| `capture-confirmed` | `ctx.WaitForSignal` | Blocked-on banner → **"Waiting for signal capture-confirmed — times out in …"**, then clears when the signal is delivered |
| `settle` | `ctx.Run` | Steps tab → `run:settle` (Ok) |
| `clearing` | `ctx.Sleep` | Blocked-on banner → **"Sleeping — wakes in …"**; Steps tab → `sleep:clearing` |
| `notify` | child via `ctx.Step` + await | Blocked-on banner → **"Awaiting child …"** |

The capture signal is delivered ~8s after enqueue **by reference**
(`engine.SignalByKey`), so this also live-exercises addressing a parked
workflow by its business key.

## Run

```sh
docker compose up -d        # from the examples/ directory
go run .
# open http://localhost:8081/console/
```

Click a recent payment and open the **Steps** tab. Watch the step list fill
(`authorize → settle → clearing`) and the **Blocked-on** banner cycle through
all three wait kinds as the workflow progresses.

A short activity timeout (5s) is set at enqueue on purpose: it's well under the
signal wait and sleep, so those waits **park** (releasing the worker) — which is
exactly what surfaces the Blocked-on banner. Without it they'd wait in-process
and only show as "processing".
