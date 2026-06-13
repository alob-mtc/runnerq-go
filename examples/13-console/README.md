# 13 — The Observability Console

RunnerQ ships an embedded web console — live queue stats, activity browsing,
results, and lifecycle timelines over Server-Sent Events — with no npm and no
build step.

## What this shows

A realistic, never-ending stream of order workflows feeds the dashboard so you
can see every state move in real time: **running**, **retrying** (every third
order has a flaky first attempt), **waiting** (each order durably sleeps while
"packing"), **completed**, and the parent→child **workflow tree** (each order
spawns a shipping activity).

This is the example to leave running while you click around the UI.

## Run it

```bash
docker compose up -d        # from the examples/ directory, if not already up
go run .
# open http://localhost:8081/console/
```

`Ctrl-C` to stop.

## The key idea

```go
inspector := observability.NewQueueInspector(backend).
    WithMaxWorkers(engine.MaxConcurrentActivities())

mux := http.NewServeMux()
mux.Handle("/console/", http.StripPrefix("/console", ui.RunnerQUI(inspector)))
http.ListenAndServe(":8081", mux)
```

Mount it on your own `http.ServeMux` alongside your app's routes. All SSE
subscribers share a single backend connection via a fan-out hub, so a wall of
dashboard tabs costs one database connection, not one per tab. For a custom UI,
serve just the JSON API with `ui.ObservabilityAPI(inspector)`.

> **Security:** the console and API are unauthenticated and the SSE stream
> sends `Access-Control-Allow-Origin: *`. Put them behind your own auth and
> don't expose them publicly. See
> [docs/observability.md](../../docs/observability.md).

That's the full walkthrough. The reference docs are in [docs/](../../docs/).
