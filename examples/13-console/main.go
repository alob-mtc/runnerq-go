// 13 — The Observability Console
//
// RunnerQ ships an embedded web console (no npm, no build step) with live
// queue stats, activity browsing, results, and lifecycle timelines over SSE.
//
// This runs a realistic, never-ending stream of order workflows — with
// retries, durable sleeps, and child activities — so the dashboard shows
// every state: running, retrying, waiting, completed, and the workflow tree.
// Open the console and watch it work.
//
//	docker compose up -d        # from the examples/ directory
//	go run .
//	# open http://localhost:8081/console/
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/alob-mtc/runnerq-go"
	"github.com/alob-mtc/runnerq-go/observability"
	"github.com/alob-mtc/runnerq-go/observability/ui"
	"github.com/alob-mtc/runnerq-go/storage/postgres"
)

type FulfillOrder struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *FulfillOrder) ActivityType() string { return "fulfill_order" }

func (h *FulfillOrder) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	var in struct {
		Order int `json:"order"`
	}
	if err := json.Unmarshal(payload, &in); err != nil {
		return nil, runnerq.NewNonRetryError("bad payload: " + err.Error())
	}

	if _, err := ctx.Run("reserve", func() (json.RawMessage, error) {
		return json.RawMessage(`{"reserved":true}`), nil
	}); err != nil {
		return nil, err
	}

	// Every third order has a flaky downstream on its first attempt — this
	// populates the "retrying" state in the console.
	if in.Order%3 == 0 && ctx.RetryCount == 0 {
		return nil, runnerq.NewRetryError("inventory service hiccup")
	}

	if _, err := ctx.Run("charge", func() (json.RawMessage, error) {
		return json.RawMessage(`{"charge_id":"ch"}`), nil
	}); err != nil {
		return nil, err
	}

	// A durable wait — shows up as "waiting" in the console.
	if err := ctx.Sleep("packing", 8*time.Second); err != nil {
		return nil, err
	}

	// A child activity — shows the workflow tree.
	ship, err := ctx.ActivityExecutor.Activity("ship_order").Step("ship").Payload(payload).Execute(ctx.Ctx)
	if err != nil {
		return nil, err
	}
	return ship.GetResult(ctx.Ctx)
}

type ShipOrder struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *ShipOrder) ActivityType() string { return "ship_order" }
func (h *ShipOrder) Handle(_ runnerq.ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
	time.Sleep(time.Second)
	return json.RawMessage(`{"tracking":"1Z999"}`), nil
}

func main() {
	ctx := context.Background()

	backend, err := postgres.New(ctx, databaseURL(), "console_demo")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer backend.Close()

	engine, err := runnerq.Builder().Backend(backend).MaxWorkers(8).Build()
	if err != nil {
		log.Fatalf("build: %v", err)
	}
	engine.RegisterActivity("fulfill_order", &FulfillOrder{})
	engine.RegisterActivity("ship_order", &ShipOrder{})
	go engine.Start(ctx)

	// Keep a steady stream of orders flowing so the dashboard always has
	// something to show.
	var seq atomic.Int64
	go func() {
		for {
			n := seq.Add(1)
			payload, _ := json.Marshal(map[string]int{"order": int(n)})
			engine.GetActivityExecutor().Activity("fulfill_order").Payload(payload).Execute(ctx)
			time.Sleep(2 * time.Second)
		}
	}()

	// Mount the console. Pass MaxWorkers so the dashboard can show capacity.
	inspector := observability.NewQueueInspector(backend).WithMaxWorkers(engine.MaxConcurrentActivities())
	mux := http.NewServeMux()
	mux.Handle("/console/", http.StripPrefix("/console", ui.RunnerQUI(inspector)))

	fmt.Println("orders are flowing — open the console:")
	fmt.Println("  http://localhost:8081/console/")
	fmt.Println("(Ctrl-C to stop)")
	if err := http.ListenAndServe(":8081", mux); err != nil {
		log.Fatal(err)
	}
}

func databaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:runnerq@localhost:5432/runnerq"
}
