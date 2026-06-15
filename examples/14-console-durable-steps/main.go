// 14 — Console: durable step history & "blocked on"
//
// A focused visual test for the console's durable-execution views. Each
// payment workflow walks through every durable primitive so you can SEE them
// in the console's run detail:
//
//   - ctx.Run("authorize") / ctx.Run("settle")  → the Steps tab lists named
//     "run" checkpoints with their stored results.
//   - ctx.WaitForSignal("capture-confirmed")     → parks; the "Blocked on"
//     banner reads "Waiting for signal capture-confirmed — times out in …".
//     A driver delivers the signal by reference (SignalByKey) a few seconds
//     later, so you can watch the banner clear and the next step run.
//   - ctx.Sleep("clearing")                       → parks; banner reads
//     "Sleeping — wakes in …".
//   - a child step ("notify") that the parent awaits → banner reads
//     "Awaiting child …".
//
// Open the console, click a recent payment, and watch the Steps tab fill and
// the Blocked-on banner cycle through all three wait kinds.
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

const paymentActivity = "process_payment"

// ProcessPayment exercises run / signal-wait / sleep / await-child in one
// durable workflow. A short activity timeout (set at enqueue) makes the
// signal-wait, sleep, and child-await all PARK rather than wait in-process —
// which is what surfaces the "Blocked on" banner in the console.
type ProcessPayment struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *ProcessPayment) ActivityType() string { return paymentActivity }

func (h *ProcessPayment) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	// Step 1 — a named side effect. Shows as "run:authorize" in the Steps tab.
	if _, err := ctx.Run("authorize", func() (json.RawMessage, error) {
		return json.RawMessage(`{"auth_id":"auth_8842","amount_cents":4200}`), nil
	}); err != nil {
		return nil, err
	}

	// Step 2 — wait for an external "capture confirmed" signal. Parks the
	// workflow → banner: "Waiting for signal capture-confirmed". The driver
	// goroutine delivers it by reference; on timeout we proceed anyway so a
	// missed signal never wedges the demo.
	if _, err := ctx.WaitForSignal("capture-confirmed", 90*time.Second); err != nil {
		if !runnerq.IsSignalTimeout(err) {
			return nil, err // propagate the yield sentinel and real errors
		}
	}

	// Step 3 — another named side effect: "run:settle".
	if _, err := ctx.Run("settle", func() (json.RawMessage, error) {
		return json.RawMessage(`{"settled":true,"ledger_ref":"LGR-1190"}`), nil
	}); err != nil {
		return nil, err
	}

	// Step 4 — a durable timer. Parks → banner: "Sleeping — wakes in …".
	if err := ctx.Sleep("clearing", 10*time.Second); err != nil {
		return nil, err
	}

	// Step 5 — spawn and await a child. Parks → banner: "Awaiting child …".
	notify, err := ctx.ActivityExecutor.Activity("notify_customer").Step("notify").Payload(payload).Execute(ctx.Ctx)
	if err != nil {
		return nil, err
	}
	if _, err := notify.GetResult(ctx.Ctx); err != nil {
		return nil, err
	}

	return json.RawMessage(`{"status":"completed"}`), nil
}

// NotifyCustomer is the awaited child; it takes a few seconds so the parent
// visibly parks "Awaiting child" while it runs.
type NotifyCustomer struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *NotifyCustomer) ActivityType() string { return "notify_customer" }
func (h *NotifyCustomer) Handle(_ runnerq.ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
	time.Sleep(4 * time.Second)
	return json.RawMessage(`{"notified":true,"channel":"email"}`), nil
}

func main() {
	ctx := context.Background()

	backend, err := postgres.New(ctx, databaseURL(), "console_steps_demo")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer backend.Close()

	engine, err := runnerq.Builder().Backend(backend).MaxWorkers(20).Build()
	if err != nil {
		log.Fatalf("build: %v", err)
	}
	engine.RegisterActivity(paymentActivity, &ProcessPayment{})
	engine.RegisterActivity("notify_customer", &NotifyCustomer{})
	go func() {
		if err := engine.Start(ctx); err != nil {
			log.Fatalf("engine stopped: %v", err)
		}
	}()

	// Enqueue a steady stream of payments, each keyed by a human reference, and
	// schedule its capture signal a few seconds out. Staggering keeps several
	// workflows parked in different wait states at any moment, so the console
	// always has something to look at.
	var seq atomic.Int64
	go func() {
		for {
			n := seq.Add(1)
			ref := fmt.Sprintf("pay-%04d", n)
			payload, _ := json.Marshal(map[string]any{"reference": ref, "amount_cents": 4200})

			// A 5s timeout (well under the 90s signal wait and 10s sleep) forces
			// those waits to PARK, which is what shows the Blocked-on banner.
			if _, err := engine.GetActivityExecutor().
				Activity(paymentActivity).
				IdempotencyKeyOption(ref, runnerq.ReturnExisting).
				Timeout(5 * time.Second).
				Payload(payload).
				Execute(ctx); err != nil {
				log.Printf("enqueue %s failed: %v", ref, err)
			}

			// Deliver "capture-confirmed" by reference ~8s later — watch the
			// signal-wait banner clear in the console. This also live-exercises
			// SignalByKey (address a parked workflow by its business key).
			capturePayload, _ := json.Marshal(map[string]any{"captured_at": "live", "fee_cents": 63})
			time.AfterFunc(8*time.Second, func() {
				if err := engine.SignalByKey(ctx, paymentActivity, ref, "capture-confirmed", capturePayload); err != nil {
					if !runnerq.IsActivityNotFound(err) {
						log.Printf("signal %s failed: %v", ref, err)
					}
				}
			})

			time.Sleep(3 * time.Second)
		}
	}()

	inspector := observability.NewQueueInspector(backend).WithMaxWorkers(engine.MaxConcurrentActivities())
	mux := http.NewServeMux()
	mux.Handle("/console/", http.StripPrefix("/console", ui.RunnerQUI(inspector)))

	// PORT lets this run alongside other console examples (13-console defaults
	// to 8081 too). Default 8082 to avoid colliding with it out of the box.
	addr := ":" + consolePort()
	fmt.Println("payments are flowing — open the console and click a recent payment:")
	fmt.Printf("  http://localhost%s/console/\n", addr)
	fmt.Println("watch the Steps tab fill (authorize → settle → clearing) and the")
	fmt.Println("Blocked-on banner cycle: Waiting for signal → Sleeping → Awaiting child.")
	fmt.Println("(Ctrl-C to stop)")
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func consolePort() string {
	if v := os.Getenv("PORT"); v != "" {
		return v
	}
	return "8082"
}

func databaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:runnerq@localhost:5432/runnerq"
}
