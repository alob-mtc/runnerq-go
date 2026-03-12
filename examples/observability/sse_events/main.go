// Example to test SSE event emission
//
// This example:
//  1. Starts a worker engine with a test activity handler
//  2. Serves the console UI with SSE
//  3. Automatically enqueues test activities every 2 seconds
//  4. You should see events in the browser console
//
// ## Prerequisites
//
//	docker run -d --name runnerq-postgres \
//	    -e POSTGRES_PASSWORD=runnerq \
//	    -e POSTGRES_DB=runnerq \
//	    -p 5432:5432 \
//	    postgres:16
//
// ## Running
//
//	export DATABASE_URL="postgres://postgres:runnerq@localhost:5432/runnerq"
//	go run ./examples/observability/sse_events
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go"
	"github.com/alob-mtc/runnerq-go/observability"
	"github.com/alob-mtc/runnerq-go/observability/ui"
	"github.com/alob-mtc/runnerq-go/storage/postgres"
)

// TestActivity simulates work and retries on the first 2 attempts.
type TestActivity struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *TestActivity) ActivityType() string {
	return "test_activity"
}

func (h *TestActivity) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	fmt.Printf("Processing test activity: %s\n", string(payload))
	time.Sleep(5 * time.Second)
	if ctx.RetryCount < 2 {
		return nil, runnerq.NewRetryError("Test activity failed")
	}
	fmt.Println("Completed test activity")
	result, _ := json.Marshal(map[string]any{"status": "completed"})
	return result, nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))

	ctx := context.Background()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:runnerq@localhost:5432/runnerq"
	}

	backend, err := postgres.New(ctx, databaseURL, "test_sse")
	if err != nil {
		log.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	engine, err := runnerq.Builder().
		Backend(backend).
		MaxWorkers(4).
		Build()
	if err != nil {
		log.Fatalf("Failed to build engine: %v", err)
	}

	engine.RegisterActivity("test_activity", &TestActivity{})

	inspector := observability.NewQueueInspector(backend).WithMaxWorkers(engine.MaxConcurrentActivities())

	executor := engine.GetActivityExecutor()

	// Start worker engine in background
	go func() {
		fmt.Println("Worker engine starting...")
		if err := engine.Start(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Worker engine error: %v\n", err)
		}
	}()

	// Enqueue test activities periodically
	go func() {
		fmt.Println("Waiting 3 seconds before first test activity...")
		time.Sleep(3 * time.Second)

		counter := 1
		for {
			fmt.Printf("\nEnqueueing test activity #%d\n", counter)

			payload, _ := json.Marshal(map[string]any{
				"test":      true,
				"counter":   counter,
				"timestamp": time.Now().UTC().Format(time.RFC3339),
			})
			_, err := executor.Activity("test_activity").
				Payload(payload).
				MaxRetries(5).
				IdempotencyKeyOption(uuid.New().String(), runnerq.ReturnExisting).
				Execute(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to enqueue activity: %v\n", err)
			} else {
				fmt.Printf("Activity #%d enqueued successfully\n", counter)
			}

			counter++
			time.Sleep(2 * time.Second)
		}
	}()

	// Build UI server
	mux := http.NewServeMux()
	mux.Handle("/console/", http.StripPrefix("/console", ui.RunnerQUI(inspector)))

	addr := "0.0.0.0:8081"
	fmt.Println()
	fmt.Println("══════════════════════════════════════════════════")
	fmt.Println("  SSE Test Server Running")
	fmt.Println("══════════════════════════════════════════════════")
	fmt.Printf("  Console UI:  http://%s/console/\n", addr)
	fmt.Printf("  SSE Stream:  http://%s/console/api/observability/stream\n", addr)
	fmt.Println("══════════════════════════════════════════════════")
	fmt.Println("  Events you should see:")
	fmt.Println("     1. Enqueued   - When activity added")
	fmt.Println("     2. Dequeued   - When worker picks it up")
	fmt.Println("     3. Started    - When processing begins")
	fmt.Println("     4. Completed  - When processing finishes")
	fmt.Println("══════════════════════════════════════════════════")
	fmt.Println("  Check browser DevTools console for events")
	fmt.Println("  Activities auto-enqueue every 2 seconds")
	fmt.Println("══════════════════════════════════════════════════")
	fmt.Println()

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}
