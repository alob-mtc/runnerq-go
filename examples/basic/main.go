// Example: Using PostgreSQL Backend with RunnerQ
//
// This example demonstrates how to use the PostgreSQL backend adapter
// with RunnerQ's worker engine.
//
// ## Prerequisites
//
// Start a PostgreSQL instance:
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
//	go run ./examples/basic
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"
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
	result, _ := json.Marshal(map[string]interface{}{"status": "completed"})
	return result, nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))

	ctx := context.Background()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:runnerq@localhost:5432/runnerq"
	}

	slog.Info("=== RunnerQ PostgreSQL Backend Example ===")
	atIdx := strings.Index(databaseURL, "@")
	if atIdx < 0 {
		atIdx = len(databaseURL)
	}
	safeURL := databaseURL[:atIdx] + "@..."
	slog.Info("Connecting to PostgreSQL", "url", safeURL)

	backend, err := postgres.New(ctx, databaseURL, "example_queue")
	if err != nil {
		log.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	slog.Info("PostgreSQL backend initialized")
	slog.Info("Database schema created/verified")

	engine, err := runnerq.Builder().
		Backend(backend).
		MaxWorkers(15).
		Build()
	if err != nil {
		log.Fatalf("Failed to build engine: %v", err)
	}

	slog.Info("Worker engine created with 4 workers")

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

			payload, _ := json.Marshal(map[string]interface{}{
				"test":      true,
				"counter":   counter,
				"timestamp": time.Now().UTC().Format(time.RFC3339),
			})
			_, err := executor.Activity("test_activity").
				Payload(payload).
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
	fmt.Println("══════════════════════════════════════════════════════════")
	fmt.Println("  RunnerQ PostgreSQL Backend Example")
	fmt.Println("══════════════════════════════════════════════════════════")
	fmt.Printf("  Console UI:  http://%s/console/\n", addr)
	fmt.Printf("  SSE Stream:  http://%s/console/api/observability/stream\n", addr)
	fmt.Println("══════════════════════════════════════════════════════════")
	fmt.Println("  Features:")
	fmt.Println("    - Permanent persistence (no TTL)")
	fmt.Println("    - Multi-node safe (FOR UPDATE SKIP LOCKED)")
	fmt.Println("    - Cross-process events (PostgreSQL LISTEN/NOTIFY)")
	fmt.Println("══════════════════════════════════════════════════════════")
	fmt.Println("  Press Ctrl+C to stop")
	fmt.Println("══════════════════════════════════════════════════════════")
	fmt.Println()

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}
