// Example showing how to serve the RunnerQ Console UI with real-time updates
//
// This example demonstrates the simplest way to add observability to your RunnerQ instance.
// The UI will be available at http://localhost:8081/console/ with real-time SSE updates.
//
// Event streaming is automatically enabled internally - no configuration required!
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
//	go run ./examples/observability/console_ui
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go"
	"github.com/alob-mtc/runnerq-go/observability"
	"github.com/alob-mtc/runnerq-go/observability/ui"
	"github.com/alob-mtc/runnerq-go/storage/postgres"
)

// TestActivity simulates work.
type TestActivity struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *TestActivity) ActivityType() string {
	return "test_activity"
}

func (h *TestActivity) Handle(_ runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	fmt.Printf("Processing test activity: %s\n", string(payload))
	time.Sleep(5 * time.Second)
	fmt.Println("Completed test activity")
	result, _ := json.Marshal(map[string]interface{}{"status": "completed"})
	return result, nil
}

func main() {
	ctx := context.Background()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:runnerq@localhost:5432/runnerq"
	}

	backend, err := postgres.New(ctx, databaseURL, "my_app")
	if err != nil {
		log.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	engine, err := runnerq.Builder().
		Backend(backend).
		QueueName("my_app").
		MaxWorkers(3).
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
	fmt.Printf("RunnerQ Console: http://%s/console/\n", addr)
	fmt.Println("   Real-time updates enabled via SSE")
	fmt.Println("   Press Ctrl+C to stop")

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}
