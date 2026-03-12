// Example: Worker-level activity type filtering
//
// Demonstrates how to run multiple worker engines on the same queue where each
// engine only processes specific activity types. This enables workload isolation
// without separate queues or binaries.
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
//	go run ./examples/advanced/activity_filtering
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/alob-mtc/runnerq-go"
	"github.com/alob-mtc/runnerq-go/storage/postgres"
)

type EmailHandler struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *EmailHandler) ActivityType() string { return "send_email" }

func (h *EmailHandler) Handle(_ runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	slog.Info("[email-node] sending email", "payload", string(payload))
	time.Sleep(500 * time.Millisecond)
	result, _ := json.Marshal(map[string]any{"sent": true})
	return result, nil
}

type SmsHandler struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *SmsHandler) ActivityType() string { return "send_sms" }

func (h *SmsHandler) Handle(_ runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	slog.Info("[sms] sending SMS", "payload", string(payload))
	time.Sleep(300 * time.Millisecond)
	result, _ := json.Marshal(map[string]any{"sent": true})
	return result, nil
}

type TradeHandler struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *TradeHandler) ActivityType() string { return "execute_trade" }

func (h *TradeHandler) Handle(_ runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	slog.Info("[trade-node] executing trade", "payload", string(payload))
	time.Sleep(200 * time.Millisecond)
	result, _ := json.Marshal(map[string]any{"executed": true})
	return result, nil
}

type ReportHandler struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *ReportHandler) ActivityType() string { return "generate_report" }

func (h *ReportHandler) Handle(_ runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	slog.Info("[catch-all] generating report", "payload", string(payload))
	time.Sleep(1 * time.Second)
	result, _ := json.Marshal(map[string]any{"generated": true})
	return result, nil
}

// allHandlers returns every handler defined in this example.
func allHandlers() []runnerq.ActivityHandler {
	return []runnerq.ActivityHandler{
		&EmailHandler{},
		&SmsHandler{},
		&TradeHandler{},
		&ReportHandler{},
	}
}

// buildEngine creates a WorkerEngine with the given activity_types filter.
// It registers only the handlers that match the filter (or all if filter is nil).
func buildEngine(backend *postgres.PostgresBackend, activityTypes []string, label string) *runnerq.WorkerEngine {
	builder := runnerq.Builder().
		Backend(backend).
		QueueName("filtering_example").
		MaxWorkers(2)

	if activityTypes != nil {
		builder = builder.ActivityTypes(activityTypes)
	}

	engine, err := builder.Build()
	if err != nil {
		log.Fatalf("failed to build engine [%s]: %v", label, err)
	}

	typeSet := make(map[string]struct{})
	for _, t := range activityTypes {
		typeSet[t] = struct{}{}
	}

	for _, handler := range allHandlers() {
		if activityTypes == nil {
			engine.RegisterActivity(handler.ActivityType(), handler)
		} else if _, ok := typeSet[handler.ActivityType()]; ok {
			engine.RegisterActivity(handler.ActivityType(), handler)
		}
	}

	slog.Info("Engine ready", "label", label, "filter", activityTypes)
	return engine
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx := context.Background()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:runnerq@localhost:5432/runnerq"
	}

	backend, err := postgres.New(ctx, databaseURL, "filtering_example")
	if err != nil {
		log.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	slog.Info("PostgreSQL backend initialised")

	// Node 1: emails only
	emailEngine := buildEngine(backend, []string{"send_email", "send_sms"}, "email-node")

	// Node 2: trades only
	tradeEngine := buildEngine(backend, []string{"execute_trade"}, "trade-node")

	// Node 3: catch-all (no filter)
	catchallEngine := buildEngine(backend, nil, "catch-all")

	executor := catchallEngine.GetActivityExecutor()

	errCh := make(chan error, 3)
	go func() { errCh <- emailEngine.Start(ctx) }()
	go func() { errCh <- tradeEngine.Start(ctx) }()
	go func() { errCh <- catchallEngine.Start(ctx) }()

	// Enqueue a mix of activity types
	go func() {
		time.Sleep(2 * time.Second)

		types := []string{"send_email", "send_sms", "execute_trade", "generate_report"}
		for i := range 12 {
			activityType := types[i%len(types)]
			slog.Info("Enqueueing", "type", activityType, "seq", i+1)

			payload, _ := json.Marshal(map[string]any{"seq": i + 1})
			_, err := executor.Activity(activityType).
				Payload(payload).
				Execute(ctx)
			if err != nil {
				log.Fatalf("enqueue failed: %v", err)
			}
			time.Sleep(500 * time.Millisecond)
		}

		slog.Info("All activities enqueued. Watch the logs to see each engine claim only its types.")
		time.Sleep(10 * time.Second)
		os.Exit(0)
	}()

	fmt.Println()
	fmt.Println("=== Activity Type Filtering Example ===")
	fmt.Println("  email-node  : send_email, send_sms")
	fmt.Println("  trade-node  : execute_trade")
	fmt.Println("  catch-all   : everything (including generate_report)")
	fmt.Println("  Press Ctrl+C to stop")
	fmt.Println()

	if err := <-errCh; err != nil {
		log.Fatalf("Engine error: %v", err)
	}
}
