// Example showing multi-level parent/child activity lineage in the console.
//
// A root "research_workflow" spawns three children which themselves spawn
// grandchildren. One side-effect "audit_log" activity uses .AsRoot() to
// detach from the parent. After running for a few seconds you'll have a
// 3-level tree in the console — Gantt and Graph tabs show the structure.
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
//	go run ./examples/observability/lineage_tree
//
// Then open http://localhost:8081/console/ and click into a recent run.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/alob-mtc/runnerq-go"
	"github.com/alob-mtc/runnerq-go/observability"
	"github.com/alob-mtc/runnerq-go/observability/ui"
	"github.com/alob-mtc/runnerq-go/storage/postgres"
)

// ─────────────────────────────────────────────────────────────────────────
// Root: research_workflow
//   ├─ fetch_documents
//   ├─ analyze_data
//   │    ├─ extract_facts
//   │    └─ verify_sources
//   ├─ draft_report
//   │    ├─ outline
//   │    └─ write_intro
//   └─ audit_log (.AsRoot — detached, becomes its own root)
// ─────────────────────────────────────────────────────────────────────────

type ResearchWorkflow struct{ runnerq.DefaultDeadLetterHandler }

func (h *ResearchWorkflow) ActivityType() string { return "research_workflow" }

func (h *ResearchWorkflow) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	fmt.Printf("[root] research_workflow %s starting\n", ctx.ActivityID)
	work(800)

	// Spawn fetch_documents (await result)
	fetchFut, err := spawn(ctx, "fetch_documents", payload)
	if err != nil {
		return nil, runnerq.NewRetryError(err.Error())
	}

	// Spawn analyze_data and draft_report in parallel (don't await yet)
	analyzeFut, err := spawn(ctx, "analyze_data", payload)
	if err != nil {
		return nil, runnerq.NewRetryError(err.Error())
	}
	draftFut, err := spawn(ctx, "draft_report", payload)
	if err != nil {
		return nil, runnerq.NewRetryError(err.Error())
	}

	// Fire-and-forget audit log, detached from this lineage tree.
	auditPayload, _ := json.Marshal(map[string]any{"event": "research_started", "trigger": ctx.ActivityID.String()})
	if _, err := ctx.ActivityExecutor.Activity("audit_log").
		Payload(auditPayload).
		AsRoot().
		Execute(ctx.Ctx); err != nil {
		fmt.Printf("[root] audit_log failed: %v\n", err)
	}

	// Await all three children
	awaitCtx, cancel := context.WithTimeout(ctx.Ctx, 90*time.Second)
	defer cancel()
	if _, err := fetchFut.GetResult(awaitCtx); err != nil {
		fmt.Printf("[root] fetch_documents failed: %v\n", err)
	}
	if _, err := analyzeFut.GetResult(awaitCtx); err != nil {
		fmt.Printf("[root] analyze_data failed: %v\n", err)
	}
	if _, err := draftFut.GetResult(awaitCtx); err != nil {
		fmt.Printf("[root] draft_report failed: %v\n", err)
	}

	work(400)
	fmt.Printf("[root] research_workflow %s done\n", ctx.ActivityID)
	return json.RawMessage(`{"status":"ok","artifacts":3}`), nil
}

// ─────────────────────────────────────────────────────────────────────────

type FetchDocuments struct{ runnerq.DefaultDeadLetterHandler }

func (h *FetchDocuments) ActivityType() string { return "fetch_documents" }

func (h *FetchDocuments) Handle(ctx runnerq.ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
	fmt.Printf("  [d=%d] fetch_documents %s\n", ctx.Depth, ctx.ActivityID)
	work(2000 + rand.Intn(1500))
	return json.RawMessage(`{"docs":42}`), nil
}

// ─────────────────────────────────────────────────────────────────────────

type AnalyzeData struct{ runnerq.DefaultDeadLetterHandler }

func (h *AnalyzeData) ActivityType() string { return "analyze_data" }

func (h *AnalyzeData) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	fmt.Printf("  [d=%d] analyze_data %s\n", ctx.Depth, ctx.ActivityID)
	work(900)

	extractFut, err := spawn(ctx, "extract_facts", payload)
	if err != nil {
		return nil, runnerq.NewRetryError(err.Error())
	}
	verifyFut, err := spawn(ctx, "verify_sources", payload)
	if err != nil {
		return nil, runnerq.NewRetryError(err.Error())
	}

	awaitCtx, cancel := context.WithTimeout(ctx.Ctx, 60*time.Second)
	defer cancel()
	_, _ = extractFut.GetResult(awaitCtx)
	_, _ = verifyFut.GetResult(awaitCtx)

	work(500)
	return json.RawMessage(`{"facts":12,"verified":11}`), nil
}

// ─────────────────────────────────────────────────────────────────────────

type ExtractFacts struct{ runnerq.DefaultDeadLetterHandler }

func (h *ExtractFacts) ActivityType() string { return "extract_facts" }

func (h *ExtractFacts) Handle(ctx runnerq.ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
	fmt.Printf("    [d=%d] extract_facts %s\n", ctx.Depth, ctx.ActivityID)
	work(1500 + rand.Intn(1500))
	return json.RawMessage(`{"facts":12}`), nil
}

// ─────────────────────────────────────────────────────────────────────────

// VerifySources sometimes fails on the first attempt to demonstrate retries.
type VerifySources struct {
	runnerq.DefaultDeadLetterHandler
	attempts atomic.Int64
}

func (h *VerifySources) ActivityType() string { return "verify_sources" }

func (h *VerifySources) Handle(ctx runnerq.ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
	fmt.Printf("    [d=%d] verify_sources %s (retry=%d)\n", ctx.Depth, ctx.ActivityID, ctx.RetryCount)
	work(800 + rand.Intn(800))
	if ctx.RetryCount == 0 && h.attempts.Add(1)%3 == 0 {
		return nil, runnerq.NewRetryError("transient: source unreachable")
	}
	return json.RawMessage(`{"verified":11}`), nil
}

// ─────────────────────────────────────────────────────────────────────────

type DraftReport struct{ runnerq.DefaultDeadLetterHandler }

func (h *DraftReport) ActivityType() string { return "draft_report" }

func (h *DraftReport) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	fmt.Printf("  [d=%d] draft_report %s\n", ctx.Depth, ctx.ActivityID)
	work(700)

	outlineFut, err := spawn(ctx, "outline", payload)
	if err != nil {
		return nil, runnerq.NewRetryError(err.Error())
	}
	introFut, err := spawn(ctx, "write_intro", payload)
	if err != nil {
		return nil, runnerq.NewRetryError(err.Error())
	}

	awaitCtx, cancel := context.WithTimeout(ctx.Ctx, 60*time.Second)
	defer cancel()
	_, _ = outlineFut.GetResult(awaitCtx)
	_, _ = introFut.GetResult(awaitCtx)

	work(600)
	return json.RawMessage(`{"draft":"v1"}`), nil
}

// ─────────────────────────────────────────────────────────────────────────

type Outline struct{ runnerq.DefaultDeadLetterHandler }

func (h *Outline) ActivityType() string { return "outline" }

func (h *Outline) Handle(ctx runnerq.ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
	fmt.Printf("    [d=%d] outline %s\n", ctx.Depth, ctx.ActivityID)
	work(1200 + rand.Intn(800))
	return json.RawMessage(`{"sections":5}`), nil
}

// ─────────────────────────────────────────────────────────────────────────

type WriteIntro struct{ runnerq.DefaultDeadLetterHandler }

func (h *WriteIntro) ActivityType() string { return "write_intro" }

func (h *WriteIntro) Handle(ctx runnerq.ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
	fmt.Printf("    [d=%d] write_intro %s\n", ctx.Depth, ctx.ActivityID)
	work(2200 + rand.Intn(1200))
	return json.RawMessage(`{"words":348}`), nil
}

// ─────────────────────────────────────────────────────────────────────────

type AuditLog struct{ runnerq.DefaultDeadLetterHandler }

func (h *AuditLog) ActivityType() string { return "audit_log" }

func (h *AuditLog) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	fmt.Printf("[detached d=%d] audit_log %s payload=%s\n", ctx.Depth, ctx.ActivityID, string(payload))
	work(300)
	return json.RawMessage(`{"logged":true}`), nil
}

// ─────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────

func spawn(ctx runnerq.ActivityContext, activityType string, payload json.RawMessage) (*runnerq.ActivityFuture, error) {
	return ctx.ActivityExecutor.Activity(activityType).
		Payload(payload).
		Timeout(60 * time.Second).
		MaxRetries(2).
		Execute(ctx.Ctx)
}

func work(maxMs int) {
	time.Sleep(time.Duration(maxMs) * time.Millisecond)
}

// ─────────────────────────────────────────────────────────────────────────

func main() {
	ctx := context.Background()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:runnerq@localhost:5432/runnerq"
	}

	backend, err := postgres.New(ctx, databaseURL, "lineage_demo")
	if err != nil {
		log.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	engine, err := runnerq.Builder().
		Backend(backend).
		QueueName("lineage_demo").
		MaxWorkers(8).
		Build()
	if err != nil {
		log.Fatalf("Failed to build engine: %v", err)
	}

	engine.RegisterActivity("research_workflow", &ResearchWorkflow{})
	engine.RegisterActivity("fetch_documents", &FetchDocuments{})
	engine.RegisterActivity("analyze_data", &AnalyzeData{})
	engine.RegisterActivity("extract_facts", &ExtractFacts{})
	engine.RegisterActivity("verify_sources", &VerifySources{})
	engine.RegisterActivity("draft_report", &DraftReport{})
	engine.RegisterActivity("outline", &Outline{})
	engine.RegisterActivity("write_intro", &WriteIntro{})
	engine.RegisterActivity("audit_log", &AuditLog{})

	inspector := observability.NewQueueInspector(backend).WithMaxWorkers(engine.MaxConcurrentActivities())

	executor := engine.GetActivityExecutor()

	go func() {
		fmt.Println("Worker engine starting (8 workers)...")
		if err := engine.Start(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Worker engine error: %v\n", err)
		}
	}()

	// Kick off a new root research_workflow every 20 seconds.
	go func() {
		fmt.Println("Waiting 3 seconds before first run...")
		time.Sleep(3 * time.Second)

		counter := 1
		for {
			payload, _ := json.Marshal(map[string]any{
				"topic":     fmt.Sprintf("Q%d quarterly review", counter),
				"depth":     "deep",
				"timestamp": time.Now().UTC().Format(time.RFC3339),
			})
			_, err := executor.Activity("research_workflow").
				Payload(payload).
				Timeout(2 * time.Minute).
				MaxRetries(1).
				Execute(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to enqueue root: %v\n", err)
			} else {
				fmt.Printf("Root research_workflow #%d enqueued\n\n", counter)
			}
			counter++
			time.Sleep(20 * time.Second)
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("/console/", http.StripPrefix("/console", ui.RunnerQUI(inspector)))

	addr := "0.0.0.0:8081"
	fmt.Printf("RunnerQ Console: http://%s/console/\n", addr)
	fmt.Println("   Click into a 'research_workflow' run to see the lineage tree.")
	fmt.Println("   Press Ctrl+C to stop")

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}
