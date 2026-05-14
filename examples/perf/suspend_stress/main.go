// Stress test for the parent-blocking-on-children starvation pattern that
// motivated the SuspendOnAwait engine flag.
//
// The pattern: each "root" activity spawns one leaf plus two "mid-parent"
// activities. Each mid-parent spawns FANOUT leaves and awaits them. The
// root awaits all three children. While a parent waits in GetResult it
// pins one worker slot. With ROOTS=N and a few workers per process, the
// classical fixed-goroutine pool saturates on suspended parents within a
// few seconds and the leaves can't dequeue → either work stalls until
// activity timeouts fire (→ DLQ growth) or it takes orders of magnitude
// longer to drain than the actual work warrants.
//
// SuspendOnAwait flips that: a parent in GetResult releases its slot so a
// leaf can dispatch immediately. Reserving a fraction of slots for leaf
// types (ReserveSlotsForLeaves) bounds the wake-up deadlock risk.
//
// ## Caveat: GetResult poll storm
//
// ActivityFuture.GetResult currently polls runnerq_results every 100ms. In
// suspend mode the number of concurrent waiters is no longer capped by the
// worker pool — hundreds of parents can be waiting at once, multiplying
// the poll rate proportionally. The DB connection pool has to be sized to
// absorb that traffic; this example does so automatically. The proper
// long-term fix is event-driven futures (subscribe to the existing event
// hub instead of polling), tracked separately.
//
// ## Each invocation uses a unique queue name
//
// To avoid one stalled run leaving rows behind that contaminate the next,
// the example defaults to suspend_stress_<short-uuid> per invocation. Set
// QUEUE_NAME if you want to share state across runs (debugging only).
//
// ## Running
//
//	# Baseline (suspend off): expect DLQ growth and slow drain.
//	SUSPEND=0 ROOTS=100 WORKERS=20 go run ./examples/perf/suspend_stress
//
//	# Suspend on, no reservation: usually drains; small risk of stalls.
//	SUSPEND=1 ROOTS=100 WORKERS=20 go run ./examples/perf/suspend_stress
//
//	# Suspend on with reservation: clean drain, no DLQ.
//	SUSPEND=1 LEAVES_RESERVED=4 ROOTS=100 WORKERS=20 go run ./examples/perf/suspend_stress
//
// The DATABASE_URL env var selects the Postgres instance (default:
// postgres://postgres:runnerq@localhost:5432/runnerq).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go"
	"github.com/alob-mtc/runnerq-go/observability"
	"github.com/alob-mtc/runnerq-go/storage/postgres"
)

// ─────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────

type config struct {
	roots          int
	workers        int
	suspend        bool
	leavesReserved int
	fanout         int
	workMS         int
	activityTOSec  int
	queueName      string
	drainTimeout   time.Duration
}

func loadConfig() config {
	// Default to a unique queue name per invocation so stale rows from a
	// previous stalled run don't contaminate the next one. Override with
	// QUEUE_NAME if you want to share state across runs (e.g. for debugging).
	defaultQueue := fmt.Sprintf("suspend_stress_%s", uuid.New().String()[:8])
	c := config{
		roots:          envInt("ROOTS", 100),
		workers:        envInt("WORKERS", 20),
		suspend:        envInt("SUSPEND", 0) != 0,
		leavesReserved: envInt("LEAVES_RESERVED", 0),
		fanout:         envInt("FANOUT", 2),
		workMS:         envInt("WORK_MS", 200),
		activityTOSec:  envInt("ACTIVITY_TIMEOUT_SEC", 120),
		queueName:      envStr("QUEUE_NAME", defaultQueue),
		drainTimeout:   time.Duration(envInt("DRAIN_TIMEOUT_SEC", 300)) * time.Second,
	}
	return c
}

func envInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("invalid %s=%q: %v", k, v, err)
	}
	return n
}

func envStr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// ─────────────────────────────────────────────────────────────────────────
// Activity types
//
// Tree shape (depth 2):
//
//	stress_root          (parent — awaits 3 children)
//	├─ stress_leaf       (leaf, FANOUT-1 instances)
//	└─ stress_mid × 2    (parent — awaits FANOUT leaves)
//	    └─ stress_leaf × FANOUT
//
// Per-root slot pressure while awaiting: 1 (root) + 2 (mids) = 3 slots
// held by suspended-or-running parents. With WORKERS=20 and ROOTS=100,
// after ~7 roots are in-flight every slot is a held parent. The leaves
// can't dequeue → starvation.
// ─────────────────────────────────────────────────────────────────────────

type stressRoot struct {
	runnerq.DefaultDeadLetterHandler
	cfg config
}

func (h *stressRoot) ActivityType() string { return "stress_root" }

func (h *stressRoot) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	// Sibling fan-out: one direct leaf plus two mid-parents.
	leafFut, err := spawn(ctx, "stress_leaf", payload, h.cfg)
	if err != nil {
		return nil, runnerq.NewRetryError(err.Error())
	}
	midFuts := make([]*runnerq.ActivityFuture, 0, 2)
	for range 2 {
		f, err := spawn(ctx, "stress_mid", payload, h.cfg)
		if err != nil {
			return nil, runnerq.NewRetryError(err.Error())
		}
		midFuts = append(midFuts, f)
	}
	if _, err := leafFut.GetResult(ctx.Ctx); err != nil {
		return nil, err
	}
	for _, f := range midFuts {
		if _, err := f.GetResult(ctx.Ctx); err != nil {
			return nil, err
		}
	}
	return json.RawMessage(`{"ok":true}`), nil
}

type stressMid struct {
	runnerq.DefaultDeadLetterHandler
	cfg config
}

func (h *stressMid) ActivityType() string { return "stress_mid" }

func (h *stressMid) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	futs := make([]*runnerq.ActivityFuture, 0, h.cfg.fanout)
	for i := 0; i < h.cfg.fanout; i++ {
		f, err := spawn(ctx, "stress_leaf", payload, h.cfg)
		if err != nil {
			return nil, runnerq.NewRetryError(err.Error())
		}
		futs = append(futs, f)
	}
	for _, f := range futs {
		if _, err := f.GetResult(ctx.Ctx); err != nil {
			return nil, err
		}
	}
	return json.RawMessage(`{"ok":true}`), nil
}

type stressLeaf struct {
	runnerq.DefaultDeadLetterHandler
	cfg     config
	counter *atomic.Int64
}

func (h *stressLeaf) ActivityType() string { return "stress_leaf" }

func (h *stressLeaf) Handle(ctx runnerq.ActivityContext, _ json.RawMessage) (json.RawMessage, error) {
	select {
	case <-time.After(time.Duration(h.cfg.workMS) * time.Millisecond):
	case <-ctx.Ctx.Done():
		return nil, ctx.Ctx.Err()
	}
	h.counter.Add(1)
	return json.RawMessage(`{"ok":true}`), nil
}

func spawn(ctx runnerq.ActivityContext, t string, payload json.RawMessage, cfg config) (*runnerq.ActivityFuture, error) {
	return ctx.ActivityExecutor.Activity(t).
		Payload(payload).
		Timeout(time.Duration(cfg.activityTOSec) * time.Second).
		MaxRetries(2).
		Execute(ctx.Ctx)
}

// ─────────────────────────────────────────────────────────────────────────
// Run loop: enqueue ROOTS, watch Stats() drain, print metrics, exit.
// ─────────────────────────────────────────────────────────────────────────

func main() {
	cfg := loadConfig()
	printConfig(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	databaseURL := envStr("DATABASE_URL", "postgres://postgres:runnerq@localhost:5432/runnerq")

	// Pool size has to absorb the GetResult poll storm in suspend mode:
	// every concurrently-suspended parent polls runnerq_results every
	// 100ms. With several hundred suspended parents that's thousands of
	// queries per second sharing the pool with Dequeue/Stats/Ack. Sized
	// generously here for the stress test; production pools should be
	// tuned to match expected concurrent-parent counts.
	poolSize := int32(cfg.workers + 30)
	if cfg.suspend {
		extra := max(cfg.roots/4, 30)
		poolSize = int32(cfg.workers + extra)
	}
	backend, err := postgres.WithConfig(ctx, databaseURL, cfg.queueName, 30_000, poolSize)
	if err != nil {
		log.Fatalf("backend: %v", err)
	}
	defer backend.Close()

	leafCounter := &atomic.Int64{}

	builder := runnerq.Builder().
		Backend(backend).
		QueueName(cfg.queueName).
		MaxWorkers(cfg.workers)
	if cfg.suspend {
		builder = builder.SuspendOnAwait(true)
		if cfg.leavesReserved > 0 {
			builder = builder.ReserveSlotsForLeaves(cfg.leavesReserved, []string{"stress_leaf"})
		}
	}
	engine, err := builder.Build()
	if err != nil {
		log.Fatalf("engine: %v", err)
	}

	engine.RegisterActivity("stress_root", &stressRoot{cfg: cfg})
	engine.RegisterActivity("stress_mid", &stressMid{cfg: cfg})
	engine.RegisterActivity("stress_leaf", &stressLeaf{cfg: cfg, counter: leafCounter})

	inspector := observability.NewQueueInspector(backend).WithMaxWorkers(cfg.workers)

	// Start engine.
	engineDone := make(chan struct{})
	go func() {
		defer close(engineDone)
		if err := engine.Start(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "engine.Start: %v\n", err)
		}
	}()

	// Baseline DLQ count.
	baselineStats, err := inspector.Stats(ctx)
	if err != nil {
		log.Fatalf("baseline stats: %v", err)
	}
	baselineDLQ := baselineStats.DeadLetterActivities

	// Burst-enqueue the roots.
	executor := engine.GetActivityExecutor()
	startEnqueue := time.Now()
	for i := 0; i < cfg.roots; i++ {
		payload, _ := json.Marshal(map[string]any{"i": i})
		if _, err := executor.Activity("stress_root").
			Payload(payload).
			Timeout(time.Duration(cfg.activityTOSec) * time.Second).
			MaxRetries(0).
			Execute(ctx); err != nil {
			log.Fatalf("enqueue root %d: %v", i, err)
		}
	}
	enqueueDur := time.Since(startEnqueue)
	fmt.Printf("\nEnqueued %d roots in %s\n", cfg.roots, enqueueDur.Round(time.Millisecond))

	// Watch the drain.
	stallStart := time.Now()
	drainStart := time.Now()
	var lastPending, lastRunning uint64
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()

	expectedLeaves := int64(cfg.roots * (1 + 2*cfg.fanout)) // 1 direct leaf + 2 mids × FANOUT leaves

	for {
		select {
		case <-tick.C:
			s, err := inspector.Stats(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "stats: %v\n", err)
				continue
			}

			inFlight := s.PendingActivities + s.ProcessingActivities + s.RetryingActivities + s.ScheduledActivities
			dlqDelta := s.DeadLetterActivities - baselineDLQ
			leavesDone := leafCounter.Load()

			fmt.Printf("[t=%s] pending=%d running=%d retry=%d dlq+%d leaves_done=%d/%d active_workers=%d/%d\n",
				time.Since(drainStart).Round(time.Second),
				s.PendingActivities, s.ProcessingActivities, s.RetryingActivities,
				dlqDelta,
				leavesDone, expectedLeaves,
				s.ActiveWorkers, deref(s.MaxWorkers, cfg.workers))

			if inFlight == 0 && leavesDone >= expectedLeaves {
				fmt.Printf("\nDrained in %s\n", time.Since(drainStart).Round(time.Second))
				printSummary(cfg, dlqDelta, time.Since(drainStart), leavesDone, expectedLeaves)
				engine.Stop()
				<-engineDone
				return
			}

			// Stall detector — if nothing moved for ~30s, we're starved.
			if s.PendingActivities == lastPending && s.ProcessingActivities == lastRunning {
				if time.Since(stallStart) > 30*time.Second && inFlight > 0 {
					fmt.Printf("\nSTALLED (no progress for 30s). Likely parent-await starvation.\n")
					printSummary(cfg, dlqDelta, time.Since(drainStart), leavesDone, expectedLeaves)
					engine.Stop()
					<-engineDone
					os.Exit(2)
				}
			} else {
				stallStart = time.Now()
				lastPending = s.PendingActivities
				lastRunning = s.ProcessingActivities
			}

			if time.Since(drainStart) > cfg.drainTimeout {
				fmt.Printf("\nDRAIN TIMEOUT after %s\n", cfg.drainTimeout)
				printSummary(cfg, dlqDelta, time.Since(drainStart), leavesDone, expectedLeaves)
				engine.Stop()
				<-engineDone
				os.Exit(3)
			}

		case <-ctx.Done():
			return
		}
	}
}

func printConfig(c config) {
	fmt.Printf("=== suspend_stress ===\n")
	fmt.Printf("roots=%d workers=%d suspend=%v leaves_reserved=%d fanout=%d work_ms=%d activity_timeout=%ds\n",
		c.roots, c.workers, c.suspend, c.leavesReserved, c.fanout, c.workMS, c.activityTOSec)
	fmt.Printf("queue=%s\n", c.queueName)

	totalLeaves := c.roots * (1 + 2*c.fanout)
	totalActivities := c.roots*3 + totalLeaves // root + 2 mids + leaves, per tree
	idealSec := float64(totalLeaves*c.workMS) / float64(c.workers) / 1000.0
	fmt.Printf("expected: %d total activities (%d leaves); ideal drain ≈ %.1fs at full leaf parallelism\n\n",
		totalActivities, totalLeaves, idealSec)

	// The goal is for actual drain time to approach the ideal. With suspend
	// off and workers small, expect orders of magnitude longer due to
	// slot-pinning by suspended parents — often a stall.
}

func printSummary(c config, dlqDelta uint64, elapsed time.Duration, leavesDone, expected int64) {
	fmt.Printf("\n=== summary ===\n")
	fmt.Printf("config: roots=%d workers=%d suspend=%v leaves_reserved=%d\n",
		c.roots, c.workers, c.suspend, c.leavesReserved)
	fmt.Printf("elapsed: %s\n", elapsed.Round(time.Second))
	fmt.Printf("leaves_done: %d / %d\n", leavesDone, expected)
	fmt.Printf("dlq_delta: +%d\n", dlqDelta)
}

func deref(p *int, fallback int) int {
	if p == nil {
		return fallback
	}
	return *p
}
