# RunnerQ (Go)

A robust, scalable activity queue and worker system for Go applications with pluggable storage backends.

## Features

- **Pluggable backend system** - Interface-based storage abstraction; PostgreSQL is built-in
- **Priority-based activity processing** - Support for Critical, High, Normal, and Low priority levels
- **Activity scheduling** - Precise timestamp-based scheduling for future execution
- **Intelligent retry mechanism** - Built-in retry mechanism with exponential backoff
- **Dead letter queue** - Failed activities are moved to a dead letter queue for inspection
- **Concurrent activity processing** - Configurable number of concurrent workers (goroutines)
- **Graceful shutdown** - Proper shutdown handling with OS signal support
- **Activity orchestration** - Activities can execute other activities for complex workflows
- **Comprehensive error handling** - Retryable and non-retryable error types
- **Activity metadata** - Support for custom metadata on activities
- **Built-in observability console** - Real-time web UI for monitoring and managing activities
- **Worker-level activity type filtering** - Isolate workloads by restricting each engine to specific activity types
- **Queue statistics** - Monitoring capabilities and metrics collection

## Storage Backends

| Backend | Status | Use Case |
|---------|--------|----------|
| **PostgreSQL** | Built-in | Default. Permanent persistence, SQL-based queries |
| **Custom** | Supported | Implement the `Storage` interface for your own backend |

## Installation

```sh
go get github.com/alob-mtc/runnerq-go
```

## Quick Start

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/alob-mtc/runnerq-go"
	"github.com/alob-mtc/runnerq-go/storage/postgres"
)

// SendEmailHandler implements activity processing for emails.
type SendEmailHandler struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *SendEmailHandler) ActivityType() string {
	return "send_email"
}

func (h *SendEmailHandler) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	var data map[string]interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, runnerq.NewNonRetryError("Invalid payload format")
	}

	to, ok := data["to"].(string)
	if !ok || to == "" {
		return nil, runnerq.NewNonRetryError("Missing 'to' field")
	}

	fmt.Printf("Sending email to: %s\n", to)

	result, _ := json.Marshal(map[string]interface{}{
		"message": fmt.Sprintf("Email sent to %s", to),
		"status":  "delivered",
	})
	return result, nil
}

func main() {
	ctx := context.Background()

	backend, err := postgres.New(ctx, "postgres://localhost/mydb", "my_app")
	if err != nil {
		log.Fatal(err)
	}
	defer backend.Close()

	engine, err := runnerq.Builder().
		Backend(backend).
		QueueName("my_app").
		MaxWorkers(8).
		SchedulePollInterval(30 * time.Second).
		Build()
	if err != nil {
		log.Fatal(err)
	}

	engine.RegisterActivity("send_email", &SendEmailHandler{})

	executor := engine.GetActivityExecutor()

	// Execute an activity with custom options
	emailPayload, _ := json.Marshal(map[string]interface{}{
		"to":      "user@example.com",
		"subject": "Welcome!",
	})
	future, err := executor.Activity("send_email").
		Payload(emailPayload).
		MaxRetries(5).
		Timeout(10 * time.Minute).
		Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Schedule an activity for future execution (10 seconds from now)
	scheduledPayload, _ := json.Marshal(map[string]interface{}{
		"to":      "user@example.com",
		"subject": "Reminder",
	})
	_, err = executor.Activity("send_email").
		Payload(scheduledPayload).
		MaxRetries(3).
		Timeout(5 * time.Minute).
		Delay(10 * time.Second).
		Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Tag a cron-triggered activity (useful for observability filtering)
	cronPayload, _ := json.Marshal(map[string]interface{}{
		"job": "nightly_reconcile",
	})
	_, err = executor.Activity("reconcile_accounts").
		Payload(cronPayload).
		Metadata("source", "cron").
		Metadata("cron_job", "nightly_reconcile").
		Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Execute an activity with default options
	simplePayload, _ := json.Marshal(map[string]interface{}{
		"to": "admin@example.com",
	})
	_, err = executor.Activity("send_email").
		Payload(simplePayload).
		Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Wait for the email result in a goroutine
	go func() {
		resultCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		result, err := future.GetResult(resultCtx)
		if err != nil {
			fmt.Printf("Failed to get result: %v\n", err)
			return
		}
		fmt.Printf("Email result: %s\n", string(result))
	}()

	// Start the worker engine (blocks until shutdown signal or context cancellation)
	if err := engine.Start(ctx); err != nil {
		log.Fatal(err)
	}
}
```

## Builder Pattern API

RunnerQ provides a fluent builder pattern for both `WorkerEngine` configuration and activity execution.

### WorkerEngine Builder

```go
import (
	"time"

	"github.com/alob-mtc/runnerq-go"
	"github.com/alob-mtc/runnerq-go/storage/postgres"
)

// Basic configuration
engine, err := runnerq.Builder().
	Backend(backend).
	QueueName("my_app").
	MaxWorkers(8).
	SchedulePollInterval(30 * time.Second).
	Build()

// With custom metrics
engine, err := runnerq.Builder().
	Backend(backend).
	QueueName("my_app").
	MaxWorkers(8).
	Metrics(&PrometheusMetrics{}).
	Build()

// Restrict this engine to specific activity types (workload isolation)
engine, err := runnerq.Builder().
	Backend(backend).
	QueueName("my_app").
	ActivityTypes([]string{"send_email", "send_sms"}).
	Build()
```

### Activity Builder

```go
executor := engine.GetActivityExecutor()

// Fluent activity execution
payload, _ := json.Marshal(map[string]interface{}{
	"to":      "user@example.com",
	"subject": "Hello",
})
future, err := executor.Activity("send_email").
	Payload(payload).
	MaxRetries(5).
	Timeout(10 * time.Minute).
	Execute(ctx)

// Schedule activity for future execution
scheduledPayload, _ := json.Marshal(map[string]interface{}{"user_id": 123})
future, err := executor.Activity("send_reminder").
	Payload(scheduledPayload).
	Delay(1 * time.Hour).
	Execute(ctx)

// Attach metadata (for correlation, routing, or cron tagging)
future, err := executor.Activity("reconcile_accounts").
	Payload(payload).
	Metadata("source", "cron").
	MetadataMap(map[string]string{
		"cron_job": "nightly_reconcile",
		"tenant":   "acme",
	}).
	Execute(ctx)

// Simple activity with defaults
simplePayload, _ := json.Marshal(map[string]interface{}{"data": "example"})
future, err := executor.Activity("process_data").
	Payload(simplePayload).
	Execute(ctx)
```

## Activity Types

Activity types in RunnerQ are simple strings that identify different types of activities.

### Examples

```go
"send_email"
"process_payment"
"provision_card"
"update_card_status"
"process_webhook_event"

"user.registration"
"email-notification"
"background_sync"
```

## Custom Activity Handlers

Create custom activity handlers by implementing the `ActivityHandler` interface:

```go
import (
	"encoding/json"
	"fmt"

	"github.com/alob-mtc/runnerq-go"
)

type PaymentHandler struct {
	runnerq.DefaultDeadLetterHandler
	// Add your dependencies here (database connections, external APIs, etc.)
}

func (h *PaymentHandler) ActivityType() string {
	return "process_payment"
}

func (h *PaymentHandler) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	var data map[string]interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, runnerq.NewNonRetryError("Invalid payload format")
	}

	amount, _ := data["amount"].(float64)
	currency, _ := data["currency"].(string)
	if currency == "" {
		currency = "USD"
	}

	fmt.Printf("Processing payment: %.2f %s\n", amount, currency)

	if amount <= 0 {
		return nil, runnerq.NewNonRetryError("Invalid amount")
	}

	result, _ := json.Marshal(map[string]interface{}{
		"transaction_id": "txn_123456",
		"amount":         amount,
		"currency":       currency,
		"status":         "completed",
	})
	return result, nil
}

// Register the handler
engine.RegisterActivity("process_payment", &PaymentHandler{})
```

## Activity Priority and Options

Activities can be configured with priority and execution parameters:

```go
// High priority with custom retry and timeout settings
payload, _ := json.Marshal(map[string]interface{}{"to": "user@example.com"})
future, err := executor.Activity("send_email").
	Payload(payload).
	Priority(runnerq.PriorityCritical). // Highest priority
	MaxRetries(10).                      // Retry up to 10 times
	Timeout(15 * time.Minute).           // 15 minute timeout
	Execute(ctx)

// Default options (Normal priority, 3 retries, 300s timeout)
future, err := executor.Activity("send_email").
	Payload(payload).
	Execute(ctx)
```

Available priorities:
- `runnerq.PriorityCritical` - Highest priority (processed first)
- `runnerq.PriorityHigh` - High priority
- `runnerq.PriorityNormal` - Default priority
- `runnerq.PriorityLow` - Lowest priority

## Getting Activity Results

Activities can return results that can be retrieved asynchronously:

```go
future, err := executor.Activity("send_email").
	Payload(payload).
	Execute(ctx)
if err != nil {
	log.Fatal(err)
}

// Get the result (this will poll until the activity completes or ctx is cancelled)
resultCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
defer cancel()

result, err := future.GetResult(resultCtx)
if err != nil {
	log.Printf("Failed: %v", err)
	return
}

var emailResult struct {
	Message string `json:"message"`
	Status  string `json:"status"`
}
json.Unmarshal(result, &emailResult)
fmt.Printf("Email result: %+v\n", emailResult)
```

## Activity Orchestration

Activities can execute other activities using the `ActivityExecutor` available in the `ActivityContext`. This enables powerful workflow orchestration:

```go
type OrderProcessingHandler struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *OrderProcessingHandler) ActivityType() string {
	return "process_order"
}

func (h *OrderProcessingHandler) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	var data map[string]interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, runnerq.NewNonRetryError("Invalid payload")
	}

	orderID, _ := data["order_id"].(string)

	// Step 1: Validate payment using the fluent API
	paymentPayload, _ := json.Marshal(map[string]interface{}{"order_id": orderID})
	_, err := ctx.ActivityExecutor.Activity("validate_payment").
		Payload(paymentPayload).
		Priority(runnerq.PriorityHigh).
		MaxRetries(3).
		Timeout(2 * time.Minute).
		Execute(ctx.Ctx)
	if err != nil {
		return nil, runnerq.NewRetryError(fmt.Sprintf("Failed to enqueue payment validation: %v", err))
	}

	// Step 2: Update inventory
	inventoryPayload, _ := json.Marshal(map[string]interface{}{"order_id": orderID})
	_, err = ctx.ActivityExecutor.Activity("update_inventory").
		Payload(inventoryPayload).
		Execute(ctx.Ctx)
	if err != nil {
		return nil, runnerq.NewRetryError(fmt.Sprintf("Failed to enqueue inventory update: %v", err))
	}

	// Step 3: Schedule delivery notification for later
	notifPayload, _ := json.Marshal(map[string]interface{}{
		"order_id":       orderID,
		"customer_email": data["customer_email"],
	})
	_, err = ctx.ActivityExecutor.Activity("send_delivery_notification").
		Payload(notifPayload).
		Priority(runnerq.PriorityNormal).
		MaxRetries(5).
		Timeout(5 * time.Minute).
		Delay(1 * time.Hour).
		Execute(ctx.Ctx)
	if err != nil {
		return nil, runnerq.NewRetryError(fmt.Sprintf("Failed to schedule notification: %v", err))
	}

	result, _ := json.Marshal(map[string]interface{}{
		"order_id":        orderID,
		"status":          "processing",
		"steps_initiated": []string{"payment_validation", "inventory_update", "delivery_notification"},
	})
	return result, nil
}
```

### Benefits of Activity Orchestration

- **Modularity**: Break complex workflows into smaller, reusable activities
- **Reliability**: Each sub-activity has its own retry logic and error handling
- **Monitoring**: Track progress of individual workflow steps
- **Scalability**: Sub-activities can be processed by different workers
- **Flexibility**: Different priority levels and timeouts for different steps
- **Scheduling**: Schedule activities for future execution
- **Fluent API**: Clean, readable activity execution with method chaining

## Metrics and Monitoring

RunnerQ provides comprehensive metrics collection through the `MetricsSink` interface, allowing you to integrate with your preferred monitoring system.

### Basic Metrics Implementation

```go
import (
	"fmt"
	"time"

	"github.com/alob-mtc/runnerq-go"
)

type LoggingMetrics struct{}

func (m *LoggingMetrics) IncCounter(name string, value uint64) {
	fmt.Printf("METRIC: %s += %d\n", name, value)
}

func (m *LoggingMetrics) ObserveDuration(name string, dur time.Duration) {
	fmt.Printf("METRIC: %s = %v\n", name, dur)
}

// Use with WorkerEngine
engine, err := runnerq.Builder().
	Backend(backend).
	QueueName("my_app").
	Metrics(&LoggingMetrics{}).
	Build()
```

### Available Metrics

The library automatically collects the following metrics:

- **`activity_completed`** - Number of activities completed successfully
- **`activity_retry`** - Number of activities that requested retry
- **`activity_failed_non_retry`** - Number of activities that failed permanently
- **`activity_timeout`** - Number of activities that timed out

### No-op Metrics

If you don't need metrics collection, the built-in `NoopMetrics` is used by default:

```go
var metrics runnerq.NoopMetrics

// These calls do nothing
metrics.IncCounter("activities_completed", 1)
metrics.ObserveDuration("activity_execution", 5*time.Second)
```

## Advanced Features

### Activity Scheduling

RunnerQ supports scheduling activities for future execution:

```go
executor := engine.GetActivityExecutor()

// Schedule an activity to run in 1 hour
payload, _ := json.Marshal(map[string]interface{}{
	"user_id": 123,
	"message": "Don't forget!",
})
future, err := executor.Activity("send_reminder").
	Payload(payload).
	Delay(1 * time.Hour).
	Execute(ctx)

// Schedule for 2 hours from now
reportPayload, _ := json.Marshal(map[string]interface{}{"report_type": "monthly"})
future, err := executor.Activity("process_report").
	Payload(reportPayload).
	Delay(2 * time.Hour).
	Execute(ctx)
```

### Workload Isolation (Activity Type Filtering)

By default every worker engine dequeues all activity types from the queue. When you need to isolate workloads — for example, keeping slow report-generation jobs from starving latency-sensitive email sends — you can restrict each engine to specific activity types with `.ActivityTypes()`:

```go
import (
	"github.com/alob-mtc/runnerq-go"
	"github.com/alob-mtc/runnerq-go/storage/postgres"
)

backend, err := postgres.New(ctx, "postgres://localhost/runnerq", "my_app")

// Node 1 — only processes email-related activities
emailEngine, err := runnerq.Builder().
	Backend(backend).
	ActivityTypes([]string{"send_email", "send_sms"}).
	MaxWorkers(4).
	Build()
emailEngine.RegisterActivity("send_email", &SendEmailHandler{})
emailEngine.RegisterActivity("send_sms", &SendSmsHandler{})

// Node 2 — only processes trades
tradeEngine, err := runnerq.Builder().
	Backend(backend).
	ActivityTypes([]string{"execute_trade"}).
	MaxWorkers(8).
	Build()
tradeEngine.RegisterActivity("execute_trade", &TradeHandler{})

// Node 3 — catch-all, processes anything not claimed above
catchallEngine, err := runnerq.Builder().
	Backend(backend).
	MaxWorkers(2).
	Build()
// Register all handlers on the catch-all node
```

All engines share the same backend and queue. Each engine's `Dequeue()` only claims activities matching its declared types; an engine with no filter acts as a catch-all.

**Startup validation:** If `ActivityTypes` is set and any listed type does not have a registered handler, the engine panics at `Start()` with a clear error message.

See `examples/advanced/activity_filtering/` for a complete working example.

### Pluggable Storage Backends

RunnerQ uses an interface-based storage abstraction that allows you to swap out the persistence layer.

#### PostgreSQL Backend

```go
import "github.com/alob-mtc/runnerq-go/storage/postgres"

backend, err := postgres.New(ctx, "postgres://user:password@localhost/runnerq", "my_queue")
if err != nil {
	log.Fatal(err)
}
defer backend.Close()

engine, err := runnerq.Builder().
	Backend(backend).
	MaxWorkers(8).
	Build()
```

**PostgreSQL Backend Features:**
- **Permanent Persistence** - Activities stored indefinitely (no TTL expiration)
- **Multi-node Safe** - Uses `FOR UPDATE SKIP LOCKED` for concurrent job claiming
- **Cross-process Events** - PostgreSQL `LISTEN/NOTIFY` for real-time event streaming
- **Atomic Idempotency** - Separate table with `INSERT ... ON CONFLICT` for race-safe key claiming
- **History Preservation** - Never deletes activity records

**Schema Tables Created:**
- `runnerq_activities` - Main activity storage
- `runnerq_events` - Event history timeline
- `runnerq_results` - Activity execution results
- `runnerq_idempotency` - Idempotency key mapping

See `examples/basic/` for a complete working example, and `examples/advanced/activity_filtering/` for workload isolation with multiple engines.

#### Implementing a Custom Backend

You can implement your own backend by implementing the `Storage` interface (which combines `QueueStorage` and `InspectionStorage`):

```go
import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
)

type MyCustomBackend struct {
	// Your backend state (connection pool, config, etc.)
}

// QueueStorage methods

func (b *MyCustomBackend) Enqueue(ctx context.Context, activity storage.QueuedActivity) error {
	// Implement activity enqueuing
	panic("not implemented")
}

func (b *MyCustomBackend) Dequeue(ctx context.Context, workerID string, timeout time.Duration, activityTypes []string) (*storage.QueuedActivity, error) {
	// Implement activity claiming.
	// When activityTypes is non-nil, only claim matching types.
	panic("not implemented")
}

func (b *MyCustomBackend) AckSuccess(ctx context.Context, activityID uuid.UUID, result json.RawMessage, workerID string) error {
	// Mark activity as completed
	panic("not implemented")
}

func (b *MyCustomBackend) AckFailure(ctx context.Context, activityID uuid.UUID, failure storage.FailureKind, workerID string) (bool, error) {
	// Handle activity failure (retry or dead-letter). Returns true if dead-lettered.
	panic("not implemented")
}

// ... implement remaining QueueStorage methods:
//   ProcessScheduled, RequeueExpired, ExtendLease,
//   StoreResult, GetResult, CheckIdempotency, SchedulesNatively

// InspectionStorage methods

func (b *MyCustomBackend) Stats(ctx context.Context) (*storage.QueueStats, error) {
	panic("not implemented")
}

func (b *MyCustomBackend) ListPending(ctx context.Context, offset, limit int) ([]storage.ActivitySnapshot, error) {
	panic("not implemented")
}

// ... implement remaining InspectionStorage methods:
//   ListProcessing, ListScheduled, ListCompleted, ListDeadLetter,
//   GetActivity, GetActivityEvents, EventStream

// Use your custom backend
engine, err := runnerq.Builder().
	Backend(&MyCustomBackend{}).
	MaxWorkers(8).
	Build()
```

#### Storage Interface Reference

The storage abstraction consists of two interfaces:

**`QueueStorage`** - Core queue operations:
- `Enqueue()` - Add activity to the queue
- `Dequeue()` - Claim an activity for processing (PostgreSQL picks up due scheduled/retrying activities directly here)
- `AckSuccess()` - Mark activity as completed
- `AckFailure()` - Handle activity failure (retry or dead-letter)
- `ProcessScheduled()` - Move due scheduled activities to ready queue (PostgreSQL returns `0, nil` as it handles this natively)
- `RequeueExpired()` - Reclaim activities with expired leases
- `ExtendLease()` - Extend activity processing lease
- `StoreResult()` / `GetResult()` - Activity result storage
- `CheckIdempotency()` - Idempotency key handling
- `SchedulesNatively()` - Whether the backend handles scheduling in `Dequeue()` (skips the polling loop if `true`)

**`InspectionStorage`** - Observability operations:
- `Stats()` - Get queue statistics
- `ListPending()` / `ListProcessing()` / `ListScheduled()` / `ListCompleted()` - List activities by status
- `ListDeadLetter()` - List dead-lettered activities
- `GetActivity()` - Get specific activity details
- `GetActivityEvents()` - Get activity lifecycle events
- `EventStream()` - Stream real-time events (multiplexed via fan-out hub in `QueueInspector` — one connection serves all SSE subscribers)

### Graceful Shutdown

The worker engine supports graceful shutdown via OS signals (`SIGINT`, `SIGTERM`) or context cancellation:

```go
engine, err := runnerq.Builder().
	Backend(backend).
	MaxWorkers(8).
	Build()

// Option 1: Start in background, stop programmatically
go func() {
	if err := engine.Start(ctx); err != nil {
		log.Printf("Engine error: %v", err)
	}
}()

time.Sleep(10 * time.Second)
engine.Stop() // Graceful shutdown

// Option 2: Start with cancellable context
ctx, cancel := context.WithCancel(context.Background())
go func() {
	time.Sleep(10 * time.Second)
	cancel() // Triggers shutdown
}()
engine.Start(ctx) // Blocks until cancelled
```

### Activity Context and Metadata

Access rich context information in your activity handlers:

```go
func (h *MyHandler) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	// Access activity metadata
	fmt.Printf("Processing activity %s of type %s\n", ctx.ActivityID, ctx.ActivityType)
	fmt.Printf("This is retry attempt #%d\n", ctx.RetryCount)

	// Check for context cancellation / deadline
	if ctx.Ctx.Err() != nil {
		return nil, runnerq.NewNonRetryError("Activity was cancelled")
	}

	// Access custom metadata
	if correlationID, ok := ctx.Metadata["correlation_id"]; ok {
		fmt.Printf("Correlation ID: %s\n", correlationID)
	}

	// Execute sub-activities via the embedded executor
	subPayload, _ := json.Marshal(map[string]interface{}{"parent": ctx.ActivityID.String()})
	_, err := ctx.ActivityExecutor.Activity("child_task").
		Payload(subPayload).
		Execute(ctx.Ctx)

	result, _ := json.Marshal(map[string]interface{}{"status": "processed"})
	return result, nil
}
```

### Queue Statistics

Monitor queue performance and health using the inspector:

```go
import (
	"github.com/alob-mtc/runnerq-go/observability"
)

inspector := observability.NewQueueInspector(backend).WithMaxWorkers(engine.MaxConcurrentActivities())

stats, err := inspector.Stats(ctx)
if err != nil {
	log.Fatal(err)
}

fmt.Println("Queue stats:")
fmt.Printf("  Pending activities: %d\n", stats.PendingActivities)
fmt.Printf("  Processing activities: %d\n", stats.ProcessingActivities)
fmt.Printf("  Scheduled activities: %d\n", stats.ScheduledActivities)
fmt.Printf("  Dead letter queue size: %d\n", stats.DeadLetterActivities)
fmt.Println("Priority distribution:")
fmt.Printf("  Critical: %d\n", stats.CriticalPriority)
fmt.Printf("  High: %d\n", stats.HighPriority)
fmt.Printf("  Normal: %d\n", stats.NormalPriority)
fmt.Printf("  Low: %d\n", stats.LowPriority)
```

For a visual dashboard with real-time updates, see the [Observability Console](#observability-console) section.

## Error Handling

The library provides comprehensive error handling with clear separation between retryable and non-retryable errors.

### Activity Handler Results

```go
func (h *MyHandler) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	var data struct {
		ID    string `json:"id"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, runnerq.NewNonRetryError("Invalid data format")
	}

	if data.ID == "" {
		return nil, runnerq.NewNonRetryError("Missing required field: id")
	}

	// Perform operation that might temporarily fail
	result, err := externalAPICall(data)
	if err != nil {
		return nil, runnerq.NewRetryError(fmt.Sprintf("API call failed: %v", err))
	}

	resultJSON, _ := json.Marshal(map[string]interface{}{"result": result})
	return resultJSON, nil
}
```

**Error Types:**
- `runnerq.NewRetryError(message)` - Will be retried with exponential backoff
- `runnerq.NewNonRetryError(message)` - Will not be retried
- Any `error` returned that is not an `*ActivityError` is treated as retryable

### Dead Letter Callback

When an activity exhausts all retries, it moves to the dead letter queue. Handle this by implementing `OnDeadLetter` (or embed `DefaultDeadLetterHandler` for a no-op):

```go
type MyHandler struct{}

func (h *MyHandler) ActivityType() string { return "my_activity" }

func (h *MyHandler) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	return nil, nil
}

func (h *MyHandler) OnDeadLetter(ctx runnerq.ActivityContext, payload json.RawMessage, errorMsg string) {
	fmt.Printf("Activity %s dead-lettered: %s\n", ctx.ActivityID, errorMsg)
	// Use for cleanup, notifications, or logging
}
```

The `DefaultDeadLetterHandler` provides an empty `OnDeadLetter` implementation — embed it in your handler struct if you don't need dead letter handling.

### Worker Engine Errors

```go
executor := engine.GetActivityExecutor()

payload, _ := json.Marshal(map[string]interface{}{"id": "123", "value": "test"})
future, err := executor.Activity("my_activity").
	Payload(payload).
	Execute(ctx)
if err != nil {
	fmt.Printf("Failed to enqueue activity: %v\n", err)
	return
}

resultCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
defer cancel()

result, err := future.GetResult(resultCtx)
if err != nil {
	if we, ok := runnerq.IsWorkerError(err); ok {
		fmt.Printf("Worker error (retryable=%v): %v\n", we.IsRetryable(), we)
	} else {
		fmt.Printf("Error: %v\n", err)
	}
	return
}
fmt.Printf("Activity completed: %s\n", string(result))
```

### Error Recovery Patterns

```go
func (h *ResilientHandler) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	switch {
	case ctx.RetryCount <= 2:
		// First few attempts: retry on any error
		return h.processWithRetry(payload)
	case ctx.RetryCount <= 5:
		// Middle attempts: more conservative
		return h.processConservative(payload)
	default:
		// Final attempts: only retry on specific errors
		return h.processFinalAttempt(payload)
	}
}
```

### Default Values

When using the builder pattern, sensible defaults are provided:

```go
// Uses these defaults:
// - queue_name: "default"
// - max_workers: 10
// - schedule_poll_interval: 5 seconds
// - lease_ms: 60000 (60s)
// - reaper_interval: 5 seconds
// - reaper_batch_size: 100
engine, err := runnerq.Builder().
	Backend(backend).
	Build()
```

## Observability Console

RunnerQ includes a built-in web-based observability console for monitoring and managing your activity queues in real-time.

### Features

- **Real-time Updates** - Server-Sent Events (SSE) for instant activity updates
- **Live Statistics** - Monitor queue health with processing, pending, scheduled, and dead-letter counts
- **Priority Distribution** - See activity breakdown by priority level (Critical, High, Normal, Low)
- **Activity Management** - Browse and search activities across all queues (pending, processing, scheduled, completed, dead-letter)
- **Activity Results** - View execution results and outputs for completed activities
- **Event Timeline** - Detailed activity lifecycle events with multiple view modes
- **Zero Setup** - Embedded HTML, no build tools or npm required

### SSE Fan-Out Architecture

The real-time event stream uses a fan-out hub inside `QueueInspector` so that **all SSE
subscribers share a single backend connection**, regardless of how many clients are
connected.

```text
                        ┌──────────────┐
  Backend.EventStream() │  notifyHub   │──► SSE client 1
  (1 connection)  ─────►│  (inspector) │──► SSE client 2
                        │   broadcast  │──► SSE client 3
                        └──────────────┘
                         lazy start/stop
```

**How it works:**

- The first call to `SubscribeEvents()` lazily starts a single backend `EventStream`
  listener (e.g. one PostgreSQL `LISTEN` connection).
- Each additional subscriber gets its own buffered channel fed by non-blocking broadcast.
  If a subscriber's channel is full (slow client), events are dropped for that subscriber
  only — other subscribers are unaffected.
- When the last subscriber disconnects, the backend connection is released automatically.
- The HTTP layer enforces a concurrent SSE connection limit (default: 64). Requests beyond
  this limit receive `503 Service Unavailable`.

This design means 100 SSE clients consume 1 database connection instead of 100, eliminating
connection pool exhaustion as a denial-of-service vector.

**Custom backends** get fan-out for free — the hub operates on the `EventStream()` channel
returned by any `InspectionStorage` implementation, so there is nothing backend-specific to
implement.

### Quick Start

```go
import (
	"net/http"

	"github.com/alob-mtc/runnerq-go"
	"github.com/alob-mtc/runnerq-go/observability"
	"github.com/alob-mtc/runnerq-go/observability/ui"
	"github.com/alob-mtc/runnerq-go/storage/postgres"
)

backend, _ := postgres.New(ctx, "postgres://localhost/mydb", "my_app")

engine, _ := runnerq.Builder().
	Backend(backend).
	QueueName("my_app").
	Build()

inspector := observability.NewQueueInspector(backend).WithMaxWorkers(engine.MaxConcurrentActivities())

// Mount the console — serves the UI and API including SSE
mux := http.NewServeMux()
mux.Handle("/console/", http.StripPrefix("/console", ui.RunnerQUI(inspector)))

fmt.Println("RunnerQ Console: http://localhost:8081/console/")
http.ListenAndServe(":8081", mux)
```

### Integration with Existing Apps

Easily integrate the console into your existing HTTP server:

```go
mux := http.NewServeMux()

// Your existing routes
mux.HandleFunc("/api/users", listUsers)
mux.HandleFunc("/api/posts", listPosts)

// Add the RunnerQ console
mux.Handle("/console/", http.StripPrefix("/console", ui.RunnerQUI(inspector)))

http.ListenAndServe(":8080", mux)
```

### API-Only Mode

If you prefer to build a custom UI, serve just the API:

```go
mux.Handle("/api/observability/", http.StripPrefix("/api/observability", ui.ObservabilityAPI(inspector)))
```

### Example

See the complete examples:

```bash
# Start PostgreSQL
docker run -d --name runnerq-postgres \
    -e POSTGRES_PASSWORD=runnerq \
    -e POSTGRES_DB=runnerq \
    -p 5432:5432 \
    postgres:16

# Run the console example
export DATABASE_URL="postgres://postgres:runnerq@localhost:5432/runnerq"
go run ./examples/observability/console_ui

# Open http://localhost:8081/console/
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
