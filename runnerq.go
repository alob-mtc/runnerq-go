// Package runnerq provides a durable activity queue and worker system for Go.
//
// # Features
//
//   - Priority-based activity processing (Critical, High, Normal, Low)
//   - Activity scheduling with precise timestamp-based scheduling
//   - Intelligent retry mechanism with exponential backoff
//   - Dead letter queue handling for activities exceeding retry limits
//   - Concurrent activity processing with configurable worker pools
//   - Graceful shutdown with proper cleanup
//   - Activity orchestration enabling activities to execute other activities
//   - Comprehensive error handling with retryable and non-retryable types
//   - Pluggable storage backends (PostgreSQL built-in)
//   - Worker-level activity type filtering
//   - Queue statistics and monitoring
//   - Web-based observability console
//
// # Quick Start
//
//	backend, _ := postgres.New(ctx, "postgres://localhost/mydb", "my_app")
//	engine, _ := runnerq.Builder().
//	    Backend(backend).
//	    QueueName("my_app").
//	    MaxWorkers(8).
//	    Build()
//
//	engine.RegisterActivity("send_email", &SendEmailHandler{})
//	engine.Start(ctx)
package runnerq
