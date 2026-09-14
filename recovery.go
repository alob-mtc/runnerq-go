package runnerq

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/alob-mtc/runnerq-go/storage"
)

const storageAttemptTimeout = 5 * time.Second

// retryStorage retains the caller's operation inputs until persistence succeeds
// or cancellation/ownership/permanent failure ends recovery. Each attempt gets a
// fresh deadline; a timeout of one database attempt does not cancel the retry loop.
func retryStorage(ctx context.Context, operation string, metrics MetricsSink, renew func(context.Context) error, fn func(context.Context) error) error {
	if metrics == nil {
		metrics = NoopMetrics{}
	}
	started := time.Now()
	delay := 100 * time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		attempt, cancel := context.WithTimeout(ctx, storageAttemptTimeout)
		err := fn(attempt)
		attemptExpired := attempt.Err() == context.DeadlineExceeded
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		se, typed := storage.IsStorageError(err)
		if !(typed && se.IsRetryable()) && !(attemptExpired && errors.Is(err, context.DeadlineExceeded)) {
			return err
		}
		if renew != nil {
			rctx, rcancel := context.WithTimeout(ctx, storageAttemptTimeout)
			rerr := renew(rctx)
			rcancel()
			if rerr != nil {
				// A committed completion clears ownership before its reply may
				// be lost. Only the write's next reconciliation can distinguish
				// that success from a competing claim; renewal cannot decide it.
				slog.Warn("Could not renew claim during storage recovery", "operation", operation, "error", rerr)
			}
		}
		metrics.IncCounter("storage_retry", 1)
		slog.Warn("Retrying storage operation", "operation", operation, "pending_for", time.Since(started), "error", err)
		wait := time.Duration(float64(delay) * (0.95 + rand.Float64()*0.1))
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay = min(delay*2, 30*time.Second)
	}
}

func retryableError(err error) bool {
	var re RetryableError
	return !errors.As(err, &re) || re.IsRetryable()
}
