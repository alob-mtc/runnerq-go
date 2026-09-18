package runnerq

import (
	"context"
	"log/slog"
	"time"

	"github.com/alob-mtc/runnerq-go/storage"
	"github.com/google/uuid"
)

// Scoped to one execution. Checkpoint persistence outlives the handler deadline
// during recovery, but remains bounded by engine shutdown and claim ownership.
type attemptQueue struct {
	activityQueue
	backend        storage.Storage
	owner          uuid.UUID
	worker         string
	persistenceCtx context.Context
	metrics        MetricsSink
}

// attemptHeartbeatInterval is how often a running handler's claim is renewed
// and re-verified. It bounds how long an execution that lost its lease (a
// stalled process, a partition longer than the lease) keeps running
// unawares. Several beats fit inside attemptLeaseExtension, so a missed one
// does not cost the claim.
const attemptHeartbeatInterval = 10 * time.Second

// attemptLeaseExtension is how far each renewal pushes the lease out. A
// renewal never shortens a lease.
const attemptLeaseExtension = 60 * time.Second

func (q *attemptQueue) renew(ctx context.Context) error {
	if b, ok := q.backend.(storage.AttemptLeaseStorage); ok {
		owned, err := b.ExtendLeaseForWorker(ctx, q.owner, q.worker, attemptLeaseExtension)
		if err != nil {
			return err
		}
		if !owned {
			return &storage.StorageError{Kind: storage.ErrClaimLost, Message: "activity was reclaimed by another execution"}
		}
	}
	return nil
}

// Spawns issued by a handler are fenced on its claim when the backend supports
// it: an execution that lost its lease may still be running user code while
// its replacement issues the same spawns, and only one of them may add
// children to the tree.
func (q *attemptQueue) Enqueue(ctx context.Context, a *activity) error {
	if b, ok := q.backend.(storage.SpawnStorage); ok {
		return b.EnqueueForWorker(ctx, activityToQueued(a), q.owner, q.worker)
	}
	return q.activityQueue.Enqueue(ctx, a)
}

func (q *attemptQueue) ScheduleActivity(ctx context.Context, a *activity) error {
	b, ok := q.backend.(storage.SpawnStorage)
	if !ok {
		return q.activityQueue.ScheduleActivity(ctx, a)
	}
	queued := activityToQueued(a)
	if queued.ScheduledAt == nil {
		now := time.Now().UTC()
		queued.ScheduledAt = &now
	}
	return b.EnqueueForWorker(ctx, queued, q.owner, q.worker)
}

func (q *attemptQueue) EnqueueIdempotent(ctx context.Context, a *activity) (*storage.IdempotencyResult, error) {
	if b, ok := q.backend.(storage.SpawnStorage); ok {
		queued := activityToQueued(a)
		return b.EnqueueIdempotentForWorker(ctx, &queued, q.owner, q.worker)
	}
	return q.activityQueue.EnqueueIdempotent(ctx, a)
}

// heartbeat renews the claim every interval until ctx ends (the handler
// returned or ran out its timeout) or the claim turns out to be lost, in which
// case it calls revoke with the ErrClaimLost error and exits. A renewal that
// merely fails is retried on the next beat: the lease already covers the
// handler's whole timeout, so an outage shorter than that costs nothing. The
// returned stop blocks until the goroutine has exited, so no renewal can race
// the acknowledgement that follows — a beat landing after the ack would find
// the claim released and misreport it as lost.
func (q *attemptQueue) heartbeat(ctx context.Context, interval time.Duration, revoke context.CancelCauseFunc) (stop func()) {
	if _, ok := q.backend.(storage.AttemptLeaseStorage); !ok {
		return func() {}
	}
	if interval <= 0 {
		interval = attemptHeartbeatInterval
	}
	metrics := q.metrics
	if metrics == nil {
		metrics = NoopMetrics{}
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			attempt, attemptCancel := context.WithTimeout(ctx, storageAttemptTimeout)
			err := q.renew(attempt)
			attemptCancel()
			if err == nil || ctx.Err() != nil {
				continue
			}
			if se, ok := storage.IsStorageError(err); ok && se.Kind == storage.ErrClaimLost {
				revoke(err)
				return
			}
			metrics.IncCounter("activity_heartbeat_failed", 1)
			slog.Warn("Could not renew activity claim; retrying on the next heartbeat", "activity_id", q.owner, "error", err)
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func (q *attemptQueue) GetResult(ctx context.Context, id uuid.UUID) (out *activityResult, err error) {
	err = q.retryRead(ctx, "read checkpoint", func(ctx context.Context) error {
		var err error
		out, err = q.activityQueue.GetResult(ctx, id)
		return err
	})
	return
}

func (q *attemptQueue) WaitForResult(ctx context.Context, id uuid.UUID) (out *activityResult, err error) {
	err = q.retryRead(ctx, "wait for result", func(ctx context.Context) error {
		var err error
		out, err = q.activityQueue.WaitForResult(ctx, id)
		return err
	})
	return
}

func (q *attemptQueue) StoreResult(_ context.Context, id, owner uuid.UUID, result activityResult, step string) error {
	return retryStorage(q.persistenceCtx, "persist checkpoint", q.metrics, q.renew, func(ctx context.Context) error {
		if b, ok := q.backend.(storage.CheckpointStorage); ok {
			return b.StoreCheckpoint(ctx, id, owner, q.worker, storage.ActivityResult{Data: result.Data, State: storage.ResultState(result.State)}, step)
		}
		return q.activityQueue.StoreResult(ctx, id, owner, result, step)
	})
}

func (q *attemptQueue) registerFuture(ctx context.Context, id uuid.UUID) error {
	if b, ok := q.backend.(storage.DependencyStorage); ok {
		return retryStorage(ctx, "register result dependency", q.metrics, q.renew, func(ctx context.Context) error {
			return b.RegisterDependency(ctx, q.owner, id, q.worker)
		})
	}
	return nil
}

// Read retries have no uncertain commit to reconcile. Once renewal establishes
// claim loss, don't return a missing checkpoint that could trigger another effect.
func (q *attemptQueue) retryRead(ctx context.Context, operation string, fn func(context.Context) error) error {
	var lost error
	renew := func(ctx context.Context) error {
		err := q.renew(ctx)
		if se, ok := storage.IsStorageError(err); ok && se.Kind == storage.ErrClaimLost {
			lost = err
		}
		return err
	}
	return retryStorage(ctx, operation, q.metrics, renew, func(ctx context.Context) error {
		if lost != nil {
			return lost
		}
		return fn(ctx)
	})
}
