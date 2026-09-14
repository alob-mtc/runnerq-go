package runnerq

import (
	"context"
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

func (q *attemptQueue) renew(ctx context.Context) error {
	if b, ok := q.backend.(storage.AttemptLeaseStorage); ok {
		owned, err := b.ExtendLeaseForWorker(ctx, q.owner, q.worker, 60*time.Second)
		if err != nil {
			return err
		}
		if !owned {
			return &storage.StorageError{Kind: storage.ErrClaimLost, Message: "activity was reclaimed during persistence recovery"}
		}
	}
	return nil
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
