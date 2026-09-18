package storage

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// AttemptLeaseStorage renews only the claim identified by workerID. Engines use
// a fresh workerID per dequeue attempt, not a reusable process/slot label.
type AttemptLeaseStorage interface {
	ExtendLeaseForWorker(ctx context.Context, activityID uuid.UUID, workerID string, extendBy time.Duration) (bool, error)
}

// CheckpointStorage provides immutable local checkpoints fenced by the owning
// execution. Retrying identical data succeeds; a different outcome conflicts.
// Mutable signal delivery must not use this interface.
type CheckpointStorage interface {
	StoreCheckpoint(ctx context.Context, resultID, ownerID uuid.UUID, workerID string, result ActivityResult, step string) error
}

// SpawnStorage fences activities spawned from inside a handler on the spawning
// execution's claim: the claim check and the insert commit together, so an
// execution that lost its lease — and whose replacement may already be issuing
// the same spawns — cannot add children to the tree. ownerID is the executing
// activity, which is also the fence for spawns detached with AsRoot. Both
// methods return ErrClaimLost when workerID no longer holds ownerID.
type SpawnStorage interface {
	EnqueueForWorker(ctx context.Context, a QueuedActivity, ownerID uuid.UUID, workerID string) error
	EnqueueIdempotentForWorker(ctx context.Context, a *QueuedActivity, ownerID uuid.UUID, workerID string) (*IdempotencyResult, error)
}

// DependencyStorage keeps result consumers independent of parent lineage.
// References survive replay and are removed with consumer-tree retention.
// RegisterDependency rejects missing producers for rehydrated activity futures.
// YieldForResult atomically records a wait, checks readiness, and parks or wakes
// the current claim. A nil producer denotes an external signal.
type DependencyStorage interface {
	RegisterDependency(ctx context.Context, waiterID, resultID uuid.UUID, workerID string) error
	YieldForResult(ctx context.Context, waiterID, resultID uuid.UUID, producerID *uuid.UUID, wakeAt time.Time, workerID, kind, step string) error
}
