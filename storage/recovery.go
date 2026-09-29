package storage

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// AttemptLeaseStorage renews only the claim held by workerID, which is a
// fresh token per dequeue attempt, not a reusable slot label.
type AttemptLeaseStorage interface {
	ExtendLeaseForWorker(ctx context.Context, activityID uuid.UUID, workerID string, extendBy time.Duration) (bool, error)
}

// CheckpointStorage stores immutable checkpoints fenced on the owning
// execution. Retrying identical data succeeds; a different outcome is
// ErrCheckpointConflict. Signals, which are mutable, must not use it.
type CheckpointStorage interface {
	StoreCheckpoint(ctx context.Context, resultID, ownerID uuid.UUID, workerID string, result ActivityResult, step string) error
}

// SpawnStorage fences handler spawns on the spawning execution's claim: the
// claim check and insert commit together, so an execution that lost its lease
// (whose replacement may be issuing the same spawns) cannot add children.
// ownerID is the executing activity, the fence even for AsRoot spawns. Both
// methods return ErrClaimLost when workerID no longer holds ownerID.
type SpawnStorage interface {
	EnqueueForWorker(ctx context.Context, a QueuedActivity, ownerID uuid.UUID, workerID string) error
	EnqueueIdempotentForWorker(ctx context.Context, a *QueuedActivity, ownerID uuid.UUID, workerID string) (*IdempotencyResult, error)
}

// EncodedStorage adds an encoding argument (e.g. the TypeScript SDK's
// superjson-v1) to the calls whose arguments can't carry one. The Go SDK
// doesn't use it: it only writes plain JSON.
//
// DequeueBatchEncoded is DequeueBatch restricted to inputs in one of
// serializations (empty or "json-v1" is plain JSON). AckSuccessEncoded and
// SignalActivityEncoded store the given encoding with the result or payload.
type EncodedStorage interface {
	DequeueBatchEncoded(ctx context.Context, workerIDPrefix string, limit int, timeout time.Duration, activityTypes []string, serializations []string) ([]DequeuedActivity, error)
	AckSuccessEncoded(ctx context.Context, activityID uuid.UUID, result json.RawMessage, serialization string, workerID string) error
	SignalActivityEncoded(ctx context.Context, activityID uuid.UUID, signalID uuid.UUID, name string, payload json.RawMessage, serialization string) error
}

// DependencyStorage records which activity waits on which result, independent
// of parent lineage. Dependencies survive replay and are deleted with the
// waiter's tree. RegisterDependency rejects a missing producer (rehydrated futures).
// YieldForResult atomically records the wait, checks readiness, and parks or
// wakes the claim; a nil producerID means an external signal.
type DependencyStorage interface {
	RegisterDependency(ctx context.Context, waiterID, resultID uuid.UUID, workerID string) error
	YieldForResult(ctx context.Context, waiterID, resultID uuid.UUID, producerID *uuid.UUID, wakeAt time.Time, workerID, kind, step string) error
}
