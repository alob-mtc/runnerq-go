package storage

import (
	"context"
	"encoding/json"
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

// EncodedStorage serves workers whose payloads and results carry an encoding
// (the TypeScript SDK's native superjson-v1 besides plain JSON), for the calls
// whose arguments can't carry one. The Go SDK never needs it: its claims take
// plain JSON only, and a QueuedActivity or ActivityResult carries its own
// Serialization everywhere else.
//
// DequeueBatchEncoded is DequeueBatch claiming only activities whose input is
// in one of serializations (empty or "json-v1" is plain JSON). AckSuccessEncoded
// is AckSuccess with the result's encoding, and SignalActivityEncoded is
// SignalActivity with the payload's.
type EncodedStorage interface {
	DequeueBatchEncoded(ctx context.Context, workerIDPrefix string, limit int, timeout time.Duration, activityTypes []string, serializations []string) ([]DequeuedActivity, error)
	AckSuccessEncoded(ctx context.Context, activityID uuid.UUID, result json.RawMessage, serialization string, workerID string) error
	SignalActivityEncoded(ctx context.Context, activityID uuid.UUID, signalID uuid.UUID, name string, payload json.RawMessage, serialization string) error
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
