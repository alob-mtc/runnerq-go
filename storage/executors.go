package storage

import (
	"time"

	"github.com/google/uuid"
)

// ExecutorReportingStorage is a backend that reports the engines running on
// it, the way RunnerQ Cloud's hosted storage does so its console can show a
// hosted app's workers without an agent. An engine calls ExecutorStarted
// once it is running, with a function that reads its current state, and
// ExecutorStopped when it has stopped. Both must return promptly: report in
// the background.
type ExecutorReportingStorage interface {
	ExecutorStarted(info ExecutorInfo, state func() ExecutorState)
	ExecutorStopped(id string)
}

// ExecutorInfo describes an engine (an executor) that has started.
type ExecutorInfo struct {
	// ID is the engine's instance id, unique per process run: its executor
	// id in RunnerQ Cloud.
	ID             string
	Queue          string
	ActivityTypes  []string
	MaxConcurrency int
	StartedAt      time.Time
}

// ExecutorState is what an executor is doing at a moment.
type ExecutorState struct {
	// Running are the activities it is executing, oldest first.
	Running []RunningActivity
	// Draining: a shutdown has begun; intake has stopped and in-flight
	// activities are finishing.
	Draining bool
}

// RunningActivity is an activity an executor is executing.
type RunningActivity struct {
	ID        uuid.UUID
	Type      string
	Attempt   int
	StartedAt time.Time
}
