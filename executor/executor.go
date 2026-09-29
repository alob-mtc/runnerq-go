// Package executor describes a running engine (an executor): who it is,
// what it is doing, and what it has done. The engine is the one producer
// of these snapshots; the RunnerQ Cloud agent, the cloud storage adapter
// and metrics exporters all read them, each on its own schedule and over
// its own transport.
package executor

import (
	"os"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
)

// Info is who an executor is: fixed for its run.
type Info struct {
	// ID is the engine's instance id, unique per run: its executor id in
	// RunnerQ Cloud.
	ID             string
	Queue          string
	ActivityTypes  []string
	MaxConcurrency int
	StartedAt      time.Time
	Hostname       string
	SDK            SDK
	// Labels are free-form tags (region, deploy version) from the engine's
	// configuration.
	Labels map[string]string
}

// SDK names the RunnerQ SDK the executor runs.
type SDK struct {
	Name, Version, Language string
}

// State is what an executor is doing now.
type State struct {
	// Running are the activities it is executing, oldest first.
	Running []Running
	// Draining: a shutdown has begun; intake has stopped and in-flight
	// activities are finishing.
	Draining bool
}

// Running is an activity an executor is executing.
type Running struct {
	ID        uuid.UUID
	Type      string
	Attempt   int
	StartedAt time.Time
}

// Counters are what an executor has done since it started.
type Counters struct {
	// Claimed activities started executing here.
	Claimed uint64
	// Succeeded completed; Retried asked for another attempt; Failed failed
	// for good; TimedOut ran past their timeout; DeadLettered ran out of
	// attempts.
	Succeeded, Retried, Failed, TimedOut, DeadLettered uint64
	// ClaimsLost: another worker took over an activity running here.
	ClaimsLost uint64
	// HeartbeatFailures: claim renewals that failed.
	HeartbeatFailures uint64
	// LastClaimLag is how long the last claimed activity waited to be
	// claimed after it was due.
	LastClaimLag time.Duration
}

// Snapshot is an executor at a moment.
type Snapshot struct {
	Info     Info
	State    State
	Counters Counters
	At       time.Time
}

// Source gives a running executor's snapshots.
type Source interface {
	Snapshot() Snapshot
}

// Observer hears an executor start and stop, and reads its snapshots from
// the Source on its own schedule. Both calls must return promptly.
//
// An engine calls every Observer registered with WorkerEngine.Observe, and
// also its storage backend when the backend is an Observer (RunnerQ Cloud's
// storage adapter reports hosted workers that way).
type Observer interface {
	ExecutorStarted(src Source)
	ExecutorStopped(id string)
}

// ThisSDK names the runnerq-go SDK compiled into this binary; its version
// is "unknown" without build information.
func ThisSDK() SDK {
	const module = "github.com/alob-mtc/runnerq-go"
	sdk := SDK{Name: "runnerq-go", Version: "unknown", Language: "go"}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return sdk
	}
	if info.Main.Path == module {
		sdk.Version = info.Main.Version
		return sdk
	}
	for _, dep := range info.Deps {
		if dep.Path == module {
			sdk.Version = dep.Version
			if dep.Replace != nil {
				sdk.Version = dep.Replace.Version
			}
		}
	}
	return sdk
}

// Hostname is this machine's name, or empty.
func Hostname() string {
	h, _ := os.Hostname()
	return h
}
