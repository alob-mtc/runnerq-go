package conductor

import (
	"encoding/json"
	"fmt"
)

// protocolVersion is the highest Conductor protocol version this agent speaks.
const protocolVersion = 1

type kind string

const (
	kindRequest  kind = "req"
	kindResponse kind = "res"
	kindEvent    kind = "evt"
)

// envelope is one frame on the wire.
type envelope struct {
	V     int             `json:"v"`
	Kind  kind            `json:"kind"`
	ID    string          `json:"id,omitempty"`
	Type  string          `json:"type"`
	Data  json.RawMessage `json:"data,omitempty"`
	Error *wireError      `json:"error,omitempty"`
}

type errorCode string

const (
	codeInvalidArgument errorCode = "invalid_argument"
	codeNotFound        errorCode = "not_found"
	codeForbidden       errorCode = "forbidden"
	codeUnsupported     errorCode = "unsupported"
	codeUnavailable     errorCode = "unavailable"
	codeInternal        errorCode = "internal"
)

// wireError is a failed response. Handlers return it to choose the code; any
// other error is reported as internal.
type wireError struct {
	Code    errorCode `json:"code"`
	Message string    `json:"message"`
}

func (e *wireError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

func errorf(code errorCode, format string, args ...any) *wireError {
	return &wireError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Message types.
const (
	typeHello   = "hello"
	typeGoodbye = "goodbye"

	typeStats             = "stats"
	typeListActivities    = "list_activities"
	typeListRoots         = "list_roots"
	typeGetActivity       = "get_activity"
	typeGetActivityEvents = "get_activity_events"
	typeGetActivitySteps  = "get_activity_steps"
	typeGetActivityResult = "get_activity_result"
	typeGetChildren       = "get_children"
	typeGetSubtree        = "get_subtree"
	typeListDeadLetter    = "list_dead_letter"

	typeSignal = "signal"
)

type dataMode string

const (
	dataModeFull         dataMode = "full"
	dataModeMetadataOnly dataMode = "metadata_only"
)

type sdkInfo struct {
	Name     string `json:"name"`
	Version  string `json:"version"`
	Language string `json:"language"`
}

type hello struct {
	ProtocolVersions []int    `json:"protocol_versions"`
	SDK              sdkInfo  `json:"sdk"`
	ExecutorID       string   `json:"executor_id"`
	Hostname         string   `json:"hostname"`
	ActivityTypes    []string `json:"activity_types,omitempty"`
	MaxWorkers       int      `json:"max_workers"`
	Capabilities     []string `json:"capabilities"`
}

type welcome struct {
	Version   int      `json:"version"`
	SessionID string   `json:"session_id"`
	App       string   `json:"app"`
	DataMode  dataMode `json:"data_mode"`
}

type goodbye struct {
	Reason string `json:"reason"`
}

type page struct {
	Offset int `json:"offset,omitempty"`
	Limit  int `json:"limit,omitempty"`
}

type listRequest struct {
	Status string `json:"status"`
	page
}

type activityRef struct {
	ID string `json:"id"`
}

type activityPage struct {
	ID string `json:"id"`
	page
}

type signalRequest struct {
	ID           string          `json:"id,omitempty"`
	Key          string          `json:"key,omitempty"`
	ActivityType string          `json:"activity_type,omitempty"`
	Name         string          `json:"name"`
	Payload      json.RawMessage `json:"payload,omitempty"`
}

type controlResult struct {
	Status string `json:"status,omitempty"`
}
