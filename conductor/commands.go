package conductor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/storage"
)

var commandKinds = map[string]storage.CommandKind{
	typeActivitiesCancel:      storage.CommandCancel,
	typeActivitiesRetry:       storage.CommandRetry,
	typeActivitiesRunNow:      storage.CommandRunNow,
	typeActivitiesReschedule:  storage.CommandReschedule,
	typeActivitiesSetPriority: storage.CommandSetPriority,
	typeActivitiesDelete:      storage.CommandDelete,
	typeActivitiesSignal:      storage.CommandSignal,
}

func (h *handlers) addCommands(t map[string]handlerFunc) {
	if h.cs == nil || !h.allowControl {
		return
	}
	for msgType, kind := range commandKinds {
		t[msgType] = h.command(kind)
	}
}

func (h *handlers) commandCapabilities(caps map[string]capability) {
	if h.cs == nil || !h.allowControl {
		return
	}
	for msgType, kind := range commandKinds {
		targets := []string{"filter", "ids"}
		if kind == storage.CommandSignal {
			targets = []string{"filter", "idempotency_key", "ids"}
		}
		caps[msgType] = capability{V: 1, Targets: targets}
	}
}

// fingerprint identifies a command's input independent of JSON key order.
func fingerprint(data json.RawMessage) string {
	var v any
	if json.Unmarshal(data, &v) == nil {
		if canon, err := json.Marshal(v); err == nil {
			data = canon
		}
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (h *handlers) command(kind storage.CommandKind) handlerFunc {
	return func(ctx context.Context, data json.RawMessage) (any, error) {
		req, err := decode[commandRequest](data)
		if err != nil {
			return nil, err
		}
		cmd, badIDs, err := h.toCommand(kind, req)
		if err != nil {
			return nil, err
		}
		cmd.Fingerprint = fingerprint(data)

		res := &storage.CommandResult{}
		if len(req.Target.IDs) == 0 || len(cmd.Target.IDs) > 0 {
			res, err = h.cs.ApplyCommand(ctx, cmd)
		} // else every id was foreign
		if err != nil {
			if se, ok := storage.IsStorageError(err); ok && se.Kind == storage.ErrConflict {
				return nil, errorf(codeConflict, "%s", se.Message)
			}
			return nil, err
		}
		// Stop a cancelled activity running here now, not at its next heartbeat.
		if kind == storage.CommandCancel && !cmd.DryRun && !res.Replayed && h.engine != nil {
			for _, it := range res.Items {
				if it.Outcome == storage.CommandApplied {
					h.engine.Interrupt(it.ID)
				}
			}
		}

		out := commandResult{Matched: res.Matched, Applied: res.Applied, Cascaded: res.Cascaded,
			More: res.More, Replayed: res.Replayed, Results: make([]commandItem, 0, len(res.Items)+len(badIDs))}
		for _, it := range res.Items {
			ci := commandItem{ID: it.ID.String(), Outcome: it.Outcome, Status: it.Status}
			switch {
			case it.ErrMessage == "":
			case it.ErrKind == storage.ErrNotFound:
				ci.Error = errorf(codeNotFound, "%s", it.ErrMessage)
			default:
				ci.Error = errorf(codeFailedPrecondition, "%s", it.ErrMessage)
				if it.Status != "" {
					ci.Error.Details = map[string]any{"status": it.Status}
				}
			}
			out.Results = append(out.Results, ci)
		}
		// Ids this backend could not have issued do not exist.
		for _, id := range badIDs {
			out.Results = append(out.Results, commandItem{ID: id, Outcome: storage.CommandSkipped,
				Error: errorf(codeNotFound, "no such activity")})
		}
		return out, nil
	}
}

// toCommand also returns the target ids this backend could not have issued.
func (h *handlers) toCommand(kind storage.CommandKind, req commandRequest) (storage.Command, []string, error) {
	cmd := storage.Command{ID: req.CommandID, Kind: kind, DryRun: req.DryRun, Reason: req.Reason}
	if q := req.Target.Queue; q != "" && q != h.queue {
		e := fieldError(codeFailedPrecondition, "target.queue", "this executor serves queue %q, not %q", h.queue, q)
		return cmd, nil, e
	}

	// Each command accepts only its own fields.
	unexpected := func(field string, set bool) error {
		if set {
			return fieldError(codeInvalidArgument, field, "%s does not apply to %s", field, kind)
		}
		return nil
	}
	for field, set := range map[string]bool{
		"cascade":        req.Cascade != "" && kind != storage.CommandCancel && kind != storage.CommandDelete,
		"reset_attempts": req.ResetAttempts && kind != storage.CommandRetry,
		"at":             req.At != "" && kind != storage.CommandReschedule,
		"priority":       req.Priority != 0 && kind != storage.CommandSetPriority,
		"name":           req.Name != "" && kind != storage.CommandSignal,
		"payload":        len(req.Payload) > 0 && kind != storage.CommandSignal,
	} {
		if err := unexpected(field, set); err != nil {
			return cmd, nil, err
		}
	}

	switch kind {
	case storage.CommandCancel:
		switch req.Cascade {
		case "", "children":
			cmd.CascadeChildren = true // cascading is the default
		case "none":
		default:
			return cmd, nil, fieldError(codeInvalidArgument, "cascade", "cascade must be children or none")
		}
	case storage.CommandDelete:
		if req.Cascade != "" && req.Cascade != "tree" {
			return cmd, nil, fieldError(codeInvalidArgument, "cascade", "delete always removes the whole tree")
		}
	case storage.CommandRetry:
		cmd.ResetAttempts = req.ResetAttempts
	case storage.CommandReschedule:
		at, err := time.Parse(time.RFC3339Nano, req.At)
		if err != nil {
			return cmd, nil, fieldError(codeInvalidArgument, "at", "at must be an RFC 3339 timestamp")
		}
		cmd.At = at
	case storage.CommandSetPriority:
		cmd.Priority = storage.ActivityPriority(req.Priority)
	case storage.CommandSignal:
		cmd.SignalName, cmd.SignalPayload = req.Name, req.Payload
	}

	var badIDs []string
	t := req.Target
	switch {
	case len(t.IDs) > 0:
		for _, s := range t.IDs {
			if id, err := uuid.Parse(s); err == nil {
				cmd.Target.IDs = append(cmd.Target.IDs, id)
			} else {
				badIDs = append(badIDs, s)
			}
		}
	case t.Filter != nil:
		f, err := toStorageFilter(t.Filter)
		if err != nil {
			return cmd, nil, err
		}
		cmd.Target.Filter, cmd.Target.Max = f, t.Max
	case t.IdempotencyKey != "":
		if t.Type == "" {
			return cmd, nil, fieldError(codeInvalidArgument, "target.type", "an idempotency_key target needs the activity type")
		}
		cmd.Target.IdempotencyKey = storage.BusinessIdempotencyKey(t.IdempotencyKey, t.Type)
	}
	return cmd, badIDs, nil
}
