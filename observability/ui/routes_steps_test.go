package ui_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/observability"
	"github.com/alob-mtc/runnerq-go/observability/ui"
	"github.com/alob-mtc/runnerq-go/storage"
	"github.com/alob-mtc/runnerq-go/storage/postgres"
)

// The /steps route returns an activity's durable checkpoint history through
// inspector → backend, decoding the stored "kind:name" step identity.
func TestStepsRouteReturnsCheckpoints(t *testing.T) {
	dsn := os.Getenv("RUNNERQ_TEST_DSN")
	if dsn == "" {
		t.Skip("RUNNERQ_TEST_DSN not set; skipping integration test")
	}
	ctx := context.Background()
	queueName := "t_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
	backend, err := postgres.New(ctx, dsn, queueName)
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	defer backend.Close()

	owner := uuid.New()
	if err := backend.StoreResult(ctx, uuid.New(), owner,
		storage.ActivityResult{Data: json.RawMessage(`{"ok":true}`), State: storage.ResultOk}, "run:demo"); err != nil {
		t.Fatalf("seed step: %v", err)
	}

	srv := httptest.NewServer(ui.ObservabilityAPI(observability.NewQueueInspector(backend)))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/activities/" + owner.String() + "/steps")
	if err != nil {
		t.Fatalf("GET steps: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var steps []observability.StepView
	if err := json.NewDecoder(resp.Body).Decode(&steps); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(steps) != 1 || steps[0].Kind != "run" || steps[0].Name != "demo" || steps[0].State != "Ok" {
		t.Fatalf("steps = %+v, want one run:demo Ok", steps)
	}
}
