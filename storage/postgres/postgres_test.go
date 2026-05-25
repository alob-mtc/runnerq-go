package postgres

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestActivityRowToSnapshotMapsScheduledStatus(t *testing.T) {
	scheduledAt := time.Date(2026, 5, 23, 16, 58, 14, 0, time.UTC)
	row := activityRow{
		id:           uuid.New(),
		activityType: "CheckTransactionFinalization",
		payload:      []byte(`{}`),
		priority:     2,
		status:       "scheduled",
		createdAt:    scheduledAt.Add(-5 * time.Minute),
		scheduledAt:  &scheduledAt,
		maxRetries:   1,
	}

	got := row.toSnapshot()
	if got.Status != "Scheduled" {
		t.Fatalf("scheduled row status = %q, want %q", got.Status, "Scheduled")
	}
	if got.ScheduledAt == nil || !got.ScheduledAt.Equal(scheduledAt) {
		t.Fatalf("scheduled_at = %v, want %v", got.ScheduledAt, scheduledAt)
	}
}
