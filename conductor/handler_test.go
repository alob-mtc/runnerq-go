package conductor

import (
	"context"
	"encoding/json"
	"testing"
)

func TestHandlerServesStorage(t *testing.T) {
	_, b, queue := pgEngine(t)
	id := enqueue(t, b, `{"n":1}`)
	ctx := context.Background()
	h := NewHandler(b, HandlerConfig{Queue: queue, AllowControl: true})

	var caps map[string]json.RawMessage
	if err := json.Unmarshal(h.Capabilities(), &caps); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{typeActivitiesList, typeActivitiesGet, typeActivitiesAggregate, typeActivitiesCancel} {
		if _, ok := caps[want]; !ok {
			t.Errorf("capabilities miss %s: %v", want, caps)
		}
	}
	for _, not := range []string{typeExecutorDescribe, typeEventsSubscribe} {
		if _, ok := caps[not]; ok {
			t.Errorf("capabilities include %s", not)
		}
	}

	list, err := h.Serve(ctx, typeActivitiesList, mustJSON(t, map[string]any{"filter": inQueue(queue)}))
	if err != nil {
		t.Fatal(err)
	}
	var page struct {
		Items []struct {
			ID    string `json:"id"`
			Queue string `json:"queue"`
		} `json:"items"`
	}
	if err := json.Unmarshal(list, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ID != id.String() || page.Items[0].Queue != queue {
		t.Fatalf("list: %s", list)
	}

	cancel := map[string]any{"command_id": "c1", "target": map[string]any{"ids": []string{id.String()}, "queue": queue}}
	res, err := h.Serve(ctx, typeActivitiesCancel, mustJSON(t, cancel))
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Applied int `json:"applied"`
	}
	if json.Unmarshal(res, &out); out.Applied != 1 {
		t.Fatalf("cancel: %s", res)
	}

	other := map[string]any{"command_id": "c2", "target": map[string]any{"ids": []string{id.String()}, "queue": "elsewhere"}}
	if _, err := h.Serve(ctx, typeActivitiesCancel, mustJSON(t, other)); err == nil || err.Code != string(codeFailedPrecondition) {
		t.Fatalf("another queue's command: %v", err)
	}
	if _, err := h.Serve(ctx, typeExecutorDescribe, nil); err == nil || err.Code != string(codeUnsupported) {
		t.Fatalf("executor.describe: %v", err)
	}
	if _, err := h.Serve(ctx, typeActivitiesGet, mustJSON(t, map[string]any{"id": "not-an-id"})); err == nil {
		t.Fatal("bad id accepted")
	}

	readOnly := NewHandler(b, HandlerConfig{Queue: queue})
	if _, err := readOnly.Serve(ctx, typeActivitiesCancel, mustJSON(t, cancel)); err == nil || err.Code != string(codeUnsupported) {
		t.Fatalf("command without AllowControl: %v", err)
	}
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
