package ui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go/observability"
)

// RunnerQUI creates an http.Handler that serves the console UI and API.
// Mount it on any path, e.g. http.Handle("/console/", http.StripPrefix("/console", RunnerQUI(inspector)))
func RunnerQUI(inspector *observability.QueueInspector) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", serveUI)
	mux.HandleFunc("GET /api/observability/stats", statsHandler(inspector))
	mux.HandleFunc("GET /api/observability/activities/{key}", activityCollectionOrDetail(inspector))
	mux.HandleFunc("GET /api/observability/activities/{id}/events", activityEventsHandler(inspector))
	mux.HandleFunc("GET /api/observability/activities/{id}/result", activityResultHandler(inspector))
	mux.HandleFunc("GET /api/observability/dead-letter", deadLettersHandler(inspector))
	mux.HandleFunc("GET /api/observability/stream", eventStreamHandler(inspector))
	return mux
}

// ObservabilityAPI creates an http.Handler with just the API routes.
func ObservabilityAPI(inspector *observability.QueueInspector) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stats", statsHandler(inspector))
	mux.HandleFunc("GET /activities/{key}", activityCollectionOrDetail(inspector))
	mux.HandleFunc("GET /activities/{id}/events", activityEventsHandler(inspector))
	mux.HandleFunc("GET /activities/{id}/result", activityResultHandler(inspector))
	mux.HandleFunc("GET /dead-letter", deadLettersHandler(inspector))
	mux.HandleFunc("GET /stream", eventStreamHandler(inspector))
	return mux
}

func serveUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(ConsoleHTML))
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func parsePagination(r *http.Request) (offset, limit int) {
	offset = 0
	limit = 50
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	return
}

func statsHandler(inspector *observability.QueueInspector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats, err := inspector.Stats(r.Context())
		if err != nil {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, stats)
	}
}

func activityCollectionOrDetail(inspector *observability.QueueInspector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")

		// Try parsing as UUID for detail lookup
		if id, err := uuid.Parse(key); err == nil {
			activity, err := inspector.GetActivity(r.Context(), id)
			if err != nil {
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}
			if activity == nil {
				http.Error(w, "Not Found", http.StatusNotFound)
				return
			}
			writeJSON(w, http.StatusOK, activity)
			return
		}

		offset, limit := parsePagination(r)
		ctx := r.Context()

		switch key {
		case "pending":
			items, err := inspector.ListPending(ctx, offset, limit)
			if err != nil {
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}
			writeJSON(w, http.StatusOK, items)
		case "processing":
			items, err := inspector.ListProcessing(ctx, offset, limit)
			if err != nil {
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}
			writeJSON(w, http.StatusOK, items)
		case "scheduled":
			items, err := inspector.ListScheduled(ctx, offset, limit)
			if err != nil {
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}
			writeJSON(w, http.StatusOK, items)
		case "completed":
			items, err := inspector.ListCompleted(ctx, offset, limit)
			if err != nil {
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}
			writeJSON(w, http.StatusOK, items)
		case "dead_letter":
			items, err := inspector.ListDeadLetter(ctx, offset, limit)
			if err != nil {
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}
			var snapshots []observability.ActivitySnapshot
			for _, item := range items {
				snapshots = append(snapshots, item.Activity)
			}
			writeJSON(w, http.StatusOK, snapshots)
		default:
			http.Error(w, "Bad Request", http.StatusBadRequest)
		}
	}
}

func activityEventsHandler(inspector *observability.QueueInspector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		idStr := r.PathValue("id")
		id, err := uuid.Parse(idStr)
		if err != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
		_, limit := parsePagination(r)
		events, err := inspector.RecentEvents(r.Context(), id, limit)
		if err != nil {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, events)
	}
}

func activityResultHandler(inspector *observability.QueueInspector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		idStr := r.PathValue("id")
		id, err := uuid.Parse(idStr)
		if err != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
		result, err := inspector.GetResult(r.Context(), id)
		if err != nil {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, result)
	}
}

func deadLettersHandler(inspector *observability.QueueInspector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		offset, limit := parsePagination(r)
		records, err := inspector.ListDeadLetter(r.Context(), offset, limit)
		if err != nil {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, records)
	}
}

func eventStreamHandler(inspector *observability.QueueInspector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming not supported", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		ch, err := inspector.EventStream(r.Context())
		if err != nil {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		for {
			select {
			case event, ok := <-ch:
				if !ok {
					return
				}
				data, err := json.Marshal(event)
				if err != nil {
					continue
				}
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			case <-r.Context().Done():
				return
			}
		}
	}
}
