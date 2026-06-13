// 09 — Cross-Process Futures
//
// A future is just an activity ID plus a backend handle, so it's rehydratable:
// enqueue a job in your web tier, hand the caller an ID, and let any process —
// a different replica, a polling endpoint, a separate worker fleet — await the
// result later with runnerq.FutureFor(backend, id).
//
// This models a web API: POST /jobs enqueues and returns an ID; GET /jobs/{id}
// reconstructs the future from just that ID and reports status or result.
//
//	docker compose up -d        # from the examples/ directory
//	go run .
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/alob-mtc/runnerq-go"
	"github.com/alob-mtc/runnerq-go/storage/postgres"
)

type ResizeImage struct {
	runnerq.DefaultDeadLetterHandler
}

func (h *ResizeImage) ActivityType() string { return "resize_image" }

func (h *ResizeImage) Handle(ctx runnerq.ActivityContext, payload json.RawMessage) (json.RawMessage, error) {
	time.Sleep(2 * time.Second) // simulate real work
	return json.RawMessage(`{"width":800,"height":600}`), nil
}

func main() {
	ctx := context.Background()

	backend, err := postgres.New(ctx, databaseURL(), "cross_process")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}

	engine, err := runnerq.Builder().Backend(backend).MaxWorkers(4).Build()
	if err != nil {
		log.Fatalf("build: %v", err)
	}
	engine.RegisterActivity("resize_image", &ResizeImage{})

	engineDone := make(chan struct{})
	go func() {
		defer close(engineDone)
		if err := engine.Start(ctx); err != nil {
			log.Printf("engine stopped: %v", err)
		}
	}()
	defer func() {
		engine.Stop()
		<-engineDone
		backend.Close()
	}()

	mux := http.NewServeMux()

	// POST /jobs — enqueue and return the activity ID. The caller polls with it.
	mux.HandleFunc("/jobs", func(w http.ResponseWriter, r *http.Request) {
		fut, err := engine.GetActivityExecutor().
			Activity("resize_image").
			Payload(json.RawMessage(`{"src":"photo.jpg"}`)).
			Execute(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"id": fut.ActivityID().String()})
	})

	// GET /jobs/{id} — reconstruct the future from the ID ALONE and check it.
	// This handler never saw the original *ActivityFuture; in a real system it
	// could be a different process entirely.
	mux.HandleFunc("/jobs/", func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(strings.TrimPrefix(r.URL.Path, "/jobs/"))
		if err != nil {
			http.Error(w, "bad id", http.StatusBadRequest)
			return
		}
		// Short deadline: if it's not done yet, report "running" rather than block.
		waitCtx, cancel := context.WithTimeout(r.Context(), 200*time.Millisecond)
		defer cancel()
		result, err := runnerq.FutureFor(backend, id).GetResult(waitCtx)
		if err != nil {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"status": "running"})
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(append([]byte(`{"status":"done","result":`), append(result, '}')...))
	})

	srv := &http.Server{Addr: ":8080", Handler: mux}
	go srv.ListenAndServe()
	defer srv.Close()
	time.Sleep(200 * time.Millisecond)

	// Drive it: enqueue, poll immediately (running), poll after work (done).
	id := postJSON("http://localhost:8080/jobs")["id"]
	fmt.Printf("enqueued job %s\n", id)

	fmt.Printf("  poll right away: %v\n", getJSON("http://localhost:8080/jobs/"+id))
	time.Sleep(3 * time.Second)
	fmt.Printf("  poll after work: %v\n", getJSON("http://localhost:8080/jobs/"+id))
	fmt.Println("\n✓ the GET handler awaited the result from just the ID — no shared future.")
}

func postJSON(url string) map[string]string {
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	var m map[string]string
	json.NewDecoder(resp.Body).Decode(&m)
	return m
}

func getJSON(url string) string {
	resp, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func databaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:runnerq@localhost:5432/runnerq"
}
