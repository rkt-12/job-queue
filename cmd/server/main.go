package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"

	"jobqueue/internal/queue"
	"jobqueue/internal/store"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/jobqueue?sslmode=disable"
	}
	ctx := context.Background()
	db, err := store.Connect(ctx, dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer db.Close()
	q := queue.New(db)

	mux := http.NewServeMux()

	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Type           string          `json:"type"`
			Payload        json.RawMessage `json:"payload"`
			Priority       string          `json:"priority"`
			DelayMs        int64           `json:"delayMs"`
			MaxAttempts    int             `json:"maxAttempts"`
			IdempotencyKey string          `json:"idempotencyKey"`
			DependsOn      string          `json:"dependsOn"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if body.Type == "" {
			http.Error(w, `{"error":"type is required"}`, 400)
			return
		}
		var payload any = map[string]any{}
		if len(body.Payload) > 0 {
			payload = body.Payload
		}
		id, err := q.Enqueue(r.Context(), body.Type, payload, queue.EnqueueOpts{
			Priority: body.Priority, DelayMs: body.DelayMs, MaxAttempts: body.MaxAttempts,
			IdempotencyKey: body.IdempotencyKey, DependsOn: body.DependsOn,
		})
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, map[string]string{"id": id})
	})

	// Requirement 06: inspect queue depth, in-flight jobs, dead-letter contents
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		s, err := q.GetStats(r.Context())
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, s)
	})

	mux.HandleFunc("GET /dlq", func(w http.ResponseWriter, r *http.Request) {
		list, err := q.ListDeadLetters(r.Context(), 50)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		if list == nil {
			list = []queue.DeadLetter{}
		}
		writeJSON(w, list)
	})

	mux.HandleFunc("POST /dlq/{id}/replay", func(w http.ResponseWriter, r *http.Request) {
		jobID, err := q.ReplayDeadLetter(r.Context(), r.PathValue("id"))
		if err != nil {
			http.Error(w, err.Error(), 404)
			return
		}
		writeJSON(w, map[string]string{"replayed": jobID})
	})

	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "web/dashboard.html")
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}
	log.Printf("[server] listening on http://localhost:%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
