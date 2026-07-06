package queue

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

type Queue struct {
	DB *sql.DB
}

func New(db *sql.DB) *Queue { return &Queue{DB: db} }

// Job mirrors a row of the jobs table.
type Job struct {
	ID             string
	Type           string
	Queue          string
	Payload        json.RawMessage
	Status         string
	Attempts       int
	MaxAttempts    int
	IdempotencyKey *string
	DependsOn      *string
}

// generates random job IDs
func newID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// EnqueueOpts configures a job. Zero values gives defaults.
type EnqueueOpts struct {
	Priority       string // high | default | low (default: "default")
	DelayMs        int64  // run no earlier than now+DelayMs
	MaxAttempts    int    // default: 5
	IdempotencyKey string
	DependsOn      string // job ID that must complete first
}

// Enqueue inserts a new job row. Whether it's immediate, scheduled, or
// blocked on a dependency, it's a "pending" row with different run_at / depends_on values
func (q *Queue) Enqueue(ctx context.Context, jobType string, payload any, opts EnqueueOpts) (string, error) {
	id := newID()
	priority := opts.Priority
	if priority == "" {
		priority = "default"
	}
	maxAttempts := opts.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 5
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}
	runAt := time.Now()
	if opts.DelayMs > 0 {
		runAt = runAt.Add(time.Duration(opts.DelayMs) * time.Millisecond)
	}
	var idemKey, dependsOn *string
	if opts.IdempotencyKey != "" {
		idemKey = &opts.IdempotencyKey
	}
	if opts.DependsOn != "" {
		dependsOn = &opts.DependsOn
	}

	_, err = q.DB.ExecContext(ctx, `
		INSERT INTO jobs (id, type, queue, payload, status, max_attempts, idempotency_key, run_at, depends_on, created_at, updated_at)
		VALUES ($1,$2,$3,$4,'pending',$5,$6,$7,$8, now(), now())
	`, id, jobType, priority, body, maxAttempts, idemKey, runAt, dependsOn)
	if err != nil {
		return "", fmt.Errorf("insert job: %w", err)
	}
	return id, nil
}
