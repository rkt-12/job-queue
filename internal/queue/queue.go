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

// Claim is a locked-out job ready for processing, returned by Dequeue.
type Claim struct{ Job Job }

const priorityOrder = `CASE queue WHEN 'high' THEN 0 WHEN 'default' THEN 1 WHEN 'low' THEN 2 ELSE 3 END`

// Dequeue takes the highest-priority, earliest-due, dependency-satisfied pending job
// and marks it "processing" with a visibility-timeout lock in the
// UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED) statement. SKIP LOCKED is what
// guarantees two workers can never claim the same row: if worker B's SELECT scans a row
// worker A's transaction already locked, B just skips it and looks at the next candidate
// instead of blocking. Returns (nil, nil) if there's nothing to do right now.
func (q *Queue) Dequeue(ctx context.Context, workerName string, visibilityTimeout time.Duration) (*Claim, error) {
	row := q.DB.QueryRowContext(ctx, `
		UPDATE jobs
		SET status = 'processing',
		    locked_by = $1,
		    locked_until = now() + $2::interval,
		    updated_at = now()
		WHERE id = (
			SELECT j.id
			FROM jobs j
			WHERE j.status = 'pending'
			  AND j.run_at <= now()
			  AND (
			        j.depends_on IS NULL
			        OR EXISTS (SELECT 1 FROM jobs d WHERE d.id = j.depends_on AND d.status = 'completed')
			      )
			ORDER BY `+priorityOrder+`, j.run_at ASC
			FOR UPDATE OF j SKIP LOCKED
			LIMIT 1
		)
		RETURNING id, type, queue, payload, status, attempts, max_attempts, idempotency_key, depends_on
	`, workerName, fmt.Sprintf("%d milliseconds", visibilityTimeout.Milliseconds()))

	var j Job
	err := row.Scan(&j.ID, &j.Type, &j.Queue, &j.Payload, &j.Status, &j.Attempts, &j.MaxAttempts, &j.IdempotencyKey, &j.DependsOn)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("dequeue: %w", err)
	}
	return &Claim{Job: j}, nil
}

// MarkSuccess completes the job, records the idempotency result (if any), and
// unblocks any jobs whose depends_on points at this one — they simply become
// eligible for the next Dequeue's EXISTS check, no explicit "wake up" needed.
func (q *Queue) MarkSuccess(ctx context.Context, job Job, result any) error {
	body, _ := json.Marshal(result)
	tx, err := q.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET status='completed', updated_at=now() WHERE id=$1`, job.ID); err != nil {
		return err
	}
	if job.IdempotencyKey != nil {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO idempotency_ledger (idempotency_key, result, completed_at) VALUES ($1,$2,now())
			ON CONFLICT (idempotency_key) DO NOTHING
		`, *job.IdempotencyKey, body); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// AlreadyCompleted checks the idempotency ledger BEFORE any side-effecting work runs.
// Returns (result, true) if found, (nil, false) if not.
func (q *Queue) AlreadyCompleted(ctx context.Context, idempotencyKey *string) (json.RawMessage, bool, error) {
	if idempotencyKey == nil || *idempotencyKey == "" {
		return nil, false, nil
	}
	var result json.RawMessage
	err := q.DB.QueryRowContext(ctx, `SELECT result FROM idempotency_ledger WHERE idempotency_key=$1`, *idempotencyKey).Scan(&result)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return result, true, nil
}
