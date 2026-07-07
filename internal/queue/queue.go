package queue

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
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

const baseBackoff = time.Second
const maxBackoff = 5 * time.Minute

// computebackoff determines how long to wait before retrying a failed job.
// without this a failing job would be retried again immediately
func ComputeBackoff(attempts int) time.Duration {
	exp := float64(baseBackoff) * math.Pow(2, float64(attempts))
	if exp > float64(maxBackoff) {
		exp = float64(maxBackoff)
	}
	jitterN, _ := rand.Int(rand.Reader, big.NewInt(int64(exp*0.2)+1))
	return time.Duration(exp) + time.Duration(jitterN.Int64())
}

type FailOutcome struct {
	Dead     bool
	Attempts int
	Delay    time.Duration
}

// this  is responsible for handling what happens after a job execution fails.
// It decides whether the job should be retried or moved to the dead-letter queue.
// this handles both a genuine handler error AND a reclaimed (crashed-worker)
// job identically — from the queue's point of view a job that never got acked and a
// job that explicitly errored are the same event: an attempt that didn't succeed.
func (q *Queue) MarkFailure(ctx context.Context, job Job, errMsg string) (FailOutcome, error) {
	attempts := job.Attempts + 1
	if attempts >= job.MaxAttempts {
		tx, err := q.DB.BeginTx(ctx, nil)
		if err != nil {
			return FailOutcome{}, err
		}
		defer tx.Rollback()

		if _, err := tx.ExecContext(ctx, `
			INSERT INTO dead_letters (id, job_id, type, queue, payload, attempts, error, failed_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7, now())
		`, newID(), job.ID, job.Type, job.Queue, job.Payload, attempts, errMsg); err != nil {
			return FailOutcome{}, err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE jobs SET status='dead', attempts=$1, last_error=$2, updated_at=now() WHERE id=$3
		`, attempts, errMsg, job.ID); err != nil {
			return FailOutcome{}, err
		}
		if err := tx.Commit(); err != nil {
			return FailOutcome{}, err
		}
		return FailOutcome{Dead: true, Attempts: attempts}, nil
	}

	delay := ComputeBackoff(attempts)
	_, err := q.DB.ExecContext(ctx, `
		UPDATE jobs
		SET status='pending', attempts=$1, last_error=$2, run_at=now()+$3::interval,
		    locked_by=NULL, locked_until=NULL, updated_at=now()
		WHERE id=$4
	`, attempts, errMsg, fmt.Sprintf("%d milliseconds", delay.Milliseconds()), job.ID)
	if err != nil {
		return FailOutcome{}, err
	}
	return FailOutcome{Dead: false, Attempts: attempts, Delay: delay}, nil
}

// this is the crash recovery mechanism. It detects jobs that were claimed by a worker but never completed
// because the worker crashed, hung, or was killed, and makes sure those jobs don't remain stuck forever
// any job still "processing" past its locked_until is presumed to belong to a dead worker (it crashed, was
// killed, or the process hung) and is routed through the exact same
// retry/backoff/DLQ path as a normal failure. Runs inside a transaction so
// FOR UPDATE SKIP LOCKED actually holds the lock for the duration of the reap
// (a bare autocommit SELECT would release it instantly), preventing two reaper
// runs from double-processing the same stale row.
func (q *Queue) ReapStale(ctx context.Context) (int, error) {
	tx, err := q.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT id, type, queue, payload, status, attempts, max_attempts, idempotency_key, depends_on
		FROM jobs
		WHERE status='processing' AND locked_until < now()
		FOR UPDATE SKIP LOCKED
	`)
	if err != nil {
		return 0, err
	}
	var stale []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.ID, &j.Type, &j.Queue, &j.Payload, &j.Status, &j.Attempts, &j.MaxAttempts, &j.IdempotencyKey, &j.DependsOn); err != nil {
			rows.Close()
			return 0, err
		}
		stale = append(stale, j)
	}
	rows.Close()
	if err := tx.Commit(); err != nil {
		return 0, err
	}

	for _, j := range stale {
		if _, err := q.MarkFailure(ctx, j, "worker died / visibility timeout exceeded"); err != nil {
			return 0, err
		}
	}
	return len(stale), nil
}

type Stats struct {
	ByStatus map[string]int `json:"byStatus"`
	ByQueue  map[string]int `json:"byQueue"`
	Dead     int            `json:"deadLetterCount"`
}

// This gives three kinds of stats: how many jobs are in each status,
// how many pending jobs are in each queue, and how many dead-lettered jobs exist.
func (q *Queue) GetStats(ctx context.Context) (Stats, error) {
	s := Stats{ByStatus: map[string]int{}, ByQueue: map[string]int{}}
	rows, err := q.DB.QueryContext(ctx, `SELECT status, COUNT(*) FROM jobs GROUP BY status`)
	if err != nil {
		return s, err
	}
	for rows.Next() {
		var status string
		var c int
		if err := rows.Scan(&status, &c); err != nil {
			rows.Close()
			return s, err
		}
		s.ByStatus[status] = c
	}
	rows.Close()

	rows, err = q.DB.QueryContext(ctx, `SELECT queue, COUNT(*) FROM jobs WHERE status='pending' GROUP BY queue`)
	if err != nil {
		return s, err
	}
	for rows.Next() {
		var qn string
		var c int
		if err := rows.Scan(&qn, &c); err != nil {
			rows.Close()
			return s, err
		}
		s.ByQueue[qn] = c
	}
	rows.Close()

	if err := q.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letters`).Scan(&s.Dead); err != nil {
		return s, err
	}
	return s, nil
}

type DeadLetter struct {
	ID       string          `json:"id"`
	JobID    string          `json:"job_id"`
	Type     string          `json:"type"`
	Queue    string          `json:"queue"`
	Payload  json.RawMessage `json:"payload"`
	Attempts int             `json:"attempts"`
	Error    string          `json:"error"`
	FailedAt time.Time       `json:"failed_at"`
}

// Lists the most recent dead-lettered jobs, up to the specified limit. This is for
// monitoring and debugging purposes, not for any kind of automated retry logic.
func (q *Queue) ListDeadLetters(ctx context.Context, limit int) ([]DeadLetter, error) {
	rows, err := q.DB.QueryContext(ctx, `
		SELECT id, job_id, type, queue, payload, attempts, COALESCE(error,''), failed_at
		FROM dead_letters ORDER BY failed_at DESC LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeadLetter
	for rows.Next() {
		var d DeadLetter
		if err := rows.Scan(&d.ID, &d.JobID, &d.Type, &d.Queue, &d.Payload, &d.Attempts, &d.Error, &d.FailedAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}

// This allows to retry a job that previously exhausted all its attempts and was moved to DLQ
// It resets the original job to pending with a fresh attempt count
// and removes it from the dead-letter table — it will be picked up by the exact
// same Dequeue query as any other pending job, no special-casing needed.
func (q *Queue) ReplayDeadLetter(ctx context.Context, deadID string) (string, error) {
	var jobID string
	err := q.DB.QueryRowContext(ctx, `SELECT job_id FROM dead_letters WHERE id=$1`, deadID).Scan(&jobID)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("dead letter not found")
	}
	if err != nil {
		return "", err
	}
	tx, err := q.DB.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
		UPDATE jobs SET status='pending', attempts=0, last_error=NULL, run_at=now(),
		       locked_by=NULL, locked_until=NULL, updated_at=now()
		WHERE id=$1
	`, jobID); err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM dead_letters WHERE id=$1`, deadID); err != nil {
		return "", err
	}
	return jobID, tx.Commit()
}
