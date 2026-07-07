// this file defines the actual work that the worker does for each job type
package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

// Handler receives the raw payload and the job's idempotency key (if any) and either returns a
// JSON-able result (success) or an error (triggers the retry/backoff/DLQ path in the worker).
type Handler func(ctx context.Context, db *sql.DB, payload json.RawMessage, idempotencyKey *string) (any, error)

var Registry = map[string]Handler{
	"flaky-task":     flakyTask,
	"always-fails":   alwaysFails,
	"charge-account": chargeAccount,
}

// flakyTask simulates flaky external work: fails the first N times then succeeds.
// Used to demonstrate retries with exponential backoff actually recovering.
func flakyTask(ctx context.Context, db *sql.DB, payload json.RawMessage, idemKey *string) (any, error) {
	var p struct {
		ID               string `json:"id"`
		FailUntilAttempt int    `json:"failUntilAttempt"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/tmp/flaky-%s.count", p.ID)
	count := 0
	if b, err := os.ReadFile(path); err == nil {
		count, _ = strconv.Atoi(string(b))
	}
	count++
	_ = os.WriteFile(path, []byte(strconv.Itoa(count)), 0644)
	if count <= p.FailUntilAttempt {
		return nil, fmt.Errorf("simulated transient failure (attempt %d)", count)
	}
	return map[string]any{"ok": true, "attempt": count}, nil
}

// alwaysFails demonstrates the dead-letter path.
func alwaysFails(ctx context.Context, db *sql.DB, payload json.RawMessage, idemKey *string) (any, error) {
	return nil, fmt.Errorf("this job type always fails, by design")
}

// chargeAccount demonstrates idempotency: re-running a partially completed job
// must NOT double-apply the side effect. ledger_entries.idempotency_key is a
// UNIQUE column, so even a race between two workers momentarily believing they
// both own this job would still only ever produce one row — the guard is a real database constraint
func chargeAccount(ctx context.Context, db *sql.DB, payload json.RawMessage, idemKey *string) (any, error) {
	var p struct {
		Account         string `json:"account"`
		AmountCents     int    `json:"amountCents"`
		CrashAfterWrite bool   `json:"crashAfterWrite"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, err
	}

	if idemKey != nil {
		var existingID int
		err := db.QueryRowContext(ctx, `SELECT id FROM ledger_entries WHERE idempotency_key=$1`, *idemKey).Scan(&existingID)
		if err == nil {
			// Side effect already applied in a previous (crashed/retried) attempt —
			// return the prior result instead of writing again.
			return map[string]any{"alreadyApplied": true, "entryId": existingID}, nil
		}
	}

	var entryID int
	err := db.QueryRowContext(ctx, `
		INSERT INTO ledger_entries (idempotency_key, account, amount_cents, created_at)
		VALUES ($1,$2,$3, now()) RETURNING id
	`, idemKey, p.Account, p.AmountCents).Scan(&entryID)
	if err != nil {
		return nil, fmt.Errorf("write ledger entry: %w", err)
	}

	// Simulate a crash AFTER the side effect but BEFORE the job is marked
	// complete, to prove the idempotency guard (not "don't crash") is what
	// prevents double-apply on the retry that follows.
	if p.CrashAfterWrite {
		return nil, fmt.Errorf("simulated crash after side-effect, before ack")
	}
	return map[string]any{"entryId": entryID}, nil
}
