// Package - store has the database connection and schema. Durability, priority
// ordering, delayed execution, visibility timeout, and dependency gating are all expressed as SQL

package store

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

const schema = `
CREATE TABLE IF NOT EXISTS jobs (
  id              TEXT PRIMARY KEY,
  type            TEXT NOT NULL,
  queue           TEXT NOT NULL DEFAULT 'default',   -- priority: high | default | low
  payload         JSONB NOT NULL,
  status          TEXT NOT NULL,                     -- pending | processing | completed | dead
  attempts        INT NOT NULL DEFAULT 0,
  max_attempts    INT NOT NULL DEFAULT 5,
  idempotency_key TEXT,
  run_at          TIMESTAMPTZ NOT NULL DEFAULT now(), -- delayed execution: don't dequeue before this
  depends_on      TEXT REFERENCES jobs(id),           -- stretch goal: simple job dependencies
  locked_by       TEXT,                               -- which worker currently owns this job
  locked_until    TIMESTAMPTZ,                        -- visibility timeout: lock expires here
  last_error      TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Partial index: the dequeue query only ever looks at pending jobs, so only index those.
CREATE INDEX IF NOT EXISTS idx_jobs_dequeue ON jobs (queue, run_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_jobs_idempotency ON jobs(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_jobs_locked_until ON jobs(locked_until) WHERE status = 'processing';

CREATE TABLE IF NOT EXISTS dead_letters (
  id         TEXT PRIMARY KEY,
  job_id     TEXT NOT NULL,
  type       TEXT NOT NULL,
  queue      TEXT NOT NULL,
  payload    JSONB NOT NULL,
  attempts   INT NOT NULL,
  error      TEXT,
  failed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotency ledger: proof a job's side effect already ran. Handlers must
-- consult this (or their own equivalent, see example charge-account handler)
-- BEFORE applying any external side effect, so a retried/reclaimed job never
-- double-applies it.
CREATE TABLE IF NOT EXISTS idempotency_ledger (
  idempotency_key TEXT PRIMARY KEY,
  result          JSONB,
  completed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Example business table the demo "charge-account" job writes to, to prove
-- idempotency in practice rather than just in theory.
CREATE TABLE IF NOT EXISTS ledger_entries (
  id              SERIAL PRIMARY KEY,
  idempotency_key TEXT UNIQUE,
  account         TEXT,
  amount_cents    INT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
`

func Connect(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	db.SetMaxOpenConns(10)
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	// Multiple processes (worker, scheduler, server) all call Connect() on startup
	// and race to run "CREATE TABLE". Postgres's catalog writes for a
	// brand-new table aren't safe under that race, so serialize migration with a
	// session-scoped advisory lock — cheap, and only matters on first boot.
	// pg_advisory_lock/unlock must run on the same underlying connection, so reserve
	// one explicitly rather than letting the pool pick a different one for each call.
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("reserve connection: %w", err)
	}
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock(727271)`); err != nil {
		conn.Close()
		return nil, fmt.Errorf("acquire migration lock: %w", err)
	}
	_, migrateErr := conn.ExecContext(ctx, schema)
	_, unlockErr := conn.ExecContext(ctx, `SELECT pg_advisory_unlock(727271)`)
	conn.Close()
	if migrateErr != nil {
		return nil, fmt.Errorf("migrate schema: %w", migrateErr)
	}
	if unlockErr != nil {
		return nil, fmt.Errorf("release migration lock: %w", unlockErr)
	}
	return db, nil
}
