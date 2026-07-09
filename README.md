# Reliable Job Queue with Dead-Letter Recovery

A background job processing engine built in Go, backed by PostgreSQL —
no message broker, no second database. Durability, job priorities, delayed execution,
crash-safe visibility timeouts, and job dependencies are all implemented as SQL against a
single `jobs` table. The dashboard looks something like this and reports every detail about the queue

![alt text](image.png)

Here, Postgres is the *only* system of record. The trick that makes this work is a single row-locking primitive:

```sql
SELECT ... FROM jobs
WHERE status = 'pending' AND run_at <= now()
ORDER BY priority, run_at
FOR UPDATE SKIP LOCKED
LIMIT 1
```

`FOR UPDATE SKIP LOCKED` guarantees that if two workers run this query at the same instant,
they can never claim the same row: whichever worker's transaction locks a row first, the other
worker's scan simply skips it and looks at the next candidate instead of blocking or double-claiming
it. That one guarantee is what makes "a job is never processed by two workers at once" true
without any application-level coordination.

## Requirements and how each is satisfied

1. **Durable queue** — jobs are rows in a Postgres table, not an in-memory structure. A crash of any process (worker, scheduler, or the database's own restart mid-transaction) leaves the job's state exactly where the last committed write left it.
2. **Priorities and delayed execution** — a `queue` column (`high`/`default`/`low`) drives `ORDER BY`, and a `run_at` timestamp column means a "delayed" job is just a pending row that isn't eligible yet. No separate scheduler queue or promotion step is needed — the same dequeue query naturally skips jobs whose time hasn't come.
3. **Retries with exponential backoff** — on failure, `attempts` increments and the job returns to `pending` with `run_at = now() + backoff`, where backoff is `min(1s * 2^attempts, 5min)` plus jitter, capped by `max_attempts`.
4. **Dead-letter queue** — once `attempts >= max_attempts`, the job moves to a `dead_letters` table with its payload, error, and attempt count preserved. Dead letters can be listed and replayed (reset to `pending` with a fresh attempt count) via the CLI, HTTP API, or dashboard.
5. **Never processed twice, crash-safe** — claiming a job sets `status='processing'`, `locked_by`, and `locked_until` (a visibility timeout) in the same atomic `UPDATE`. If the worker that claimed it dies before finishing, the row is invisible to every dequeue query until `locked_until` passes, at which point a background reaper reclaims it and routes it through the same retry/backoff/dead-letter logic as any other failure.
6. **Observability** — queue depth, in-flight job counts, job counts by status, and dead-letter contents are all exposed via CLI commands and an HTTP API, plus a live-updating dashboard.
7. **Idempotency** — an `idempotency_ledger` table records completed side effects by a caller-supplied key, checked before a handler runs. The example `charge-account` handler additionally guards its own side-effect table with a `UNIQUE` constraint on that key, so even a job that crashes after applying its effect but before being acknowledged will not double-apply it on retry.

**Stretch goals implemented:**
- **Job dependencies** — a job enqueued with `dependsOn: <jobId>` only becomes eligible once that job's status is `completed`. This is checked inline in the dequeue query (`EXISTS (...)`), so there's no separate "unblock waiting jobs" step to get wrong.
- **Live web dashboard** — a single HTML page polling the stats/DLQ endpoints every 2 seconds, with a one-click replay button per dead letter.

## Architecture

```
enqueue()  →  INSERT INTO jobs (status='pending', run_at=..., depends_on=...)
                                │
                    worker calls Dequeue():
                    UPDATE jobs SET status='processing', locked_until=now()+30s
                    WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1)
                                │
                 check idempotency_ledger for this job's key
                                │
                      run the registered handler
                        │                    │
                     success               failure
                        │                    │
              status='completed'      attempts < max?  → status='pending', run_at=now()+backoff
              + record idempotency    attempts >= max? → row in dead_letters, status='dead'
              + unblock dependents

  scheduler (runs every 1s):
    find rows WHERE status='processing' AND locked_until < now()
    → treat each as a failure (crashed worker) → same retry/backoff/dead-letter path
```

## Project layout

```
go.mod, go.sum                    Module definition — one dependency: github.com/lib/pq
internal/store/store.go           DB connection + schema (jobs, dead_letters, idempotency_ledger, ledger_entries)
internal/queue/queue.go           Core engine: Enqueue, Dequeue, MarkSuccess/MarkFailure, backoff, ReapStale, stats, DLQ
internal/handlers/handlers.go     Job type implementations: flaky-task, always-fails, charge-account
cmd/worker/main.go                Worker process: dequeue → check idempotency → run handler → ack/fail
cmd/scheduler/main.go             Reaper process: reclaims jobs abandoned by crashed workers
cmd/server/main.go                HTTP API (stdlib net/http) + serves the dashboard
cmd/cli/main.go                   Terminal commands: enqueue, stats, dlq, replay
cmd/demo/main.go                  Scripted demo that enqueues jobs exercising every requirement
web/dashboard.html                Live queue-health dashboard (polls /stats and /dlq)
```

## Prerequisites

- Go 1.22 or later
- PostgreSQL 12 or later (any version with `SKIP LOCKED` support, which has existed since 9.5)

## Setup

Start Postgres 
```

Set the connection string (needed in every terminal you use below):

```bash
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/jobqueue?sslmode=disable"
```

Fetch the dependency and build all five binaries:

```bash
go mod tidy
go build -o bin/worker    ./cmd/worker
go build -o bin/scheduler ./cmd/scheduler
go build -o bin/server    ./cmd/server
go build -o bin/cli       ./cmd/cli
go build -o bin/demo      ./cmd/demo
```

The schema is created automatically the first time any binary connects — there is no separate
migration command to run. Schema creation is guarded by a Postgres advisory lock, so it's safe
for multiple processes to start at the same time on first boot.

## Running it

Open three terminals (each needs `DATABASE_URL` set):

```bash
# Terminal 1 — reaps jobs abandoned by crashed workers
./bin/scheduler

# Terminal 2 — run this in 2-3 separate terminals to see load spread across workers
./bin/worker

# Terminal 3 — HTTP API and dashboard
./bin/server
```

`server` prints `listening on http://localhost:3000`.

## Trying the demo

In a fourth terminal:

```bash
./bin/demo
```

This enqueues jobs that exercise every requirement in one pass:
- a low-priority and a high-priority job — the high-priority one gets picked up first even though it was enqueued second
- a job delayed by 8 seconds — won't be eligible until then
- a job that fails twice then succeeds — watch the backoff delay grow between attempts in the worker logs
- a job that always fails — watch it land in the dead-letter queue after its attempt cap
- two jobs sharing an idempotency key — the second is skipped rather than reapplied
- two dependent jobs — the second only becomes eligible once the first completes

Watch it happen live at **http://localhost:3000**, or via the worker/scheduler terminal logs.

## CLI reference

```bash
./bin/cli enqueue <type> '<jsonPayload>' [priority] [maxAttempts]
./bin/cli stats
./bin/cli dlq
./bin/cli replay <deadLetterId>
```

Example:

```bash
./bin/cli enqueue flaky-task '{"id":"manual-1","failUntilAttempt":0}' high 5
```

## HTTP API reference

| Method & path | Purpose |
|---|---|
| `POST /jobs` | Enqueue a job. Body: `{"type","payload","priority","delayMs","maxAttempts","idempotencyKey","dependsOn"}` |
| `GET /stats` | Queue depth per priority, in-flight counts, job counts by status, dead-letter count |
| `GET /dlq` | List dead-lettered jobs |
| `POST /dlq/{id}/replay` | Reset a dead-lettered job to pending with a fresh attempt count |
| `GET /` | Live dashboard |

Example:

```bash
curl -X POST localhost:3000/jobs \
  -d '{"type":"flaky-task","payload":{"id":"api-test","failUntilAttempt":0},"priority":"high"}'
```

## Proving crash recovery (visibility timeout)

The real test — kill a worker mid-job:

```bash
./bin/cli enqueue flaky-task '{"id":"crash-test","failUntilAttempt":5}'
```

Watch the worker terminal for `picked up crash-test`, then immediately `kill -9` that worker
process. Within the 30-second visibility timeout, the scheduler terminal prints
`reclaimed 1 stale job(s) from dead workers`, and the job returns to `pending` for another
worker to claim — proving it was neither lost nor processed twice.

A faster way to trigger the same path without waiting on a real crash or the full timeout —
force a job into a stale locked state directly:

```bash
psql "$DATABASE_URL" -c "
  UPDATE jobs
  SET status='processing', locked_by='simulated-dead-worker', locked_until = now() - interval '1 minute'
  WHERE status='pending' LIMIT 1;
"
```

The scheduler reclaims it on its next tick (every second), without needing a real worker to die.

## Proving idempotency

The `charge-account` handler checks a `ledger_entries` table (keyed by a `UNIQUE`
`idempotency_key` column) for an existing row before writing a new one. Pass
`"crashAfterWrite": true` in the payload to simulate a failure that happens *after* the side
effect runs but *before* the job is acknowledged:

```bash
./bin/cli enqueue charge-account '{"account":"acct_1","amountCents":500,"crashAfterWrite":true}'
```

The job will retry (since the handler returned an error), but the retry's handler call finds
the existing ledger row and returns `{"alreadyApplied": true}` instead of writing a second one.
Confirm directly:

```bash
psql "$DATABASE_URL" -c "SELECT * FROM ledger_entries;"
```

Only one row will exist regardless of how many times the job retries.

## Shutting down

`Ctrl+C` in the worker, scheduler, and server terminals — all three handle `SIGINT`/`SIGTERM`
and exit gracefully. If you started Postgres via Docker: `docker stop jobqueue-pg`.
