package main

import (
	"context"
	"fmt"
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
		fmt.Println("connect error:", err)
		os.Exit(1)
	}
	defer db.Close()
	q := queue.New(db)

	fmt.Println("--- Enqueuing demo jobs (run `go run ./cmd/scheduler` and `go run ./cmd/worker` in other terminals) ---")

	// 1. Priority ordering: low enqueued first, high should still be picked up first.
	must(q.Enqueue(ctx, "flaky-task", map[string]any{"id": "low-1", "failUntilAttempt": 0}, queue.EnqueueOpts{Priority: "low"}))
	must(q.Enqueue(ctx, "flaky-task", map[string]any{"id": "high-1", "failUntilAttempt": 0}, queue.EnqueueOpts{Priority: "high"}))

	// 2. Scheduled/delayed execution — won't be eligible for 8s.
	must(q.Enqueue(ctx, "flaky-task", map[string]any{"id": "delayed-1", "failUntilAttempt": 0}, queue.EnqueueOpts{DelayMs: 8000}))

	// 3. Retries with exponential backoff, eventually succeeds (fails twice, succeeds on 3rd).
	os.Remove("/tmp/flaky-retry-demo.count")
	must(q.Enqueue(ctx, "flaky-task", map[string]any{"id": "retry-demo", "failUntilAttempt": 2}, queue.EnqueueOpts{MaxAttempts: 5}))

	// 4. Exceeds max retries -> dead-letter queue.
	must(q.Enqueue(ctx, "always-fails", map[string]any{}, queue.EnqueueOpts{MaxAttempts: 2}))

	// 5. Idempotency: same idempotencyKey enqueued twice — side effect must apply once.
	key := fmt.Sprintf("charge-demo-%d", os.Getpid())
	must(q.Enqueue(ctx, "charge-account", map[string]any{"account": "acct_123", "amountCents": 500}, queue.EnqueueOpts{IdempotencyKey: key}))
	must(q.Enqueue(ctx, "charge-account", map[string]any{"account": "acct_123", "amountCents": 500}, queue.EnqueueOpts{IdempotencyKey: key}))

	// 6. Job dependency: dep must complete before dependent becomes eligible.
	depID := must(q.Enqueue(ctx, "flaky-task", map[string]any{"id": "dep-parent", "failUntilAttempt": 0}, queue.EnqueueOpts{}))
	must(q.Enqueue(ctx, "flaky-task", map[string]any{"id": "dep-child", "failUntilAttempt": 0}, queue.EnqueueOpts{DependsOn: depID}))

	fmt.Println("Enqueued. Watch worker/scheduler logs, or open http://localhost:3000")
}

func must(id string, err error) string {
	if err != nil {
		fmt.Println("enqueue error:", err)
		os.Exit(1)
	}
	return id
}
