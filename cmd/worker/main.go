// This is the worker process of queue. It continuously polls PostgreSQL for eligible jobs,
// executes the appropriate handler, and records the outcome (success or failure).
//  It ties together all the queue features implemented.

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"jobqueue/internal/handlers"
	"jobqueue/internal/queue"
	"jobqueue/internal/store"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const visibilityTimeout = 30 * time.Second

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/jobqueue?sslmode=disable"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM) // listens to ctrl+c and SIGTERM for graceful shutdown
	defer stop()

	db, err := store.Connect(ctx, dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer db.Close()

	q := queue.New(db)
	name := "worker-" + hex.EncodeToString(randBytes(4))
	log.Printf("[worker] %s starting (pid %d)", name, os.Getpid())

	for {
		select {
		case <-ctx.Done():
			log.Println("[worker] shutting down gracefully")
			return
		default:
		}

		claim, err := q.Dequeue(ctx, name, visibilityTimeout)
		if err != nil {
			log.Printf("[worker] dequeue error: %v", err)
			time.Sleep(time.Second)
			continue
		}
		if claim == nil {
			time.Sleep(500 * time.Millisecond) // nothing to do — poll interval
			continue
		}

		job := claim.Job
		log.Printf("[worker] picked up %s (%s, attempt %d/%d)", job.ID, job.Type, job.Attempts+1, job.MaxAttempts)

		// --- Idempotency gate ---
		if result, done, _ := q.AlreadyCompleted(ctx, job.IdempotencyKey); done {
			log.Printf("[worker] job %s idempotency_key already completed — skipping side effect, marking done", job.ID)
			var parsed any
			_ = json.Unmarshal(result, &parsed)
			_ = q.MarkSuccess(ctx, job, parsed)
			continue
		}

		handler, ok := handlers.Registry[job.Type]
		if !ok {
			outcome, _ := q.MarkFailure(ctx, job, "no handler registered for type \""+job.Type+"\"")
			logOutcome(job.ID, outcome)
			continue
		}

		//executes business logic
		result, err := handler(ctx, db, job.Payload, job.IdempotencyKey)
		if err != nil {
			outcome, ferr := q.MarkFailure(ctx, job, err.Error())
			if ferr != nil {
				log.Printf("[worker] failed to record failure for %s: %v", job.ID, ferr)
				continue
			}
			logOutcome(job.ID, outcome)
			continue
		}

		if err := q.MarkSuccess(ctx, job, result); err != nil {
			log.Printf("[worker] failed to mark %s success: %v", job.ID, err)
			continue
		}
		log.Printf("[worker] job %s completed", job.ID)
	}
}

func logOutcome(jobID string, o queue.FailOutcome) {
	if o.Dead {
		log.Printf("[worker] job %s exceeded max attempts -> DEAD LETTER", jobID)
	} else {
		log.Printf("[worker] job %s failed, retry in %s (attempt %d)", jobID, o.Delay, o.Attempts)
	}
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}
