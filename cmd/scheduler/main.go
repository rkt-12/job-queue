// This file periodically finds jobs abandoned by crashed workers and sends
// them through the retry/DLQ mechanism.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"jobqueue/internal/queue"
	"jobqueue/internal/store"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/jobqueue?sslmode=disable"
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := store.Connect(ctx, dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer db.Close()

	q := queue.New(db)
	log.Println("[scheduler] starting (reaps jobs left in-flight by crashed workers)")

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Println("[scheduler] shutting down")
			return
		case <-ticker.C:
			n, err := q.ReapStale(ctx)
			if err != nil {
				log.Printf("[scheduler] reap error: %v", err)
				continue
			}
			if n > 0 {
				log.Printf("[scheduler] reclaimed %d stale job(s) from dead workers", n)
			}
		}
	}
}
