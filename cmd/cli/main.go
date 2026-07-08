// This file creates the CLI commands for interacting with the job queue

package main

import (
	"context"
	"encoding/json"
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

	if len(os.Args) < 2 {
		usage()
		return
	}

	switch os.Args[1] {
	case "enqueue":
		// usage: cli enqueue <type> '<jsonPayload>' [priority] [maxAttempts]
		if len(os.Args) < 4 {
			usage()
			return
		}
		jobType, payloadJSON := os.Args[2], os.Args[3]
		var payload any
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			fmt.Println("bad payload json:", err)
			os.Exit(1)
		}
		opts := queue.EnqueueOpts{Priority: "default", MaxAttempts: 5}
		if len(os.Args) > 4 {
			opts.Priority = os.Args[4]
		}
		if len(os.Args) > 5 {
			fmt.Sscanf(os.Args[5], "%d", &opts.MaxAttempts)
		}
		id, err := q.Enqueue(ctx, jobType, payload, opts)
		if err != nil {
			fmt.Println("error:", err)
			os.Exit(1)
		}
		fmt.Println("enqueued", id)

	case "stats":
		s, err := q.GetStats(ctx)
		if err != nil {
			fmt.Println("error:", err)
			os.Exit(1)
		}
		b, _ := json.MarshalIndent(s, "", "  ")
		fmt.Println(string(b))

	case "dlq":
		list, err := q.ListDeadLetters(ctx, 50)
		if err != nil {
			fmt.Println("error:", err)
			os.Exit(1)
		}
		b, _ := json.MarshalIndent(list, "", "  ")
		fmt.Println(string(b))

	case "replay":
		if len(os.Args) < 3 {
			usage()
			return
		}
		jobID, err := q.ReplayDeadLetter(ctx, os.Args[2])
		if err != nil {
			fmt.Println("error:", err)
			os.Exit(1)
		}
		fmt.Println("replayed as job", jobID)

	default:
		usage()
	}
}

func usage() {
	fmt.Println(`usage:
  cli enqueue <type> '<jsonPayload>' [priority] [maxAttempts]
  cli stats
  cli dlq
  cli replay <deadLetterId>`)
}
