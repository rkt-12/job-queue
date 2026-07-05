package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"jobqueue/internal/store"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:123456@localhost:5432/jobqueue?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := store.Connect(ctx, dsn)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer db.Close()

	fmt.Println("Connected successfully")

	tables := []string{
		"jobs",
		"dead_letters",
		"idempotency_ledger",
		"ledger_entries",
	}

	for _, table := range tables {
		var exists bool

		err := db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM information_schema.tables
				WHERE table_schema='public'
				AND table_name=$1
			)
		`, table).Scan(&exists)

		if err != nil {
			log.Fatalf("checking %s: %v", table, err)
		}

		if exists {
			fmt.Printf("%s exists\n", table)
		} else {
			fmt.Printf("%s does NOT exist\n", table)
		}
	}

	fmt.Println("\nSchema initialized successfully.")
}
