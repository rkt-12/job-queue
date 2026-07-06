package queue

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
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
