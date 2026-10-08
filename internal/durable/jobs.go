// Package durable implements the PostgreSQL accounting boundary for asynchronous
// validation. RabbitMQ transports notifications; PostgreSQL owns job state.
package durable

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/model"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	ErrConflict     = errors.New("idempotency key already used with another payload")
	ErrFull         = errors.New("pending job capacity reached")
	ErrNotFound     = errors.New("job not found")
	ErrInvalidEvent = errors.New("invalid work event")
)

type Job struct {
	ID          string     `json:"id"`
	Status      string     `json:"status"`
	Attempts    int        `json:"attempts"`
	MaxAttempts int        `json:"max_attempts"`
	AcceptedAt  time.Time  `json:"accepted_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	LastError   string     `json:"last_error,omitempty"`
}

type Event struct {
	JobID   string `json:"job_id"`
	Attempt int    `json:"attempt"`
}

type Counts struct {
	Accepted      int64 `json:"accepted"`
	Completed     int64 `json:"completed"`
	Failed        int64 `json:"failed"`
	Pending       int64 `json:"pending"`
	Retrying      int64 `json:"retrying"`
	OutboxPending int64 `json:"outbox_pending"`
	Retries       int64 `json:"retries"`
	Duplicates    int64 `json:"duplicates"`
	Results       int64 `json:"results"`
}

type DB struct{ SQL *sql.DB }

func Open(ctx context.Context, url string) (*DB, error) {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &DB{SQL: db}, nil
}

func (d *DB) Close() error { return d.SQL.Close() }

// Schema is additive to the original validation_results adapter. The new
// result table includes a foreign key to its durable acceptance record.
const Schema = `
CREATE TABLE IF NOT EXISTS durable_jobs (
 id TEXT PRIMARY KEY, payload JSONB NOT NULL, fingerprint TEXT NOT NULL,
 status TEXT NOT NULL CHECK (status IN ('pending','retrying','completed','failed')),
 attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
 max_attempts INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 10),
 accepted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), completed_at TIMESTAMPTZ,
 last_error TEXT NOT NULL DEFAULT '', duplicates BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS durable_outbox (
 id BIGSERIAL PRIMARY KEY, job_id TEXT NOT NULL REFERENCES durable_jobs(id),
 attempt INTEGER NOT NULL, destination TEXT NOT NULL CHECK (destination IN ('work','dead')),
 available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), sent_at TIMESTAMPTZ,
 UNIQUE (job_id, attempt, destination)
);
CREATE INDEX IF NOT EXISTS durable_outbox_pending ON durable_outbox(available_at,id) WHERE sent_at IS NULL;
CREATE TABLE IF NOT EXISTS durable_results (
 job_id TEXT PRIMARY KEY REFERENCES durable_jobs(id), endpoint_id TEXT NOT NULL,
 status TEXT NOT NULL, fingerprint TEXT NOT NULL, checked_at TIMESTAMPTZ NOT NULL
);`

func (d *DB) EnsureSchema(ctx context.Context) error {
	tx, err := d.SQL.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(7343922)"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, Schema); err != nil {
		return err
	}
	return tx.Commit()
}

func RequestIdentity(key string, endpoint model.Endpoint) (id, fingerprint string, payload []byte, err error) {
	if len(key) < 1 || len(key) > 128 || strings.TrimSpace(key) != key {
		return "", "", nil, errors.New("Idempotency-Key must contain 1..128 characters without outer whitespace")
	}
	for _, r := range key {
		if r < 33 || r > 126 {
			return "", "", nil, errors.New("Idempotency-Key must be printable ASCII without spaces")
		}
	}
	if endpoint.ID == "" || endpoint.TenantID == "" || endpoint.Region == "" || endpoint.Platform == "" || endpoint.ExpectedVersion == "" || endpoint.Sequence < 0 {
		return "", "", nil, errors.New("endpoint fields are required and sequence must be nonnegative")
	}
	payload, err = json.Marshal(endpoint)
	if err != nil {
		return "", "", nil, err
	}
	digest := sha256.Sum256([]byte(key))
	payloadDigest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), hex.EncodeToString(payloadDigest[:]), payload, nil
}

// Submit commits acceptance and its notification together. A small advisory
// transaction lock serializes only admission so capacity cannot be overshot.
func (d *DB) Submit(ctx context.Context, key string, endpoint model.Endpoint, maxAttempts, capacity int) (Job, bool, error) {
	id, fingerprint, payload, err := RequestIdentity(key, endpoint)
	if err != nil {
		return Job{}, false, err
	}
	if maxAttempts < 1 || maxAttempts > 10 || capacity < 1 {
		return Job{}, false, errors.New("invalid admission controls")
	}
	tx, err := d.SQL.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, false, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(7343921)"); err != nil {
		return Job{}, false, err
	}
	var previous string
	err = tx.QueryRowContext(ctx, "SELECT fingerprint FROM durable_jobs WHERE id=$1", id).Scan(&previous)
	if err == nil {
		if previous != fingerprint {
			return Job{}, false, ErrConflict
		}
		job, err := scanJob(tx.QueryRowContext(ctx, jobQuery+" WHERE id=$1", id))
		if err != nil {
			return Job{}, false, err
		}
		return job, false, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Job{}, false, err
	}
	var pending int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM durable_jobs WHERE status IN ('pending','retrying')").Scan(&pending); err != nil {
		return Job{}, false, err
	}
	if pending >= capacity {
		return Job{}, false, ErrFull
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO durable_jobs(id,payload,fingerprint,status,max_attempts) VALUES($1,$2,$3,'pending',$4)", id, payload, fingerprint, maxAttempts); err != nil {
		return Job{}, false, err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO durable_outbox(job_id,attempt,destination) VALUES($1,1,'work')", id); err != nil {
		return Job{}, false, err
	}
	job, err := scanJob(tx.QueryRowContext(ctx, jobQuery+" WHERE id=$1", id))
	if err != nil {
		return Job{}, false, err
	}
	return job, true, tx.Commit()
}

const jobQuery = "SELECT id,status,attempts,max_attempts,accepted_at,updated_at,completed_at,last_error FROM durable_jobs"

type scanner interface{ Scan(...any) error }

func scanJob(row scanner) (Job, error) {
	var job Job
	err := row.Scan(&job.ID, &job.Status, &job.Attempts, &job.MaxAttempts, &job.AcceptedAt, &job.UpdatedAt, &job.CompletedAt, &job.LastError)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return job, err
}

func (d *DB) Get(ctx context.Context, id string) (Job, error) {
	return scanJob(d.SQL.QueryRowContext(ctx, jobQuery+" WHERE id=$1", id))
}

func (d *DB) Counts(ctx context.Context) (Counts, error) {
	var c Counts
	err := d.SQL.QueryRowContext(ctx, `SELECT count(*),
 count(*) FILTER (WHERE status='completed'), count(*) FILTER (WHERE status='failed'),
 count(*) FILTER (WHERE status='pending'), count(*) FILTER (WHERE status='retrying'),
 COALESCE(sum(GREATEST(attempts-1,0)),0), COALESCE(sum(duplicates),0),
 (SELECT count(*) FROM durable_outbox WHERE sent_at IS NULL),
 (SELECT count(*) FROM durable_results) FROM durable_jobs`).Scan(&c.Accepted, &c.Completed, &c.Failed, &c.Pending, &c.Retrying, &c.Retries, &c.Duplicates, &c.OutboxPending, &c.Results)
	return c, err
}
