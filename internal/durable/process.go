package durable

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/model"
	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/validator"
)

type Publisher interface {
	Publish(context.Context, string, Event) error
}

type Hooks struct {
	// Hooks are injectable process fault boundaries used only by recovery tests.
	AfterPublish func()
	BeforeCommit func()
	AfterCommit  func()
}

// DispatchOne holds the row lock through broker confirmation. A crash after
// confirmation but before commit leaves the event pending and may republish.
func (d *DB) DispatchOne(ctx context.Context, p Publisher, hooks Hooks) (bool, error) {
	tx, err := d.SQL.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var id int64
	var event Event
	var destination string
	err = tx.QueryRowContext(ctx, `SELECT id,job_id,attempt,destination FROM durable_outbox
 WHERE sent_at IS NULL AND available_at <= clock_timestamp() ORDER BY id
 FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&id, &event.JobID, &event.Attempt, &destination)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := p.Publish(ctx, destination, event); err != nil {
		return false, err
	}
	if hooks.AfterPublish != nil {
		hooks.AfterPublish()
	}
	if _, err := tx.ExecContext(ctx, "UPDATE durable_outbox SET sent_at=clock_timestamp() WHERE id=$1", id); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// Process commits all state transitions before the caller acknowledges delivery.
// Transient SQL failures roll back and must be requeued by the transport.
func (d *DB) Process(ctx context.Context, event Event, v validator.Validator, retryDelay time.Duration, hooks Hooks) error {
	if !jobID.MatchString(event.JobID) || event.Attempt < 1 {
		return ErrInvalidEvent
	}
	tx, err := d.SQL.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var payload []byte
	var status string
	var attempts, maxAttempts int
	err = tx.QueryRowContext(ctx, "SELECT payload,status,attempts,max_attempts FROM durable_jobs WHERE id=$1 FOR UPDATE", event.JobID).Scan(&payload, &status, &attempts, &maxAttempts)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if status == "completed" || status == "failed" || event.Attempt <= attempts {
		_, err := tx.ExecContext(ctx, "UPDATE durable_jobs SET duplicates=duplicates+1 WHERE id=$1", event.JobID)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	if event.Attempt != attempts+1 || event.Attempt > maxAttempts {
		return ErrInvalidEvent
	}
	var endpoint model.Endpoint
	if err := json.Unmarshal(payload, &endpoint); err != nil {
		return fmt.Errorf("stored endpoint: %w", err)
	}
	result, validationErr := v.Validate(ctx, endpoint)
	if validationErr == nil {
		if _, err := tx.ExecContext(ctx, `INSERT INTO durable_results(job_id,endpoint_id,status,fingerprint,checked_at)
 VALUES($1,$2,$3,$4,$5) ON CONFLICT(job_id) DO NOTHING`, event.JobID, result.EndpointID, result.Status, result.Fingerprint, result.CheckedAt); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "UPDATE durable_jobs SET status='completed',attempts=$2,updated_at=clock_timestamp(),completed_at=clock_timestamp(),last_error='' WHERE id=$1", event.JobID, event.Attempt)
	} else {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		status, destination, nextAttempt := "retrying", "work", event.Attempt+1
		if event.Attempt == maxAttempts {
			status, destination, nextAttempt = "failed", "dead", event.Attempt
		}
		message := validationErr.Error()
		if len(message) > 512 {
			message = message[:512]
		}
		_, err = tx.ExecContext(ctx, `UPDATE durable_jobs SET status=$2,attempts=$3,updated_at=clock_timestamp(),
 completed_at=CASE WHEN $2='failed' THEN clock_timestamp() ELSE NULL END,last_error=$4 WHERE id=$1`, event.JobID, status, event.Attempt, message)
		if err == nil {
			_, err = tx.ExecContext(ctx, `INSERT INTO durable_outbox(job_id,attempt,destination,available_at)
 VALUES($1,$2,$3,clock_timestamp()+$4*interval '1 millisecond')`, event.JobID, nextAttempt, destination, retryDelay.Milliseconds())
		}
	}
	if err != nil {
		return err
	}
	if hooks.BeforeCommit != nil {
		hooks.BeforeCommit()
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if hooks.AfterCommit != nil {
		hooks.AfterCommit()
	}
	return nil
}
