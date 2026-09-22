package store

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/model"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Store persists results exactly once by message ID.
type Store interface {
	PutIfAbsent(context.Context, model.ValidationResult) (inserted bool, err error)
}

type Memory struct {
	mu      sync.RWMutex
	results map[string]model.ValidationResult
}

func NewMemory() *Memory {
	return &Memory{results: make(map[string]model.ValidationResult)}
}

func (s *Memory) PutIfAbsent(_ context.Context, result model.ValidationResult) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.results[result.MessageID]; exists {
		return false, nil
	}
	s.results[result.MessageID] = result
	return true, nil
}

func (s *Memory) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.results)
}

// PostgreSQL uses database/sql so callers can select their preferred driver.
// The schema in migrations/001_validation_results.sql defines the matching
// primary key and column types.
type PostgreSQL struct {
	db *sql.DB
}

func NewPostgreSQL(db *sql.DB) *PostgreSQL { return &PostgreSQL{db: db} }

func OpenPostgreSQL(ctx context.Context, url string) (*PostgreSQL, error) {
	database, err := sql.Open("pgx", url)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	if err := database.PingContext(ctx); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return NewPostgreSQL(database), nil
}

func (s *PostgreSQL) EnsureSchema(ctx context.Context) error {
	const statement = `
		CREATE TABLE IF NOT EXISTS validation_results (
			message_id TEXT PRIMARY KEY,
			endpoint_id TEXT NOT NULL,
			status TEXT NOT NULL,
			fingerprint TEXT NOT NULL,
			checked_at TIMESTAMPTZ NOT NULL
		)`
	if _, err := s.db.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("ensure validation schema: %w", err)
	}
	return nil
}

func (s *PostgreSQL) Contains(ctx context.Context, messageID string) (bool, error) {
	var exists bool
	if err := s.db.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM validation_results WHERE message_id = $1)",
		messageID,
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("check validation result: %w", err)
	}
	return exists, nil
}

func (s *PostgreSQL) Close() error { return s.db.Close() }

func (s *PostgreSQL) PutIfAbsent(ctx context.Context, result model.ValidationResult) (bool, error) {
	const statement = `
		INSERT INTO validation_results
			(message_id, endpoint_id, status, fingerprint, checked_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (message_id) DO NOTHING`
	execution, err := s.db.ExecContext(ctx, statement,
		result.MessageID,
		result.EndpointID,
		result.Status,
		result.Fingerprint,
		result.CheckedAt,
	)
	if err != nil {
		return false, err
	}
	rows, err := execution.RowsAffected()
	return rows == 1, err
}
