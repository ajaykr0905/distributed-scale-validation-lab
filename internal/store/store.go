package store

import (
	"context"
	"database/sql"
	"sync"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/model"
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
