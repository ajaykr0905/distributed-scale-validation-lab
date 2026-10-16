package durable

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/model"
)

// This driver verifies Process's SQL transaction decisions without claiming to
// simulate PostgreSQL locking or broker integration.
type processFixture struct {
	mu                    sync.Mutex
	payload               []byte
	status                string
	attempts, maxAttempts int
	executions            []processExecution
	commits, rollbacks    int
	commitErr             error
}
type processExecution struct {
	query string
	args  []driver.NamedValue
}
type processConn struct{ f *processFixture }
type processTx struct{ f *processFixture }
type processRows struct {
	values []driver.Value
	done   bool
}
type processDriver struct{}

func (processDriver) Open(string) (driver.Conn, error)                 { return nil, errors.New("use connector") }
func (f *processFixture) Driver() driver.Driver                        { return processDriver{} }
func (f *processFixture) Connect(context.Context) (driver.Conn, error) { return &processConn{f}, nil }
func (c *processConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (c *processConn) Close() error                                                 { return nil }
func (c *processConn) Begin() (driver.Tx, error)                                    { return &processTx{c.f}, nil }
func (c *processConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) { return c.Begin() }
func (c *processConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if !strings.Contains(query, "FROM durable_jobs") {
		return nil, errors.New("unexpected query: " + query)
	}
	f := c.f
	return &processRows{values: []driver.Value{f.payload, f.status, int64(f.attempts), int64(f.maxAttempts)}}, nil
}
func (c *processConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	c.f.executions = append(c.f.executions, processExecution{query, append([]driver.NamedValue(nil), args...)})
	return driver.RowsAffected(1), nil
}
func (tx *processTx) Commit() error {
	tx.f.mu.Lock()
	defer tx.f.mu.Unlock()
	tx.f.commits++
	return tx.f.commitErr
}
func (tx *processTx) Rollback() error {
	tx.f.mu.Lock()
	defer tx.f.mu.Unlock()
	tx.f.rollbacks++
	return nil
}
func (*processRows) Columns() []string {
	return []string{"payload", "status", "attempts", "max_attempts"}
}
func (*processRows) Close() error { return nil }
func (r *processRows) Next(values []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	copy(values, r.values)
	return nil
}

func newProcessFixture(t *testing.T, maxAttempts int) (*DB, *processFixture) {
	t.Helper()
	payload, err := json.Marshal(endpoint())
	if err != nil {
		t.Fatal(err)
	}
	f := &processFixture{payload: payload, status: "pending", maxAttempts: maxAttempts}
	db := sql.OpenDB(f)
	t.Cleanup(func() { _ = db.Close() })
	return &DB{SQL: db}, f
}

type processValidator func(context.Context, model.Endpoint) (model.ValidationResult, error)

func (v processValidator) Validate(ctx context.Context, endpoint model.Endpoint) (model.ValidationResult, error) {
	return v(ctx, endpoint)
}

func TestProcessInvalidValidatorOutputUsesFailureTransition(t *testing.T) {
	for _, maxAttempts := range []int{1, 3} {
		db, f := newProcessFixture(t, maxAttempts)
		v := processValidator(func(context.Context, model.Endpoint) (model.ValidationResult, error) {
			return model.ValidationResult{}, nil
		})
		if err := db.Process(context.Background(), Event{JobID: strings.Repeat("a", 64), Attempt: 1}, v, 0, Hooks{}); err != nil {
			t.Fatal(err)
		}
		f.mu.Lock()
		if len(f.executions) != 2 || f.commits != 1 {
			t.Fatalf("executions=%+v commits=%d", f.executions, f.commits)
		}
		want := "retrying"
		if maxAttempts == 1 {
			want = "failed"
		}
		if f.executions[0].args[1].Value != want || strings.Contains(f.executions[0].query, "durable_results") {
			t.Fatalf("invalid result persisted: %+v", f.executions)
		}
		f.mu.Unlock()
	}
}

func TestProcessRejectsInvalidEventBeforeDatabaseAccess(t *testing.T) {
	db := &DB{}
	for _, event := range []Event{
		{JobID: "invalid", Attempt: 1}, {JobID: strings.Repeat("A", 64), Attempt: 1},
		{JobID: strings.Repeat("a", 64), Attempt: -1}, {JobID: strings.Repeat("a", 64), Attempt: 0},
		{JobID: strings.Repeat("a", 64), Attempt: 11}, {JobID: strings.Repeat("a", 64), Attempt: math.MaxInt},
	} {
		if err := db.Process(context.Background(), event, nil, 0, Hooks{}); !errors.Is(err, ErrInvalidEvent) {
			t.Fatalf("event=%+v error=%v", event, err)
		}
	}
}

func TestProcessRejectsImpossibleAttemptsBeforeDuplicateAccounting(t *testing.T) {
	for _, tc := range []struct {
		status                 string
		attempts, eventAttempt int
		invalid                bool
	}{
		{"completed", 1, 2, true}, {"failed", 3, 4, true}, {"pending", 0, 2, true},
		{"completed", 1, 1, false}, {"failed", 3, 3, false}, {"retrying", 2, 1, false},
	} {
		db, f := newProcessFixture(t, 3)
		f.status, f.attempts = tc.status, tc.attempts
		calls := 0
		v := processValidator(func(context.Context, model.Endpoint) (model.ValidationResult, error) {
			calls++
			return model.ValidationResult{}, nil
		})
		err := db.Process(context.Background(), Event{JobID: strings.Repeat("a", 64), Attempt: tc.eventAttempt}, v, 0, Hooks{})
		if tc.invalid {
			if !errors.Is(err, ErrInvalidEvent) || len(f.executions) != 0 || f.rollbacks != 1 {
				t.Fatalf("case=%+v error=%v fixture=%+v", tc, err, f)
			}
		} else if err != nil || f.commits != 1 || len(f.executions) != 1 || !strings.Contains(f.executions[0].query, "duplicates=duplicates+1") {
			t.Fatalf("case=%+v error=%v fixture=%+v", tc, err, f)
		}
		if calls != 0 {
			t.Fatal("duplicate/invalid event invoked validator")
		}
	}
}

func TestProcessRejectsNegativeRetryDelayBeforeDatabaseAccess(t *testing.T) {
	db := &DB{}
	for _, delay := range []time.Duration{-time.Nanosecond, -time.Second, time.Duration(math.MinInt64)} {
		err := db.Process(context.Background(), Event{JobID: strings.Repeat("a", 64), Attempt: 1}, nil, delay, Hooks{})
		if !errors.Is(err, ErrInvalidRetryDelay) {
			t.Fatalf("delay=%s error=%v", delay, err)
		}
	}
}

func TestProcessSchedulesRetriesWithoutTruncationOrOverflow(t *testing.T) {
	for _, tc := range []struct {
		delay time.Duration
		want  int64
	}{
		{0, 0}, {time.Nanosecond, 1}, {time.Millisecond, 1}, {time.Millisecond + time.Nanosecond, 2},
		{time.Duration(math.MaxInt64), math.MaxInt64/int64(time.Millisecond) + 1},
	} {
		for _, maxAttempts := range []int{1, 3} {
			db, f := newProcessFixture(t, maxAttempts)
			v := processValidator(func(context.Context, model.Endpoint) (model.ValidationResult, error) {
				return model.ValidationResult{}, errors.New("retry me")
			})
			if err := db.Process(context.Background(), Event{JobID: strings.Repeat("a", 64), Attempt: 1}, v, tc.delay, Hooks{}); err != nil {
				t.Fatal(err)
			}
			wantDelay, wantDestination, wantAttempt := tc.want, "work", int64(2)
			if maxAttempts == 1 {
				wantDelay, wantDestination, wantAttempt = 0, "dead", 1
			}
			if len(f.executions) != 2 || f.commits != 1 {
				t.Fatalf("fixture=%+v", f)
			}
			outbox := f.executions[1]
			if outbox.args[1].Value != wantAttempt || outbox.args[2].Value != wantDestination || outbox.args[3].Value != wantDelay {
				t.Fatalf("delay=%s maxAttempts=%d outbox=%+v", tc.delay, maxAttempts, outbox)
			}
		}
	}
}

func TestProcessFailureTextRemainsSafeForPostgres(t *testing.T) {
	for _, tc := range []struct{ message, want string }{
		{"short failure", "short failure"},
		{strings.Repeat("x", 600), strings.Repeat("x", 512)},
		{strings.Repeat("x", 511) + "é", strings.Repeat("x", 511)},
		{strings.Repeat("界", 200), strings.Repeat("界", 170)},
		{"bad\x00text\xff", "bad\uFFFDtext\uFFFD"},
	} {
		for _, maxAttempts := range []int{1, 3} {
			db, f := newProcessFixture(t, maxAttempts)
			v := processValidator(func(context.Context, model.Endpoint) (model.ValidationResult, error) {
				return model.ValidationResult{}, errors.New(tc.message)
			})
			if err := db.Process(context.Background(), Event{JobID: strings.Repeat("a", 64), Attempt: 1}, v, 0, Hooks{}); err != nil {
				t.Fatal(err)
			}
			if f.commits != 1 || len(f.executions) != 2 {
				t.Fatalf("fixture=%+v", f)
			}
			message, ok := f.executions[0].args[3].Value.(string)
			if !ok || message != tc.want || len(message) > 512 || !utf8.ValidString(message) || strings.ContainsRune(message, 0) {
				t.Fatalf("stored unsafe error: %q want %q", message, tc.want)
			}
		}
	}
}
