//go:build integration

package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/model"
	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/validator"
)

func integrationDB(t *testing.T) (*DB, string) {
	t.Helper()
	if os.Getenv("SCALE_LAB_INTEGRATION") != "1" {
		t.Skip("set SCALE_LAB_INTEGRATION=1 with dedicated public lab services")
	}
	base := os.Getenv("SCALE_LAB_POSTGRES_URL")
	if base == "" {
		base = "postgres://scale_lab:local-development-only@127.0.0.1:25432/scale_lab?sslmode=disable"
	}
	admin, err := Open(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	schema := "test_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if _, err := admin.SQL.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	db, err := Open(context.Background(), parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.EnsureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(); _, _ = admin.SQL.Exec("DROP SCHEMA " + schema + " CASCADE"); _ = admin.Close() })
	return db, parsed.String()
}

func integrationBroker(t *testing.T, name string, consume bool) *Broker {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rabbit := os.Getenv("SCALE_LAB_RABBITMQ_URL")
	if rabbit == "" {
		rabbit = "amqp://scale_lab:local-development-only@127.0.0.1:25672/"
	}
	b, err := Connect(ctx, rabbit, name, consume)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func submit(t *testing.T, db *DB, key string, max int) Job {
	t.Helper()
	job, created, err := db.Submit(context.Background(), key, endpoint(), max, 100)
	if err != nil || !created {
		t.Fatalf("submit created=%t error=%v", created, err)
	}
	return job
}

func processDelivery(t *testing.T, db *DB, b *Broker, v validator.Validator) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	delivery, err := b.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var event Event
	if err := json.Unmarshal(delivery.Body, &event); err != nil {
		t.Fatal(err)
	}
	if err := db.Process(ctx, event, v, 0, Hooks{}); err != nil {
		t.Fatal(err)
	}
	if err := delivery.Ack(false); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentAdmissionAndCapacity(t *testing.T) {
	db, _ := integrationDB(t)
	var wg sync.WaitGroup
	created := make(chan bool, 16)
	errs := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, fresh, err := db.Submit(context.Background(), "same-key", endpoint(), 3, 1)
			created <- fresh
			errs <- err
		}()
	}
	wg.Wait()
	close(created)
	close(errs)
	fresh := 0
	for value := range created {
		if value {
			fresh++
		}
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if fresh != 1 {
		t.Fatalf("created=%d", fresh)
	}
	changed := endpoint()
	changed.ExpectedVersion = "v2"
	if _, _, err := db.Submit(context.Background(), "same-key", changed, 3, 1); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected conflict: %v", err)
	}
	if _, _, err := db.Submit(context.Background(), "second-key", endpoint(), 3, 1); !errors.Is(err, ErrFull) {
		t.Fatalf("expected capacity: %v", err)
	}
	c, err := db.Counts(context.Background())
	if err != nil || c.Accepted != 1 || c.OutboxPending != 1 || c.Pending != 1 {
		t.Fatalf("counts=%+v error=%v", c, err)
	}
}

type failValidator struct{}

func (failValidator) Validate(context.Context, model.Endpoint) (model.ValidationResult, error) {
	return model.ValidationResult{}, errors.New("injected validation failure")
}

func TestBoundedRetriesDeadLettersAndAccounting(t *testing.T) {
	db, _ := integrationDB(t)
	name := "durable-retry-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	b := integrationBroker(t, name, true)
	job := submit(t, db, "retry-key", 3)
	for range 3 {
		if sent, err := db.DispatchOne(context.Background(), b, Hooks{}); err != nil || !sent {
			t.Fatalf("dispatch=%t %v", sent, err)
		}
		processDelivery(t, db, b, failValidator{})
	}
	if sent, err := db.DispatchOne(context.Background(), b, Hooks{}); err != nil || !sent {
		t.Fatalf("dead dispatch=%t %v", sent, err)
	}
	final, err := db.Get(context.Background(), job.ID)
	if err != nil || final.Status != "failed" || final.Attempts != 3 {
		t.Fatalf("final=%+v err=%v", final, err)
	}
	c, err := db.Counts(context.Background())
	if err != nil || c.Accepted != c.Completed+c.Failed+c.Pending+c.Retrying || c.Results != 0 || c.Failed != 1 || c.Retries != 2 || c.OutboxPending != 0 {
		t.Fatalf("counts=%+v err=%v", c, err)
	}
	_, dead, err := b.Depth()
	if err != nil || dead != 1 {
		t.Fatalf("dead=%d err=%v", dead, err)
	}
}

func TestDatabaseFailureRollsBackAttemptAndResult(t *testing.T) {
	db, _ := integrationDB(t)
	job := submit(t, db, "sql-error", 3)
	_, err := db.SQL.Exec(`CREATE FUNCTION reject_result() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'injected database error'; END $$ LANGUAGE plpgsql; CREATE TRIGGER reject_result BEFORE INSERT ON durable_results FOR EACH ROW EXECUTE FUNCTION reject_result()`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Process(context.Background(), Event{JobID: job.ID, Attempt: 1}, validator.Basic{}, 0, Hooks{}); err == nil {
		t.Fatal("expected SQL failure")
	}
	after, err := db.Get(context.Background(), job.ID)
	if err != nil || after.Status != "pending" || after.Attempts != 0 {
		t.Fatalf("rolled-back job=%+v err=%v", after, err)
	}
	if _, err := db.SQL.Exec("DROP TRIGGER reject_result ON durable_results"); err != nil {
		t.Fatal(err)
	}
	if err := db.Process(context.Background(), Event{JobID: job.ID, Attempt: 1}, validator.Basic{}, 0, Hooks{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Process(context.Background(), Event{JobID: job.ID, Attempt: 1}, validator.Basic{}, 0, Hooks{}); err != nil {
		t.Fatal(err)
	}
	c, _ := db.Counts(context.Background())
	if c.Results != 1 || c.Completed != 1 || c.Duplicates != 1 {
		t.Fatalf("counts=%+v", c)
	}
}

type unavailablePublisher struct{}

func (unavailablePublisher) Publish(context.Context, string, Event) error {
	return errors.New("broker unavailable")
}

func TestBrokerFailureKeepsOutboxPending(t *testing.T) {
	db, _ := integrationDB(t)
	submit(t, db, "broker-outage", 3)
	if _, err := db.DispatchOne(context.Background(), unavailablePublisher{}, Hooks{}); err == nil {
		t.Fatal("expected broker failure")
	}
	c, _ := db.Counts(context.Background())
	if c.OutboxPending != 1 || c.Pending != 1 {
		t.Fatalf("counts=%+v", c)
	}
	b := integrationBroker(t, "durable-outage-"+strconv.FormatInt(time.Now().UnixNano(), 10), true)
	if sent, err := db.DispatchOne(context.Background(), b, Hooks{}); err != nil || !sent {
		t.Fatalf("recovery dispatch=%t err=%v", sent, err)
	}
	processDelivery(t, db, b, validator.Basic{})
	c, _ = db.Counts(context.Background())
	if c.Completed != 1 || c.Results != 1 {
		t.Fatalf("recovered counts=%+v", c)
	}
}

func TestWorkerAndDispatcherProcessDeath(t *testing.T) {
	for _, point := range []string{"before-commit", "after-commit", "after-publish"} {
		t.Run(point, func(t *testing.T) {
			db, dbURL := integrationDB(t)
			name := "durable-crash-" + strconv.FormatInt(time.Now().UnixNano(), 10)
			job := submit(t, db, "crash-job", 3)
			if point != "after-publish" {
				publisher := integrationBroker(t, name, false)
				if sent, err := db.DispatchOne(context.Background(), publisher, Hooks{}); err != nil || !sent {
					t.Fatalf("dispatch=%t err=%v", sent, err)
				}
			}
			command := exec.Command(os.Args[0], "-test.run=^TestFaultHelper$")
			command.Env = append(os.Environ(), "DURABLE_FAULT_HELPER=1", "DURABLE_FAULT_POINT="+point, "DURABLE_FAULT_DB="+dbURL, "DURABLE_FAULT_QUEUE="+name)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 86 {
				t.Fatalf("helper error=%v output=%s", err, output)
			}
			if point == "before-commit" {
				after, _ := db.Get(context.Background(), job.ID)
				if after.Status != "pending" || after.Attempts != 0 {
					t.Fatalf("before commit job=%+v", after)
				}
			}
			if point == "after-commit" {
				after, _ := db.Get(context.Background(), job.ID)
				if after.Status != "completed" {
					t.Fatalf("after commit job=%+v", after)
				}
			}
			b := integrationBroker(t, name, true)
			if point == "after-publish" {
				if sent, err := db.DispatchOne(context.Background(), b, Hooks{}); err != nil || !sent {
					t.Fatalf("restart dispatch=%t err=%v", sent, err)
				}
			}
			processDelivery(t, db, b, validator.Basic{})
			if point == "after-publish" {
				processDelivery(t, db, b, validator.Basic{})
			}
			c, _ := db.Counts(context.Background())
			if c.Results != 1 || c.Completed != 1 || c.Accepted != 1 {
				t.Fatalf("counts=%+v", c)
			}
			if point != "before-commit" && c.Duplicates != 1 {
				t.Fatalf("expected accounted duplicate: %+v", c)
			}
		})
	}
}

func TestFaultHelper(t *testing.T) {
	if os.Getenv("DURABLE_FAULT_HELPER") != "1" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := Open(ctx, os.Getenv("DURABLE_FAULT_DB"))
	if err != nil {
		t.Fatal(err)
	}
	point := os.Getenv("DURABLE_FAULT_POINT")
	b := integrationBroker(t, os.Getenv("DURABLE_FAULT_QUEUE"), point != "after-publish")
	crash := func() { os.Exit(86) }
	if point == "after-publish" {
		_, err = db.DispatchOne(ctx, b, Hooks{AfterPublish: crash})
	} else {
		delivery, receiveErr := b.Receive(ctx)
		if receiveErr != nil {
			t.Fatal(receiveErr)
		}
		var event Event
		if err := json.Unmarshal(delivery.Body, &event); err != nil {
			t.Fatal(err)
		}
		hooks := Hooks{BeforeCommit: crash}
		if point == "after-commit" {
			hooks = Hooks{AfterCommit: crash}
		}
		err = db.Process(ctx, event, validator.Basic{}, 0, hooks)
	}
	t.Fatal(fmt.Sprintf("fault boundary not reached: %v", err))
}

// This opt-in test only interrupts the explicitly named, newly created public
// Compose project. It never discovers or restarts unrelated Docker resources.
func TestIndependentProcessesAndBrokerRestart(t *testing.T) {
	project := os.Getenv("SCALE_LAB_DOCKER_PROJECT")
	if !strings.HasPrefix(project, "ajay-durable-") {
		t.Skip("set SCALE_LAB_DOCKER_PROJECT to the isolated ajay-durable-* Compose project")
	}
	db, dbURL := integrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	bin := t.TempDir() + "/durable"
	build := exec.CommandContext(ctx, "go", "build", "-o", bin, "../../cmd/durable")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, output)
	}
	name := "durable-reconnect-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	var commands []*exec.Cmd
	finished := false
	finish := func() {
		if finished {
			return
		}
		finished = true
		for _, command := range commands {
			_ = command.Process.Signal(syscall.SIGTERM)
		}
		for _, command := range commands {
			if err := command.Wait(); err != nil {
				t.Errorf("graceful process exit: %v", err)
			}
		}
	}
	t.Cleanup(finish)
	for _, role := range []string{"dispatcher", "worker"} {
		command := exec.CommandContext(ctx, bin, "-role", role, "-queue", name)
		command.Env = append(os.Environ(), "SCALE_LAB_POSTGRES_URL="+dbURL)
		if os.Getenv("SCALE_LAB_RABBITMQ_URL") == "" {
			command.Env = append(command.Env, "SCALE_LAB_RABBITMQ_URL=amqp://scale_lab:local-development-only@127.0.0.1:25672/")
		}
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		commands = append(commands, command)
	}
	wait := func(id string) {
		t.Helper()
		for ctx.Err() == nil {
			job, err := db.Get(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if job.Status == "completed" {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("recovery timed out")
	}
	first := submit(t, db, "before-broker-stop", 3)
	wait(first.ID)
	compose := func(args ...string) {
		t.Helper()
		command := exec.CommandContext(ctx, "docker", append([]string{"compose", "-p", project, "-f", "../../compose.durable.yaml"}, args...)...)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("isolated compose operation: %v %s", err, output)
		}
	}
	compose("stop", "rabbitmq")
	second := submit(t, db, "while-broker-stopped", 3)
	time.Sleep(100 * time.Millisecond)
	counts, err := db.Counts(ctx)
	if err != nil || counts.Accepted != 2 || counts.Completed != 1 || counts.Pending != 1 || counts.OutboxPending != 1 {
		t.Fatalf("outage accounting=%+v err=%v", counts, err)
	}
	start := time.Now()
	compose("up", "-d", "--wait", "rabbitmq")
	wait(second.ID)
	recoverySeconds := time.Since(start).Seconds()
	t.Logf("broker_restart_recovery_seconds=%.6f (includes Compose start and health wait)", recoverySeconds)
	b := integrationBroker(t, name, false)
	if err := b.Publish(ctx, "work", Event{JobID: second.ID, Attempt: 1}); err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		counts, err = db.Counts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if counts.Duplicates >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if counts.Accepted != 2 || counts.Completed != 2 || counts.Results != 2 || counts.Pending != 0 || counts.Retrying != 0 || counts.Failed != 0 || counts.Duplicates < 1 {
		t.Fatalf("recovery accounting=%+v", counts)
	}
	finish()
	if t.Failed() {
		return
	}
	if path := os.Getenv("SCALE_LAB_RECOVERY_RECEIPT"); path != "" {
		commit, err := exec.Command("git", "rev-parse", "HEAD").Output()
		if err != nil {
			t.Fatal(err)
		}
		status, err := exec.Command("git", "status", "--porcelain").Output()
		if err != nil {
			t.Fatal(err)
		}
		receipt := struct {
			CreatedAt       time.Time `json:"created_at"`
			Commit          string    `json:"commit"`
			Dirty           bool      `json:"dirty"`
			RecoverySeconds float64   `json:"broker_restart_recovery_seconds"`
			Counts          Counts    `json:"counts"`
			Boundary        string    `json:"measurement_boundary"`
		}{time.Now().UTC(), strings.TrimSpace(string(commit)), len(status) > 0, recoverySeconds, counts, "From Compose start command through health wait and second job result commit; local single-host only."}
		body, err := json.MarshalIndent(receipt, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(body, '\n'), 0644); err != nil {
			t.Error(err)
		}
	}
}
