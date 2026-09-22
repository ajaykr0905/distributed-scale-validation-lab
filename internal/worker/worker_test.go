package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/model"
	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/queue"
	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/store"
)

var errInjected = errors.New("injected failure")

type controlledValidator struct {
	failuresRemaining int
	calls             int
}

func (v *controlledValidator) Validate(_ context.Context, endpoint model.Endpoint) (model.ValidationResult, error) {
	v.calls++
	if v.failuresRemaining > 0 {
		v.failuresRemaining--
		return model.ValidationResult{}, errInjected
	}
	return model.ValidationResult{
		EndpointID:  endpoint.ID,
		Status:      "valid",
		Fingerprint: "test-fingerprint",
		CheckedAt:   time.Unix(0, 0).UTC(),
	}, nil
}

type controlledStore struct {
	delegate          *store.Memory
	failuresRemaining int
}

func (s *controlledStore) PutIfAbsent(ctx context.Context, result model.ValidationResult) (bool, error) {
	if s.failuresRemaining > 0 {
		s.failuresRemaining--
		return false, errInjected
	}
	return s.delegate.PutIfAbsent(ctx, result)
}

func TestDuplicateDeliveryCreatesOneResult(t *testing.T) {
	q := queue.NewMemory(2)
	s := store.NewMemory()
	v := &controlledValidator{}
	message := testMessage(t, "message-1")
	publish(t, q, message)
	publish(t, q, message)
	w := New(q, s, v)

	first, err := w.ProcessOne(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := w.ProcessOne(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !first.Inserted || second.Inserted {
		t.Fatalf("expected first insert and second deduplication, got %+v then %+v", first, second)
	}
	if s.Len() != 1 {
		t.Fatalf("expected one stored result, got %d", s.Len())
	}
}

func TestValidationFailureRequeuesForRetry(t *testing.T) {
	q := queue.NewMemory(1)
	s := store.NewMemory()
	v := &controlledValidator{failuresRemaining: 1}
	publish(t, q, testMessage(t, "message-retry"))
	w := New(q, s, v)

	if _, err := w.ProcessOne(context.Background()); !errors.Is(err, errInjected) {
		t.Fatalf("expected injected validation failure, got %v", err)
	}
	if _, err := w.ProcessOne(context.Background()); err != nil {
		t.Fatalf("requeued message should succeed: %v", err)
	}
	if v.calls != 2 || s.Len() != 1 {
		t.Fatalf("expected two attempts and one result, got calls=%d results=%d", v.calls, s.Len())
	}
}

func TestPersistenceFailureRequeuesForRetry(t *testing.T) {
	q := queue.NewMemory(1)
	baseStore := store.NewMemory()
	s := &controlledStore{delegate: baseStore, failuresRemaining: 1}
	v := &controlledValidator{}
	publish(t, q, testMessage(t, "message-store-retry"))
	w := New(q, s, v)

	if _, err := w.ProcessOne(context.Background()); !errors.Is(err, errInjected) {
		t.Fatalf("expected injected persistence failure, got %v", err)
	}
	if _, err := w.ProcessOne(context.Background()); err != nil {
		t.Fatalf("requeued message should succeed: %v", err)
	}
	if baseStore.Len() != 1 {
		t.Fatalf("expected one stored result, got %d", baseStore.Len())
	}
}

func TestMalformedMessageIsNotRequeued(t *testing.T) {
	q := queue.NewMemory(1)
	if err := q.Publish(context.Background(), queue.Message{ID: "bad", Payload: []byte("{")}); err != nil {
		t.Fatal(err)
	}
	w := New(q, store.NewMemory(), &controlledValidator{})
	if _, err := w.ProcessOne(context.Background()); err == nil {
		t.Fatal("malformed message should fail")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := q.Receive(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("poison message should not be requeued, got %v", err)
	}
}

func TestValidationFailureMovesToDeadLetterAfterBoundedRetries(t *testing.T) {
	q := queue.NewMemory(1)
	s := store.NewMemory()
	v := &controlledValidator{failuresRemaining: 10}
	publish(t, q, testMessage(t, "message-dead-letter"))
	w := New(q, s, v, WithMaxAttempts(3))

	for attempt := 0; attempt < 3; attempt++ {
		if _, err := w.ProcessOne(context.Background()); !errors.Is(err, errInjected) {
			t.Fatalf("attempt %d should expose the injected failure: %v", attempt+1, err)
		}
	}
	letters := q.DeadLetters()
	if len(letters) != 1 {
		t.Fatalf("expected one dead letter, got %d", len(letters))
	}
	if letters[0].Message.ID != "message-dead-letter" || letters[0].Message.Attempts != 3 {
		t.Fatalf("unexpected dead letter: %+v", letters[0])
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := q.Receive(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("exhausted message must not requeue, got %v", err)
	}
}

func testMessage(t testing.TB, id string) queue.Message {
	t.Helper()
	payload, err := json.Marshal(model.Endpoint{
		ID:              "syn-endpoint-test",
		TenantID:        "syn-tenant-test",
		Region:          "ap-south",
		Platform:        "linux",
		ExpectedVersion: "v1.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	return queue.Message{ID: id, Payload: payload}
}

func publish(t *testing.T, q queue.Queue, message queue.Message) {
	t.Helper()
	if err := q.Publish(context.Background(), message); err != nil {
		t.Fatal(err)
	}
}
