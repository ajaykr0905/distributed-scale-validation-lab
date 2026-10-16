package queue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMemoryRequeueDoesNotWaitForPublisherCapacity(t *testing.T) {
	q := NewMemory(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := q.Publish(ctx, Message{ID: "retry", Payload: []byte("original")}); err != nil {
		t.Fatal(err)
	}
	delivery, err := q.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(ctx, Message{ID: "new"}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- delivery.Nack(true) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		// Release a blocked implementation before failing the test.
		<-q.messages
		<-done
		t.Fatal("requeue blocked behind a new publisher")
	}
	for _, id := range []string{"retry", "new"} {
		got, err := q.Receive(ctx)
		if err != nil || got.Message().ID != id {
			t.Fatalf("receive %s: delivery=%v error=%v", id, got, err)
		}
		if id == "retry" && (got.Message().Attempts != 1 || string(got.Message().Payload) != "original") {
			t.Fatalf("retry changed: %+v", got.Message())
		}
	}
}

func TestMemoryConcurrentSettlementCreatesOneRetry(t *testing.T) {
	q := NewMemory(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := q.Publish(ctx, Message{ID: "one"}); err != nil {
		t.Fatal(err)
	}
	delivery, err := q.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for range 32 {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- delivery.Nack(true) }()
	}
	wg.Wait()
	close(errs)
	winners := 0
	for err := range errs {
		if err == nil {
			winners++
		} else if !errors.Is(err, ErrSettled) {
			t.Fatal(err)
		}
	}
	if winners != 1 || len(q.retries) != 1 {
		t.Fatalf("successful settlements=%d retries=%d", winners, len(q.retries))
	}
}

func TestMemoryRetryWakeupPropagatesToWaitingReceivers(t *testing.T) {
	q := NewMemory(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// Coalesce several retry notifications, as can happen during a retry burst.
	for range 16 {
		q.requeue(Message{ID: "retry"})
	}
	<-q.retryReady
	delivery, err := q.Receive(ctx)
	if err != nil || delivery == nil {
		t.Fatalf("receive: %v", err)
	}
	select {
	case <-q.retryReady:
	case <-ctx.Done():
		t.Fatal("remaining retries did not wake another receiver")
	}
}

func TestMemoryCanceledOperationsDoNotChangeReadyQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 100 {
		q := NewMemory(2)
		if err := q.Publish(ctx, Message{ID: "canceled"}); !errors.Is(err, context.Canceled) {
			t.Fatalf("publish: %v", err)
		}
		if err := q.Publish(context.Background(), Message{ID: "buffered"}); err != nil {
			t.Fatal(err)
		}
		q.requeue(Message{ID: "retry"})
		if _, err := q.Receive(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("receive: %v", err)
		}
		if len(q.messages) != 1 || len(q.retries) != 1 {
			t.Fatal("canceled operation consumed ready work")
		}
		if err := q.DeadLetter(ctx, Message{}, errors.New("failure")); !errors.Is(err, context.Canceled) || len(q.DeadLetters()) != 0 {
			t.Fatalf("dead letter: %v", err)
		}
	}
}

func TestMemoryAttemptBoundsCannotWrapOrSettleFailedRetry(t *testing.T) {
	q := NewMemory(1)
	if err := q.Publish(context.Background(), Message{Attempts: -1}); !errors.Is(err, ErrInvalidAttempts) {
		t.Fatalf("publish: %v", err)
	}
	message := Message{ID: "exhausted", Attempts: MaxAttemptCount}
	if err := q.Publish(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	d, err := q.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Nack(true); !errors.Is(err, ErrInvalidAttempts) {
		t.Fatalf("requeue: %v", err)
	}
	if err := d.Nack(false); err != nil {
		t.Fatalf("failed requeue settled delivery: %v", err)
	}
	if err := q.DeadLetter(context.Background(), message, errors.New("exhausted")); err != nil {
		t.Fatal(err)
	}
	if got := q.DeadLetters()[0].Message.Attempts; got != MaxAttemptCount {
		t.Fatalf("wrapped attempts: %d", got)
	}
}
