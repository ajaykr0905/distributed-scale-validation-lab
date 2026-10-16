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
