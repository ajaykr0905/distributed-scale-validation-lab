package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/model"
	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/queue"
	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/store"
)

type recordingDelivery struct {
	message         queue.Message
	acks            int
	nacks           []bool
	ackErr, nackErr error
}

func (d *recordingDelivery) Message() queue.Message { return d.message }
func (d *recordingDelivery) Ack() error             { d.acks++; return d.ackErr }
func (d *recordingDelivery) Nack(requeue bool) error {
	d.nacks = append(d.nacks, requeue)
	return d.nackErr
}

type oneDeliveryQueue struct{ delivery queue.Delivery }

func (q oneDeliveryQueue) Publish(context.Context, queue.Message) error    { return nil }
func (q oneDeliveryQueue) Receive(context.Context) (queue.Delivery, error) { return q.delivery, nil }

type recordingSink struct {
	oneDeliveryQueue
	err   error
	calls int
}

func (q *recordingSink) DeadLetter(context.Context, queue.Message, error) error {
	q.calls++
	return q.err
}

type validatorFunc func(context.Context, model.Endpoint) (model.ValidationResult, error)

func (f validatorFunc) Validate(ctx context.Context, endpoint model.Endpoint) (model.ValidationResult, error) {
	return f(ctx, endpoint)
}

func TestDeadLetterTransferFailurePreservesOriginal(t *testing.T) {
	transferErr := errors.New("dead-letter publish failed")
	settlementErr := errors.New("requeue failed")
	d := &recordingDelivery{message: testMessage(t, "exhausted"), nackErr: settlementErr}
	q := &recordingSink{oneDeliveryQueue: oneDeliveryQueue{delivery: d}, err: transferErr}
	w := New(q, store.NewMemory(), &controlledValidator{failuresRemaining: 1}, WithMaxAttempts(1))
	_, err := w.ProcessOne(context.Background())
	for _, want := range []error{errInjected, transferErr, settlementErr} {
		if !errors.Is(err, want) {
			t.Fatalf("lost %v in %v", want, err)
		}
	}
	if q.calls != 1 || d.acks != 0 || len(d.nacks) != 1 || !d.nacks[0] {
		t.Fatalf("transfer=%d delivery=%+v", q.calls, d)
	}
}

func TestCancellationDoesNotDeadLetterExhaustedDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &recordingDelivery{message: testMessage(t, "canceled")}
	q := &recordingSink{oneDeliveryQueue: oneDeliveryQueue{delivery: d}}
	v := validatorFunc(func(context.Context, model.Endpoint) (model.ValidationResult, error) {
		cancel()
		return model.ValidationResult{}, context.Canceled
	})
	w := New(q, store.NewMemory(), v, WithMaxAttempts(1))
	_, err := w.ProcessOne(ctx)
	if !errors.Is(err, context.Canceled) || q.calls != 0 || len(d.nacks) != 1 || !d.nacks[0] {
		t.Fatalf("error=%v sink=%d delivery=%+v", err, q.calls, d)
	}
}

func TestAckFailureAfterPersistenceRetainsResultForDeduplication(t *testing.T) {
	ackErr := errors.New("ack failed")
	d := &recordingDelivery{message: testMessage(t, "stored"), ackErr: ackErr}
	s := store.NewMemory()
	w := New(oneDeliveryQueue{delivery: d}, s, &controlledValidator{})
	if _, err := w.ProcessOne(context.Background()); !errors.Is(err, ackErr) || s.Len() != 1 || len(d.nacks) != 0 {
		t.Fatalf("error=%v stored=%d delivery=%+v", err, s.Len(), d)
	}
	d.ackErr = nil
	if result, err := w.ProcessOne(context.Background()); err != nil || result.Inserted {
		t.Fatalf("redelivery=%+v error=%v", result, err)
	}
}
