package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/model"
	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/queue"
	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/store"
	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/validator"
)

type Worker struct {
	queue       queue.Queue
	store       store.Store
	validator   validator.Validator
	maxAttempts int
}

type ProcessResult struct {
	MessageID string
	Inserted  bool
}

type Option func(*Worker)

func WithMaxAttempts(maxAttempts int) Option {
	return func(worker *Worker) {
		if maxAttempts > 0 {
			worker.maxAttempts = maxAttempts
		}
	}
}

func New(q queue.Queue, s store.Store, v validator.Validator, options ...Option) *Worker {
	result := &Worker{queue: q, store: s, validator: v, maxAttempts: 3}
	for _, option := range options {
		option(result)
	}
	return result
}

// ProcessOne acknowledges a message only after its result is durably accepted.
// A duplicate is safe: PutIfAbsent returns false and the message is acknowledged.
func (w *Worker) ProcessOne(ctx context.Context) (ProcessResult, error) {
	delivery, err := w.queue.Receive(ctx)
	if err != nil {
		return ProcessResult{}, fmt.Errorf("receive: %w", err)
	}
	message := delivery.Message()

	var endpoint model.Endpoint
	if err := json.Unmarshal(message.Payload, &endpoint); err != nil {
		return ProcessResult{}, errors.Join(
			fmt.Errorf("decode message %q: %w", message.ID, err),
			delivery.Nack(false),
		)
	}

	result, err := w.validator.Validate(ctx, endpoint)
	if err != nil {
		cause := fmt.Errorf("validate message %q: %w", message.ID, err)
		return ProcessResult{}, w.settleFailure(ctx, delivery, message, cause)
	}
	result.MessageID = message.ID

	inserted, err := w.store.PutIfAbsent(ctx, result)
	if err != nil {
		cause := fmt.Errorf("store message %q: %w", message.ID, err)
		return ProcessResult{}, w.settleFailure(ctx, delivery, message, cause)
	}
	if err := delivery.Ack(); err != nil {
		return ProcessResult{}, fmt.Errorf("ack message %q: %w", message.ID, err)
	}
	return ProcessResult{MessageID: message.ID, Inserted: inserted}, nil
}

func (w *Worker) settleFailure(ctx context.Context, delivery queue.Delivery, message queue.Message, cause error) error {
	if message.Attempts+1 < w.maxAttempts {
		return errors.Join(cause, delivery.Nack(true))
	}

	var deadLetterErr error
	if sink, ok := w.queue.(queue.DeadLetterSink); ok {
		deadLetterErr = sink.DeadLetter(ctx, message, cause)
	}
	return errors.Join(cause, deadLetterErr, delivery.Nack(false))
}
