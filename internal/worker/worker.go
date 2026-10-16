package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

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
	if strings.TrimSpace(message.ID) == "" {
		return ProcessResult{}, w.reject(ctx, delivery, message, errors.New("message ID is required"))
	}
	if message.Attempts >= w.maxAttempts {
		return ProcessResult{}, w.reject(ctx, delivery, message, errors.New("message retry budget exhausted"))
	}

	var endpoint model.Endpoint
	decoder := json.NewDecoder(bytes.NewReader(message.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&endpoint); err != nil {
		return ProcessResult{}, w.reject(ctx, delivery, message, fmt.Errorf("decode message %q: %w", message.ID, err))
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return ProcessResult{}, w.reject(ctx, delivery, message, fmt.Errorf("message %q must contain one endpoint", message.ID))
	}
	if err := validator.ValidateEndpoint(endpoint); err != nil {
		return ProcessResult{}, w.reject(ctx, delivery, message, fmt.Errorf("message %q: %w", message.ID, err))
	}

	result, err := w.validator.Validate(ctx, endpoint)
	if err == nil {
		err = validator.ValidateResult(endpoint, result)
	}
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
	if err := ctx.Err(); err != nil {
		return errors.Join(cause, err, delivery.Nack(true))
	}
	if message.Attempts >= 0 && message.Attempts < w.maxAttempts-1 {
		return errors.Join(cause, delivery.Nack(true))
	}
	return w.reject(ctx, delivery, message, cause)
}

func (w *Worker) reject(ctx context.Context, delivery queue.Delivery, message queue.Message, cause error) error {
	if err := ctx.Err(); err != nil {
		return errors.Join(cause, err, delivery.Nack(true))
	}
	if sink, ok := w.queue.(queue.DeadLetterSink); ok {
		if err := sink.DeadLetter(ctx, message, cause); err != nil {
			// The transfer has not succeeded. Preserve the original for a later
			// dead-letter attempt rather than dropping the only durable copy.
			return errors.Join(cause, err, delivery.Nack(true))
		}
	}
	return errors.Join(cause, delivery.Nack(false))
}
