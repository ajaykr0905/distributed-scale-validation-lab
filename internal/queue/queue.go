package queue

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

var ErrSettled = errors.New("delivery already settled")
var ErrInvalidAttempts = errors.New("attempt count must fit a nonnegative AMQP int32")

const MaxAttemptCount = math.MaxInt32

func checkAttempts(attempts int) error {
	if attempts < 0 || attempts > MaxAttemptCount {
		return ErrInvalidAttempts
	}
	return nil
}

func retryMessage(message Message) (Message, error) {
	if err := checkAttempts(message.Attempts); err != nil || message.Attempts == MaxAttemptCount {
		return Message{}, ErrInvalidAttempts
	}
	retry := clone(message)
	retry.Attempts++
	return retry, nil
}

type Message struct {
	ID       string
	Payload  []byte
	Attempts int
}

type DeadLetter struct {
	Message   Message   `json:"message"`
	Reason    string    `json:"reason"`
	DroppedAt time.Time `json:"dropped_at"`
}

type Delivery interface {
	Message() Message
	Ack() error
	Nack(requeue bool) error
}

// Queue is deliberately transport-neutral. An AMQP or cloud queue adapter can
// implement it without changing the worker.
type Queue interface {
	Publish(context.Context, Message) error
	Receive(context.Context) (Delivery, error)
}

// DeadLetterSink is optional. Workers use it when a message exhausts its
// bounded retry budget. A broker adapter can map this to a dead-letter
// exchange without changing the worker contract.
type DeadLetterSink interface {
	DeadLetter(context.Context, Message, error) error
}

// Memory is a dependency-free queue for local runs and deterministic tests.
type Memory struct {
	messages    chan Message
	mu          sync.RWMutex
	deadLetters []DeadLetter
	retries     []Message
	retryReady  chan struct{}
}

func NewMemory(capacity int) *Memory {
	if capacity < 1 {
		capacity = 1
	}
	return &Memory{messages: make(chan Message, capacity), retryReady: make(chan struct{}, 1)}
}

func (q *Memory) Publish(ctx context.Context, message Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkAttempts(message.Attempts); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case q.messages <- clone(message):
		return nil
	}
}

func (q *Memory) Receive(ctx context.Context) (Delivery, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		q.mu.Lock()
		if len(q.retries) > 0 {
			message := q.retries[0]
			q.retries[0] = Message{}
			q.retries = q.retries[1:]
			more := len(q.retries) > 0
			q.mu.Unlock()
			if more {
				q.notifyRetry()
			}
			return &memoryDelivery{queue: q, message: message}, nil
		}
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-q.retryReady:
			// A retry may have been taken by another receiver. Check again.
		case message := <-q.messages:
			return &memoryDelivery{queue: q, message: message}, nil
		}
	}
}

// Requeue owns an already accepted message; it must not wait for publisher
// capacity, which another publisher may have filled while it was in flight.
func (q *Memory) requeue(message Message) {
	q.mu.Lock()
	q.retries = append(q.retries, message)
	q.mu.Unlock()
	q.notifyRetry()
}

func (q *Memory) notifyRetry() {
	select {
	case q.retryReady <- struct{}{}:
	default:
	}
}

func (q *Memory) DeadLetter(ctx context.Context, message Message, cause error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkAttempts(message.Attempts); err != nil {
		return err
	}
	if message.Attempts < MaxAttemptCount {
		message.Attempts++
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	q.deadLetters = append(q.deadLetters, DeadLetter{
		Message:   clone(message),
		Reason:    cause.Error(),
		DroppedAt: time.Now().UTC(),
	})
	return nil
}

func (q *Memory) DeadLetters() []DeadLetter {
	q.mu.RLock()
	defer q.mu.RUnlock()
	result := make([]DeadLetter, len(q.deadLetters))
	for index, item := range q.deadLetters {
		item.Message = clone(item.Message)
		result[index] = item
	}
	return result
}

type memoryDelivery struct {
	queue   *Memory
	message Message
	settled atomic.Bool
}

func (d *memoryDelivery) Message() Message { return clone(d.message) }

func (d *memoryDelivery) Ack() error {
	if !d.settled.CompareAndSwap(false, true) {
		return ErrSettled
	}
	return nil
}

func (d *memoryDelivery) Nack(requeue bool) error {
	if d.settled.Load() {
		return ErrSettled
	}
	var retry Message
	if requeue {
		var err error
		retry, err = retryMessage(d.message)
		if err != nil {
			return err
		}
	}
	if !d.settled.CompareAndSwap(false, true) {
		return ErrSettled
	}
	if !requeue {
		return nil
	}
	d.queue.requeue(retry)
	return nil
}

func clone(message Message) Message {
	copyOfPayload := append([]byte(nil), message.Payload...)
	return Message{ID: message.ID, Payload: copyOfPayload, Attempts: message.Attempts}
}
