package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var ErrSettled = errors.New("delivery already settled")

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
}

func NewMemory(capacity int) *Memory {
	if capacity < 1 {
		capacity = 1
	}
	return &Memory{messages: make(chan Message, capacity)}
}

func (q *Memory) Publish(ctx context.Context, message Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case q.messages <- clone(message):
		return nil
	}
}

func (q *Memory) Receive(ctx context.Context) (Delivery, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case message := <-q.messages:
		return &memoryDelivery{queue: q, message: message}, nil
	}
}

func (q *Memory) DeadLetter(ctx context.Context, message Message, cause error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	message.Attempts++
	q.mu.Lock()
	defer q.mu.Unlock()
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
	if !d.settled.CompareAndSwap(false, true) {
		return ErrSettled
	}
	if !requeue {
		return nil
	}
	retry := clone(d.message)
	retry.Attempts++
	return d.queue.Publish(context.Background(), retry)
}

func clone(message Message) Message {
	copyOfPayload := append([]byte(nil), message.Payload...)
	return Message{ID: message.ID, Payload: copyOfPayload, Attempts: message.Attempts}
}
