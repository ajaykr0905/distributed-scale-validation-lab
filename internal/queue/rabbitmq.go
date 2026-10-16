package queue

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	connection   *amqp.Connection
	publisher    *amqp.Channel
	consumer     *amqp.Channel
	deliveries   <-chan amqp.Delivery
	workExchange string
	deadExchange string
	routingKey   string
}

func NewRabbitMQ(ctx context.Context, url, queueName string) (*RabbitMQ, error) {
	if queueName == "" {
		return nil, fmt.Errorf("queue name is required")
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	connection, err := amqp.DialConfig(url, amqp.Config{
		Heartbeat: 10 * time.Second,
		Locale:    "en_US",
		Dial: func(network, address string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, address)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}
	result := &RabbitMQ{
		connection:   connection,
		workExchange: queueName + ".work",
		deadExchange: queueName + ".dead",
		routingKey:   queueName,
	}
	if err := result.configure(queueName); err != nil {
		_ = connection.Close()
		return nil, err
	}
	return result, nil
}

func (q *RabbitMQ) configure(queueName string) error {
	topology, err := q.connection.Channel()
	if err != nil {
		return fmt.Errorf("open topology channel: %w", err)
	}
	defer topology.Close()
	for _, exchange := range []string{q.workExchange, q.deadExchange} {
		if err := topology.ExchangeDeclare(exchange, "direct", true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare exchange %q: %w", exchange, err)
		}
	}
	if _, err := topology.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare work queue: %w", err)
	}
	if err := topology.QueueBind(queueName, q.routingKey, q.workExchange, false, nil); err != nil {
		return fmt.Errorf("bind work queue: %w", err)
	}
	deadQueue := queueName + ".dead"
	if _, err := topology.QueueDeclare(deadQueue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dead queue: %w", err)
	}
	if err := topology.QueueBind(deadQueue, q.routingKey, q.deadExchange, false, nil); err != nil {
		return fmt.Errorf("bind dead queue: %w", err)
	}

	q.publisher, err = q.connection.Channel()
	if err != nil {
		return fmt.Errorf("open publisher channel: %w", err)
	}
	if err := q.publisher.Confirm(false); err != nil {
		return fmt.Errorf("enable publisher confirms: %w", err)
	}
	q.consumer, err = q.connection.Channel()
	if err != nil {
		return fmt.Errorf("open consumer channel: %w", err)
	}
	if err := q.consumer.Qos(32, 0, false); err != nil {
		return fmt.Errorf("configure consumer qos: %w", err)
	}
	q.deliveries, err = q.consumer.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume work queue: %w", err)
	}
	return nil
}

func (q *RabbitMQ) Publish(ctx context.Context, message Message) error {
	if err := checkAttempts(message.Attempts); err != nil {
		return err
	}
	return q.publishConfirmed(ctx, q.workExchange, message, amqp.Table{
		"x-attempts": int32(message.Attempts),
	})
}

func (q *RabbitMQ) Receive(ctx context.Context) (Delivery, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case delivery, open := <-q.deliveries:
		if !open {
			return nil, errors.New("rabbitmq delivery channel closed")
		}
		return &rabbitDelivery{queue: q, delivery: delivery}, nil
	}
}

func (q *RabbitMQ) DeadLetter(ctx context.Context, message Message, cause error) error {
	if err := checkAttempts(message.Attempts); err != nil {
		return err
	}
	if message.Attempts < MaxAttemptCount {
		message.Attempts++
	}
	return q.publishConfirmed(ctx, q.deadExchange, message, amqp.Table{
		"x-attempts":    int32(message.Attempts),
		"x-error-class": fmt.Sprintf("%T", cause),
		"x-error":       cause.Error(),
	})
}

func (q *RabbitMQ) publishConfirmed(ctx context.Context, exchange string, message Message, headers amqp.Table) error {
	confirmation, err := q.publisher.PublishWithDeferredConfirmWithContext(
		ctx,
		exchange,
		q.routingKey,
		true,
		false,
		amqp.Publishing{
			Headers:      headers,
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    message.ID,
			Timestamp:    time.Now().UTC(),
			Body:         append([]byte(nil), message.Payload...),
		},
	)
	if err != nil {
		return fmt.Errorf("publish message %q: %w", message.ID, err)
	}
	acknowledged, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("wait for publish confirmation %q: %w", message.ID, err)
	}
	if !acknowledged {
		return fmt.Errorf("broker negatively acknowledged message %q", message.ID)
	}
	return nil
}

func (q *RabbitMQ) Close() error {
	return errors.Join(q.consumer.Close(), q.publisher.Close(), q.connection.Close())
}

type rabbitDelivery struct {
	queue interface {
		Publish(context.Context, Message) error
	}
	delivery    amqp.Delivery
	mu          sync.Mutex
	settled     bool
	republished bool
}

func (d *rabbitDelivery) Message() Message {
	return Message{
		ID:       d.delivery.MessageId,
		Payload:  append([]byte(nil), d.delivery.Body...),
		Attempts: attemptsFromHeaders(d.delivery.Headers),
	}
}

func (d *rabbitDelivery) Ack() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.settled {
		return ErrSettled
	}
	err := d.delivery.Ack(false)
	if err == nil {
		d.settled = true
	}
	return err
}

func (d *rabbitDelivery) Nack(requeue bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.settled {
		return ErrSettled
	}
	if !requeue && !d.republished {
		err := d.delivery.Nack(false, false)
		if err == nil {
			d.settled = true
		}
		return err
	}
	if !d.republished {
		message, err := retryMessage(d.Message())
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := d.queue.Publish(ctx, message); err != nil {
			return err
		}
		d.republished = true
	}
	// A successful retry publication is remembered even if acknowledging the
	// original fails, so retrying settlement cannot publish a second copy.
	err := d.delivery.Ack(false)
	if err == nil {
		d.settled = true
	}
	return err
}

func attemptsFromHeaders(headers amqp.Table) int {
	value, exists := headers["x-attempts"]
	if !exists {
		return 0
	}
	var attempts int64
	switch typed := value.(type) {
	case int:
		attempts = int64(typed)
	case int32:
		attempts = int64(typed)
	case int64:
		attempts = typed
	case int16:
		attempts = int64(typed)
	case int8:
		attempts = int64(typed)
	default:
		return MaxAttemptCount
	}
	if attempts < 0 || attempts > MaxAttemptCount {
		return MaxAttemptCount
	}
	return int(attempts)
}
