package queue

import (
	"context"
	"errors"
	"fmt"
	"net"
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
	message.Attempts++
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
	queue    *RabbitMQ
	delivery amqp.Delivery
}

func (d *rabbitDelivery) Message() Message {
	return Message{
		ID:       d.delivery.MessageId,
		Payload:  append([]byte(nil), d.delivery.Body...),
		Attempts: attemptsFromHeaders(d.delivery.Headers),
	}
}

func (d *rabbitDelivery) Ack() error {
	return d.delivery.Ack(false)
}

func (d *rabbitDelivery) Nack(requeue bool) error {
	if !requeue {
		return d.delivery.Nack(false, false)
	}
	message := d.Message()
	message.Attempts++
	if err := d.queue.Publish(context.Background(), message); err != nil {
		return err
	}
	return d.delivery.Ack(false)
}

func attemptsFromHeaders(headers amqp.Table) int {
	value, exists := headers["x-attempts"]
	if !exists {
		return 0
	}
	switch typed := value.(type) {
	case int:
		return typed
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case int16:
		return int(typed)
	case int8:
		return int(typed)
	default:
		return 0
	}
}
