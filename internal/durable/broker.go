package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Broker is single-publisher/single-consumer. Each worker process owns its
// channel and prefetch=1 bounds the number of uncommitted deliveries it holds.
type Broker struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	name       string
	returns    <-chan amqp.Return
	deliveries <-chan amqp.Delivery
}

func Connect(ctx context.Context, url, name string, consume bool) (*Broker, error) {
	if name == "" {
		return nil, errors.New("queue name required")
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	connection, err := amqp.DialConfig(url, amqp.Config{Heartbeat: 5 * time.Second, Dial: func(network, address string) (net.Conn, error) { return dialer.DialContext(ctx, network, address) }})
	if err != nil {
		return nil, err
	}
	b := &Broker{connection: connection, name: name}
	b.channel, err = connection.Channel()
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	if err := b.configure(consume); err != nil {
		_ = b.Close()
		return nil, err
	}
	return b, nil
}

func (b *Broker) configure(consume bool) error {
	for _, suffix := range []string{"work", "dead"} {
		if err := b.channel.ExchangeDeclare(b.name+"."+suffix, "direct", true, false, false, false, nil); err != nil {
			return err
		}
		args := amqp.Table(nil)
		if suffix == "work" {
			args = amqp.Table{"x-dead-letter-exchange": b.name + ".dead", "x-dead-letter-routing-key": b.name}
		}
		queueName := b.name
		if suffix == "dead" {
			queueName += ".dead"
		}
		if _, err := b.channel.QueueDeclare(queueName, true, false, false, false, args); err != nil {
			return err
		}
		if err := b.channel.QueueBind(queueName, b.name, b.name+"."+suffix, false, nil); err != nil {
			return err
		}
	}
	if err := b.channel.Confirm(false); err != nil {
		return err
	}
	b.returns = b.channel.NotifyReturn(make(chan amqp.Return, 1))
	if consume {
		if err := b.channel.Qos(1, 0, false); err != nil {
			return err
		}
		var err error
		b.deliveries, err = b.channel.Consume(b.name, "", false, false, false, false, nil)
		return err
	}
	return nil
}

func (b *Broker) Publish(ctx context.Context, destination string, event Event) error {
	if destination != "work" && destination != "dead" {
		return errors.New("invalid outbox destination")
	}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	confirmation, err := b.channel.PublishWithDeferredConfirmWithContext(ctx, b.name+"."+destination, b.name, true, false, amqp.Publishing{DeliveryMode: amqp.Persistent, ContentType: "application/json", MessageId: event.JobID, Body: body})
	if err != nil {
		return err
	}
	ack, err := confirmation.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !ack {
		return errors.New("broker rejected publish")
	}
	select {
	case returned := <-b.returns:
		return fmt.Errorf("unroutable publish: %s", returned.ReplyText)
	default:
	}
	return nil
}

func (b *Broker) Receive(ctx context.Context) (amqp.Delivery, error) {
	select {
	case <-ctx.Done():
		return amqp.Delivery{}, ctx.Err()
	case delivery, open := <-b.deliveries:
		if !open {
			return amqp.Delivery{}, errors.New("broker connection closed")
		}
		return delivery, nil
	}
}

func (b *Broker) Depth() (ready, dead int, errorValue error) {
	q, err := b.channel.QueueInspect(b.name)
	if err != nil {
		return 0, 0, err
	}
	dq, err := b.channel.QueueInspect(b.name + ".dead")
	return q.Messages, dq.Messages, err
}

func (b *Broker) Close() error { return errors.Join(b.channel.Close(), b.connection.Close()) }
