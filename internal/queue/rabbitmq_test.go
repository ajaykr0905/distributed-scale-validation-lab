package queue

import (
	"context"
	"errors"
	"sync"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

type recordingAcknowledger struct {
	acks, nacks int
	failAck     bool
}

func (a *recordingAcknowledger) Ack(uint64, bool) error {
	a.acks++
	if a.failAck {
		a.failAck = false
		return errors.New("ack failed")
	}
	return nil
}
func (a *recordingAcknowledger) Nack(uint64, bool, bool) error { a.nacks++; return nil }
func (a *recordingAcknowledger) Reject(uint64, bool) error     { return errors.New("unexpected reject") }

type recordingPublisher struct {
	calls   int
	fail    bool
	message Message
	bounded bool
}

func (p *recordingPublisher) Publish(ctx context.Context, message Message) error {
	p.calls++
	_, p.bounded = ctx.Deadline()
	p.message = message
	if p.fail {
		p.fail = false
		return errors.New("publish failed")
	}
	return nil
}

func TestRabbitRetryRemembersConfirmedPublicationAfterAckFailure(t *testing.T) {
	ack := &recordingAcknowledger{failAck: true}
	publisher := &recordingPublisher{}
	d := &rabbitDelivery{queue: publisher, delivery: amqp.Delivery{Acknowledger: ack, MessageId: "retry", Body: []byte("body"), Headers: amqp.Table{"x-attempts": int32(1)}}}
	if err := d.Nack(true); err == nil {
		t.Fatal("expected ack failure")
	}
	if err := d.Nack(true); err != nil {
		t.Fatal(err)
	}
	if publisher.calls != 1 || ack.acks != 2 || !publisher.bounded || publisher.message.Attempts != 2 || string(publisher.message.Payload) != "body" {
		t.Fatalf("publisher=%+v acknowledger=%+v", publisher, ack)
	}
	if err := d.Ack(); !errors.Is(err, ErrSettled) {
		t.Fatalf("duplicate ack: %v", err)
	}
}

func TestRabbitRetryPublicationFailureLeavesDeliveryUnsettled(t *testing.T) {
	ack := &recordingAcknowledger{}
	publisher := &recordingPublisher{fail: true}
	d := &rabbitDelivery{queue: publisher, delivery: amqp.Delivery{Acknowledger: ack}}
	if err := d.Nack(true); err == nil || ack.acks != 0 {
		t.Fatalf("failed publish settled original: %v %+v", err, ack)
	}
	if err := d.Nack(true); err != nil || publisher.calls != 2 || ack.acks != 1 {
		t.Fatalf("retry: %v publisher=%+v acknowledger=%+v", err, publisher, ack)
	}
}

func TestRabbitConcurrentSettlementSendsOneBrokerCommand(t *testing.T) {
	ack := &recordingAcknowledger{}
	d := &rabbitDelivery{delivery: amqp.Delivery{Acknowledger: ack}}
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			if i%2 == 0 {
				err = d.Ack()
			} else {
				err = d.Nack(false)
			}
			if err != nil && !errors.Is(err, ErrSettled) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if ack.acks+ack.nacks != 1 {
		t.Fatalf("acknowledger=%+v", ack)
	}
}

func TestAttemptsFromRabbitHeaders(t *testing.T) {
	for _, test := range []struct {
		value any
		want  int
	}{
		{value: int32(2), want: 2},
		{value: int64(4), want: 4},
		{value: "invalid", want: 0},
	} {
		if got := attemptsFromHeaders(amqp.Table{"x-attempts": test.value}); got != test.want {
			t.Fatalf("attempt header %v: got %d want %d", test.value, got, test.want)
		}
	}
}
