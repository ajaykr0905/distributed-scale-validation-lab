package durable

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

func TestBrokerRejectsInvalidOperationsBeforeNetworkAccess(t *testing.T) {
	b := &Broker{}
	for _, event := range []Event{{JobID: "bad", Attempt: 1}, {JobID: strings.Repeat("a", 64), Attempt: 11}} {
		if err := b.Publish(context.Background(), "work", event); !errors.Is(err, ErrInvalidEvent) {
			t.Fatalf("publish: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Publish(ctx, "work", Event{JobID: strings.Repeat("a", 64), Attempt: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled publish: %v", err)
	}
	if _, err := b.Receive(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled receive: %v", err)
	}
	if _, err := b.Receive(context.Background()); err == nil {
		t.Fatal("publisher-only broker accepted Receive")
	}
	if err := b.Publish(context.Background(), "unknown", Event{}); err == nil {
		t.Fatal("unknown destination accepted")
	}
}

func TestConnectBoundsSilentAMQPHandshake(t *testing.T) {
	for _, mode := range []string{"deadline", "cancellation"} {
		t.Run(mode, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			accepted := make(chan net.Conn, 1)
			go func() {
				conn, err := listener.Accept()
				if err == nil {
					accepted <- conn
				}
			}()
			ctx, cancel := context.WithCancel(context.Background())
			if mode == "deadline" {
				ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
			}
			defer cancel()
			finished := make(chan error, 1)
			go func() {
				broker, err := Connect(ctx, "amqp://guest:guest@"+listener.Addr().String()+"/", "stalled-test", false)
				if broker != nil {
					_ = broker.Close()
				}
				finished <- err
			}()
			var server net.Conn
			select {
			case server = <-accepted:
			case <-time.After(time.Second):
				t.Fatal("TCP connection not accepted")
			}
			defer server.Close()
			if mode == "cancellation" {
				cancel()
			}
			select {
			case err := <-finished:
				if err == nil {
					t.Fatal("silent peer accepted as AMQP broker")
				}
			case <-time.After(500 * time.Millisecond):
				_ = server.Close()
				<-finished
				t.Fatal("Connect ignored context during AMQP handshake")
			}
		})
	}
}
