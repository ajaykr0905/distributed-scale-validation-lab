package durable

import (
	"context"
	"net"
	"testing"
	"time"
)

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
