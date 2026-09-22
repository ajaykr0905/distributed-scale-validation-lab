package queue

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

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
