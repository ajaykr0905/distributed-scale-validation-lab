package worker

import (
	"context"
	"fmt"
	"testing"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/queue"
	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/store"
)

func BenchmarkProcessOne(b *testing.B) {
	q := queue.NewMemory(1)
	s := store.NewMemory()
	v := &controlledValidator{}
	w := New(q, s, v)
	ctx := context.Background()
	payload := testMessage(b, "template").Payload

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		message := queue.Message{ID: fmt.Sprintf("benchmark-%d", i), Payload: payload}
		if err := q.Publish(ctx, message); err != nil {
			b.Fatal(err)
		}
		if _, err := w.ProcessOne(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
