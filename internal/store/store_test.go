package store

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/model"
)

func TestMemoryCanceledWritesDoNotInsertOrDeduplicate(t *testing.T) {
	s := NewMemory()
	result := model.ValidationResult{MessageID: "one"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if inserted, err := s.PutIfAbsent(ctx, result); inserted || !errors.Is(err, context.Canceled) || s.Len() != 0 {
		t.Fatalf("inserted=%t error=%v len=%d", inserted, err, s.Len())
	}
	if _, err := s.PutIfAbsent(context.Background(), result); err != nil {
		t.Fatal(err)
	}
	if inserted, err := s.PutIfAbsent(ctx, result); inserted || !errors.Is(err, context.Canceled) {
		t.Fatalf("duplicate inserted=%t error=%v", inserted, err)
	}
}

func TestMemoryConcurrentWritersInsertOnlyOnce(t *testing.T) {
	s := NewMemory()
	var inserted atomic.Int32
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := s.PutIfAbsent(context.Background(), model.ValidationResult{MessageID: "same"})
			if err != nil {
				t.Error(err)
			}
			if ok {
				inserted.Add(1)
			}
			_ = s.Len()
		}()
	}
	wg.Wait()
	if inserted.Load() != 1 || s.Len() != 1 {
		t.Fatalf("insertions=%d size=%d", inserted.Load(), s.Len())
	}
}
