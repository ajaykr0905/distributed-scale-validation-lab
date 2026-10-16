package runner

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRunRejectsUnsupportedOptions(t *testing.T) {
	for _, options := range []Options{
		{Count: 0, Workers: 1, MaxAttempts: 1}, {Count: 100001, Workers: 1, MaxAttempts: 1},
		{Count: 1, Workers: 0, MaxAttempts: 1}, {Count: 1, Workers: 129, MaxAttempts: 1},
		{Count: 1, Workers: 1, MaxAttempts: 0}, {Count: 1, Workers: 1, MaxAttempts: 11},
	} {
		if summary, err := Run(context.Background(), options); err == nil || !reflect.DeepEqual(summary, Summary{}) {
			t.Fatalf("options=%+v summary=%+v error=%v", options, summary, err)
		}
	}
}

func TestRunCompletesExactlyOnceAcrossWorkerCounts(t *testing.T) {
	for _, workers := range []int{1, 4, 128} {
		t.Run(strings.Repeat("w", workers), func(t *testing.T) {
			t.Parallel()
			summary, err := Run(context.Background(), Options{Count: 64, Workers: workers, MaxAttempts: 3})
			if err != nil || summary.Generated != 64 || summary.Stored != 64 || summary.Workers != workers || summary.Seed != "public-demo" || summary.Dataset != "synthetic" {
				t.Fatalf("summary=%+v error=%v", summary, err)
			}
		})
	}
}

func TestRunCancellationCoversGeneration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	options := Options{Seed: strings.Repeat("seed", 4096), Count: 100000, Workers: 4, MaxAttempts: 3}
	if summary, err := Run(ctx, options); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(summary, Summary{}) {
		t.Fatalf("pre-canceled summary=%+v error=%v", summary, err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	started := time.Now()
	if summary, err := Run(ctx, options); !errors.Is(err, context.DeadlineExceeded) || !reflect.DeepEqual(summary, Summary{}) {
		t.Fatalf("deadline summary=%+v error=%v", summary, err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("run did not cancel during generation")
	}
}
