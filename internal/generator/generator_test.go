package generator

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestGenerateIsDeterministic(t *testing.T) {
	first, err := Generate("public-demo", 128)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Generate("public-demo", 128)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same seed and count produced different datasets")
	}
	if first[0].ID == first[1].ID {
		t.Fatal("generated endpoint IDs must be distinct")
	}
}

func TestGenerateContextPreservesDatasetAndBoundsAllocation(t *testing.T) {
	want, err := Generate("contract", 128)
	if err != nil {
		t.Fatal(err)
	}
	got, err := GenerateContext(context.Background(), "contract", 128)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("dataset changed: %v", err)
	}
	for _, count := range []int{-1, MaxCount + 1, math.MaxInt} {
		if output, err := Generate("contract", count); err == nil || output != nil {
			t.Fatalf("count=%d error=%v", count, err)
		}
	}
	if output, err := Generate("contract", 0); err != nil || len(output) != 0 {
		t.Fatalf("zero: %v", err)
	}
}

func TestGenerateContextCancelsWithoutPartialDataset(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if output, err := GenerateContext(ctx, "contract", math.MaxInt); output != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled: %v", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	started := time.Now()
	output, err := GenerateContext(ctx, strings.Repeat("seed", 4096), MaxCount)
	if output != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("generation did not cancel: %v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("generation ignored deadline for too long")
	}
}

func TestGenerateRejectsInvalidInput(t *testing.T) {
	if _, err := Generate("", 1); err == nil {
		t.Fatal("empty seed should fail")
	}
	if _, err := Generate("demo", -1); err == nil {
		t.Fatal("negative count should fail")
	}
}
