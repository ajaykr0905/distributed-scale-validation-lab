package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/generator"
	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/queue"
	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/store"
	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/validator"
	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/worker"
)

type Options struct {
	Seed        string `json:"seed"`
	Count       int    `json:"count"`
	Workers     int    `json:"workers"`
	MaxAttempts int    `json:"max_attempts"`
}

type Summary struct {
	Dataset   string `json:"dataset"`
	Seed      string `json:"seed"`
	Generated int    `json:"generated"`
	Stored    int    `json:"stored"`
	Workers   int    `json:"workers"`
	ElapsedMS int64  `json:"elapsed_ms"`
}

func Run(ctx context.Context, options Options) (Summary, error) {
	if options.Seed == "" {
		options.Seed = "public-demo"
	}
	if options.Count < 1 || options.Count > 100_000 {
		return Summary{}, fmt.Errorf("count must be between 1 and 100000")
	}
	if options.Workers < 1 || options.Workers > 128 {
		return Summary{}, fmt.Errorf("workers must be between 1 and 128")
	}
	if options.MaxAttempts < 1 || options.MaxAttempts > 10 {
		return Summary{}, fmt.Errorf("max attempts must be between 1 and 10")
	}

	endpoints, err := generator.Generate(options.Seed, options.Count)
	if err != nil {
		return Summary{}, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	q := queue.NewMemory(max(1, len(endpoints)))
	results := store.NewMemory()
	for _, endpoint := range endpoints {
		payload, err := json.Marshal(endpoint)
		if err != nil {
			return Summary{}, err
		}
		message := queue.Message{ID: endpoint.ID + ":validation:v1", Payload: payload}
		if err := q.Publish(ctx, message); err != nil {
			return Summary{}, err
		}
	}

	started := time.Now()
	var next atomic.Int64
	var group sync.WaitGroup
	errCh := make(chan error, 1)
	for range options.Workers {
		group.Add(1)
		go func() {
			defer group.Done()
			processor := worker.New(q, results, validator.Basic{}, worker.WithMaxAttempts(options.MaxAttempts))
			for next.Add(1) <= int64(len(endpoints)) {
				if _, err := processor.ProcessOne(ctx); err != nil {
					select {
					case errCh <- err:
						cancel()
					default:
					}
					return
				}
			}
		}()
	}
	group.Wait()
	select {
	case err := <-errCh:
		return Summary{}, err
	default:
	}

	return Summary{
		Dataset:   "synthetic",
		Seed:      options.Seed,
		Generated: len(endpoints),
		Stored:    results.Len(),
		Workers:   options.Workers,
		ElapsedMS: time.Since(started).Milliseconds(),
	}, nil
}
