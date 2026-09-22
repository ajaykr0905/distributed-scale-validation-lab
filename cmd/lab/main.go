package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/runner"
)

func main() {
	count := flag.Int("count", 10_000, "number of synthetic endpoints")
	seed := flag.String("seed", "public-demo", "deterministic dataset seed")
	concurrency := flag.Int("workers", 4, "number of validation workers")
	maxAttempts := flag.Int("max-attempts", 3, "bounded processing attempts")
	flag.Parse()

	summary, err := runner.Run(context.Background(), runner.Options{
		Seed: *seed, Count: *count, Workers: *concurrency, MaxAttempts: *maxAttempts,
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(summary); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
