// benchmark measures the real HTTP -> PostgreSQL outbox -> RabbitMQ -> worker
// process path. It never substitutes memory adapters or synthesizes metrics.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/durable"
	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/generator"
)

type sample struct {
	At         time.Time `json:"at"`
	Completed  int       `json:"completed"`
	QueueReady int       `json:"queue_ready"`
	DeadReady  int       `json:"dead_ready"`
	RSSKiB     int64     `json:"process_rss_kib"`
	CPUPercent float64   `json:"process_cpu_percent"`
}

type trial struct {
	Workers         int       `json:"workers"`
	Trial           int       `json:"trial"`
	Jobs            int       `json:"jobs"`
	DurationSeconds float64   `json:"duration_seconds"`
	Throughput      float64   `json:"completed_jobs_per_second"`
	P50MS           float64   `json:"p50_accept_to_commit_ms"`
	P95MS           float64   `json:"p95_accept_to_commit_ms"`
	P99MS           float64   `json:"p99_accept_to_commit_ms"`
	Retries         int64     `json:"retries"`
	Duplicates      int64     `json:"duplicates"`
	LatencyMS       []float64 `json:"raw_accept_to_commit_ms"`
	Samples         []sample  `json:"samples"`
}

type report struct {
	CreatedAt   time.Time `json:"created_at"`
	Commit      string    `json:"commit"`
	Dirty       bool      `json:"dirty"`
	GoVersion   string    `json:"go_version"`
	OS          string    `json:"os"`
	Arch        string    `json:"arch"`
	CPUs        int       `json:"logical_cpus"`
	Seed        string    `json:"seed"`
	WarmupJobs  int       `json:"warmup_jobs_per_worker_count"`
	Trials      []trial   `json:"trials"`
	ImagePins   []string  `json:"image_pins"`
	Limitations []string  `json:"limitations"`
}

func main() {
	bin := flag.String("bin", "bin/durable", "compiled durable executable")
	count := flag.Int("count", 1000, "same seeded jobs per measured trial")
	warmup := flag.Int("warmup", 100, "unmeasured warmup jobs per worker count")
	seed := flag.String("seed", "public-durable-v1", "synthetic workload seed")
	output := flag.String("output", "artifacts/local/durable-benchmark.json", "raw report path")
	flag.Parse()
	if *count < 1 || *count > 100000 || *warmup < 1 || *warmup > 10000 || *seed == "" {
		fatal(errors.New("invalid bounded workload"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	db, err := durable.Open(ctx, os.Getenv("SCALE_LAB_POSTGRES_URL"))
	if err != nil {
		fatal(err)
	}
	defer db.Close()
	if err := db.EnsureSchema(ctx); err != nil {
		fatal(err)
	}
	r := report{CreatedAt: time.Now().UTC(), GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU(), Seed: *seed, WarmupJobs: *warmup, ImagePins: []string{"postgres:16.15-alpine@sha256:721873c34ceb9f8d8fc265984940dc982404c105f19ad51be9fdc5970a6080ea", "rabbitmq:4.2.9-management-alpine@sha256:2b5d3d29f4dde853995aac1bfbdd71c7efc764dea6409c14e2f355a79a946998"}, Limitations: []string{"Single-host local process benchmark; broker and database run in Docker Desktop, not a distributed cluster.", "Sequential HTTP producer and per-event outbox confirms can limit throughput.", "RSS/CPU samples cover API, dispatcher, and worker processes only; exclude broker/database/VM resources.", "ps CPU percentage is a process-lifetime average, not interval CPU utilization.", "Resource/queue samples start before acceptance and run throughout enqueue and drain at approximately 50ms intervals; brief peaks may still be missed.", "Queue samples measure ready messages, not unacknowledged deliveries.", "No broker outage is injected by this performance command; process-death and measured outage recovery are separate integration evidence."}}
	if bytes, err := exec.Command("git", "rev-parse", "HEAD").Output(); err == nil {
		r.Commit = strings.TrimSpace(string(bytes))
	}
	if bytes, err := exec.Command("git", "status", "--porcelain").Output(); err == nil {
		r.Dirty = len(bytes) > 0
	}
	project := os.Getenv("SCALE_LAB_DOCKER_PROJECT")
	if !strings.HasPrefix(project, "ajay-durable-") {
		fatal(errors.New("SCALE_LAB_DOCKER_PROJECT must identify the isolated ajay-durable-* public Compose project"))
	}
	ids, err := exec.CommandContext(ctx, "docker", "compose", "-p", project, "-f", "compose.durable.yaml", "ps", "-q", "postgres", "rabbitmq").Output()
	if err != nil {
		fatal(err)
	}
	containerIDs := strings.Fields(string(ids))
	if len(containerIDs) != 2 {
		fatal(errors.New("expected two running isolated PostgreSQL/RabbitMQ containers"))
	}
	images, err := exec.CommandContext(ctx, "docker", append([]string{"inspect", "--format", "{{.Config.Image}} {{.Image}}"}, containerIDs...)...).Output()
	if err != nil {
		fatal(err)
	}
	r.ImagePins = strings.Split(strings.TrimSpace(string(images)), "\n")
	for _, workers := range []int{1, 2, 4, 8} {
		values, err := measure(ctx, db, *bin, *seed, *warmup, *count, workers)
		if err != nil {
			fatal(err)
		}
		r.Trials = append(r.Trials, values...)
	}
	body, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(*output), 0755); err != nil {
		fatal(err)
	}
	if err := os.WriteFile(*output, append(body, '\n'), 0644); err != nil {
		fatal(err)
	}
	for _, v := range r.Trials {
		fmt.Printf("workers=%d trial=%d jobs=%d throughput=%.2f/s p95=%.2fms retries=%d duplicates=%d\n", v.Workers, v.Trial, v.Jobs, v.Throughput, v.P95MS, v.Retries, v.Duplicates)
	}
	fmt.Printf("raw evidence: %s\n", *output)
}

func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }

func measure(ctx context.Context, db *durable.DB, bin, seed string, warmup, count, workers int) ([]trial, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	address := listener.Addr().String()
	_ = listener.Close()
	queueName := "durable-benchmark-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	var processes []*exec.Cmd
	start := func(role string) error {
		command := exec.CommandContext(ctx, bin, "-role", role, "-address", address, "-queue", queueName)
		command.Env = os.Environ()
		command.Stderr = os.Stderr
		if err := command.Start(); err != nil {
			return err
		}
		processes = append(processes, command)
		return nil
	}
	defer func() {
		for _, command := range processes {
			_ = command.Process.Signal(syscall.SIGTERM)
		}
		for _, command := range processes {
			_ = command.Wait()
		}
	}()
	for _, role := range []string{"api", "dispatcher"} {
		if err := start(role); err != nil {
			return nil, err
		}
	}
	for range workers {
		if err := start("worker"); err != nil {
			return nil, err
		}
	}
	client := &http.Client{Timeout: 10 * time.Second}
	base := "http://" + address
	ready := false
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		response, err := client.Get(base + "/healthz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == 200 {
				ready = true
				break
			}
		}
	}
	if !ready {
		return nil, errors.New("API did not become ready")
	}
	broker, err := durable.Connect(ctx, os.Getenv("SCALE_LAB_RABBITMQ_URL"), queueName, false)
	if err != nil {
		return nil, err
	}
	defer broker.Close()
	var values []trial
	for index := 0; index <= 3; index++ {
		n := count
		if index == 0 {
			n = warmup
		}
		endpoints, err := generator.Generate(seed, n)
		if err != nil {
			return nil, err
		}
		var ids []string
		for i, endpoint := range endpoints {
			id, _, _, err := durable.RequestIdentity(fmt.Sprintf("%s-%d-%d", queueName, index, i), endpoint)
			if err != nil {
				return nil, err
			}
			ids = append(ids, id)
		}
		initial, err := captureSample(ctx, db, broker, processes, ids)
		if err != nil {
			return nil, err
		}
		start := time.Now()
		sampleCtx, stopSamples := context.WithCancel(ctx)
		sampleResult := make(chan sampleBatch, 1)
		go func() { sampleResult <- collectSamples(sampleCtx, db, broker, processes, ids, initial) }()
		for i, endpoint := range endpoints {
			body, _ := json.Marshal(endpoint)
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/api/v1/jobs", bytes.NewReader(body))
			if err != nil {
				stopSamples()
				return nil, err
			}
			request.Header.Set("Idempotency-Key", fmt.Sprintf("%s-%d-%d", queueName, index, i))
			response, err := client.Do(request)
			if err != nil {
				stopSamples()
				return nil, err
			}
			var job durable.Job
			err = json.NewDecoder(response.Body).Decode(&job)
			_ = response.Body.Close()
			if err != nil || response.StatusCode != 202 {
				stopSamples()
				return nil, fmt.Errorf("acceptance failed: status=%d err=%v", response.StatusCode, err)
			}
			if job.ID != ids[i] {
				stopSamples()
				return nil, errors.New("unexpected job identity")
			}
		}
		var completed int
		for completed < n {
			if err := db.SQL.QueryRowContext(ctx, "SELECT count(*) FROM durable_jobs WHERE id=ANY($1) AND status='completed'", ids).Scan(&completed); err != nil {
				stopSamples()
				return nil, err
			}
			if completed < n {
				select {
				case <-ctx.Done():
					stopSamples()
					return nil, ctx.Err()
				case <-time.After(20 * time.Millisecond):
				}
			}
		}
		duration := time.Since(start).Seconds()
		stopSamples()
		batch := <-sampleResult
		if batch.Err != nil {
			return nil, batch.Err
		}
		if index == 0 {
			continue
		}
		rows, err := db.SQL.QueryContext(ctx, "SELECT extract(epoch FROM (completed_at-accepted_at))*1000, GREATEST(attempts-1,0), duplicates FROM durable_jobs WHERE id=ANY($1) ORDER BY id", ids)
		if err != nil {
			return nil, err
		}
		var latencies []float64
		var retries, duplicates int64
		for rows.Next() {
			var latency float64
			var retry, duplicate int64
			if err := rows.Scan(&latency, &retry, &duplicate); err != nil {
				_ = rows.Close()
				return nil, err
			}
			latencies = append(latencies, latency)
			retries += retry
			duplicates += duplicate
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
		if len(latencies) != n {
			return nil, errors.New("missing durable latency samples")
		}
		values = append(values, trial{Workers: workers, Trial: index, Jobs: n, DurationSeconds: duration, Throughput: float64(n) / duration, P50MS: percentile(latencies, .5), P95MS: percentile(latencies, .95), P99MS: percentile(latencies, .99), Retries: retries, Duplicates: duplicates, LatencyMS: latencies, Samples: batch.Samples})
	}
	return values, nil
}

type sampleBatch struct {
	Samples []sample
	Err     error
}

func collectSamples(ctx context.Context, db *durable.DB, broker *durable.Broker, processes []*exec.Cmd, ids []string, initial sample) sampleBatch {
	samples := []sample{initial}
	for {
		value, err := captureSample(ctx, db, broker, processes, ids)
		if err != nil {
			if ctx.Err() != nil {
				return sampleBatch{Samples: samples}
			}
			return sampleBatch{Err: err}
		}
		samples = append(samples, value)
		select {
		case <-ctx.Done():
			return sampleBatch{Samples: samples}
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func captureSample(ctx context.Context, db *durable.DB, broker *durable.Broker, processes []*exec.Cmd, ids []string) (sample, error) {
	var completed int
	if err := db.SQL.QueryRowContext(ctx, "SELECT count(*) FROM durable_jobs WHERE id=ANY($1) AND status='completed'", ids).Scan(&completed); err != nil {
		return sample{}, err
	}
	ready, dead, err := broker.Depth()
	if err != nil {
		return sample{}, err
	}
	rss, cpu, err := resources(processes)
	if err != nil {
		return sample{}, err
	}
	return sample{At: time.Now().UTC(), Completed: completed, QueueReady: ready, DeadReady: dead, RSSKiB: rss, CPUPercent: cpu}, nil
}

func resources(processes []*exec.Cmd) (int64, float64, error) {
	var ids []string
	for _, command := range processes {
		ids = append(ids, strconv.Itoa(command.Process.Pid))
	}
	output, err := exec.Command("ps", "-o", "rss=,pcpu=", "-p", strings.Join(ids, ",")).Output()
	if err != nil {
		return 0, 0, err
	}
	var rss int64
	var cpu float64
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return 0, 0, errors.New("unexpected ps resource output")
		}
		r, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return 0, 0, err
		}
		c, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return 0, 0, err
		}
		rss += r
		cpu += c
	}
	return rss, cpu, nil
}

func percentile(values []float64, p float64) float64 {
	copyValues := append([]float64(nil), values...)
	sort.Float64s(copyValues)
	index := int(float64(len(copyValues)-1) * p)
	return copyValues[index]
}
