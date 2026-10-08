# Reproducible benchmark runbook

Verified results live in `artifacts/` and include every sample, the workload,
adapter selection, environment, and limitations. A checked-in result is a
record of one declared environment, never a universal performance claim.

## Real asynchronous backend measurements

Use the isolated services and environment from the README, then:

```sh
go build -o bin/durable ./cmd/durable
go build -o bin/durable-benchmark ./cmd/benchmark
export SCALE_LAB_DOCKER_PROJECT=ajay-durable-local
bin/durable-benchmark -bin bin/durable -seed public-durable-v1 \
  -warmup 100 -count 1000 -output artifacts/local/durable-benchmark.json
```

The harness launches a separate submission API, outbox dispatcher, and 1, 2, 4,
or 8 independent worker processes. Each group gets an unmeasured warmup followed
by three measured trials, all with the same seeded endpoints. Idempotency keys
are unique per trial so every measured job is actually executed.

It records every acceptance-to-result-commit latency, throughput (including
HTTP submission and polling), p50/p95/p99, ready/dead queue samples, retries,
duplicates, and aggregate API/dispatcher/worker RSS and CPU samples. Percentiles
use the floor of `(n-1)*p` over sorted samples; raw latencies retain job-ID order.
Measurements use the application's PostgreSQL clock for acceptance and commit.
RSS/CPU comes from `ps` and excludes broker/database/Docker-VM resources. The
CPU percentage is a process-lifetime average, not interval utilization. Samples
start before acceptance and continue across enqueue and drain at approximately
50 ms intervals; sampling may miss brief peaks. The report inspects only the
named Compose project's two service containers and records their actual image
references and immutable local image IDs. The
single sequential producer and per-event dispatcher confirms can bottleneck the
run; increased worker count need not increase throughput.

The ignored `artifacts/local/` output records source SHA and dirty status. Run against a clean tested
commit, retain all twelve trials, and report failures rather than retaining only
the successful configurations. Broker recovery timing is emitted separately by
`TestIndependentProcessesAndBrokerRestart`, includes Compose start/health wait,
and is not a throughput sample. No benchmark command injects a failure.

Set `SCALE_LAB_RECOVERY_RECEIPT` to an absolute ignored JSON output path when
running the recovery integration test to record its actual restart duration,
accounting, source SHA, and measurement boundary. The receipt is written only
after the independent processes exit gracefully.

Each trial uses a distinct queue; named PostgreSQL history and RabbitMQ topology
remain available for inspection. Stop the isolated Compose project afterwards.

## Original unit-level worker benchmark

```sh
./scripts/run-benchmark.sh | tee benchmark.txt
```

The script records the Go version and operating-system information, then runs
five samples with allocation reporting. `benchmark.txt` is ignored by Git so a
machine-specific result is not accidentally presented as a universal claim.

## End-to-end synthetic run

Build once, then run the same seed and count for each comparison:

```sh
go build -o bin/scale-lab ./cmd/lab
bin/scale-lab -seed public-demo -count 10000 -workers 1
bin/scale-lab -seed public-demo -count 10000 -workers 4
```

For a larger local exercise, change `-count` to the intended synthetic entity
count. Do not describe that count as validated until the command finishes and
the JSON output reports identical `generated` and `stored` values.

## Reporting checklist

- Commit SHA and clean/dirty working-tree state
- CPU model, core count, memory, OS, and Go version
- Exact command, seed, entity count, and worker count
- All samples, not only the fastest result
- Whether the run used memory adapters or separately implemented integrations
- Failures, retries, and resource saturation observed during the run
