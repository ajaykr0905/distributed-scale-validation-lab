# Local durable-backend evidence, 8 October 2026

This is the **preserved pre-fix snapshot** for source `581883c`. Its latency
fields use SQL timestamps assigned before the respective transactions commit;
they omit commit/fsync delay and are not observed end-to-end result latencies.
The raw JSON is unchanged for provenance. Use the v2 report for the corrected
client-observed committed-result metric and connection-startup reliability fix.

Source: `581883c986fc847343bebe0b8954ee421c6fb0fd`, clean worktree at both
measurement starts. Go 1.26.4, Darwin/arm64, 11 logical CPUs; PostgreSQL and
RabbitMQ ran in the isolated Docker Desktop `ajay-durable-20261008` project.
Image references and immutable local image IDs are in the raw benchmark.
Compose also pins public upstream manifest digests.

## Workload and all measured trials

Each worker count used a separate API, outbox dispatcher, and independent worker
processes; 100 warmup jobs, then three 1,000-job trials. Every group uses the same
`public-durable-v1` deterministic endpoint fixture. All 12,000 measured jobs and
400 warmup jobs completed with 12,400 durable result rows; final pending outbox
count was zero. No validation retries or duplicate notifications occurred in
the normal-load benchmark.

| Workers | Trial | Jobs/s | SQL interval p50 ms | SQL interval p95 ms | SQL interval p99 ms |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 232.77 | 27.427 | 44.401 | 57.566 |
| 1 | 2 | 204.55 | 27.858 | 46.586 | 54.305 |
| 1 | 3 | 285.12 | 26.825 | 36.740 | 39.884 |
| 2 | 1 | 227.21 | 24.433 | 39.250 | 46.278 |
| 2 | 2 | 251.49 | 24.621 | 35.754 | 44.403 |
| 2 | 3 | 266.23 | 23.868 | 33.028 | 36.788 |
| 4 | 1 | 268.12 | 23.355 | 32.117 | 35.181 |
| 4 | 2 | 253.94 | 23.183 | 32.235 | 40.287 |
| 4 | 3 | 265.80 | 23.222 | 31.004 | 34.919 |
| 8 | 1 | 233.45 | 22.681 | 31.494 | 33.839 |
| 8 | 2 | 170.07 | 23.223 | 32.467 | 39.548 |
| 8 | 3 | 151.41 | 23.424 | 33.420 | 48.969 |

Raw evidence: [all latencies and continuous samples](2026-10-08-durable-benchmark.json).
No sample or slower configuration has been removed. This pre-fix SQL timestamp
interval uses PostgreSQL timestamps assigned before commit. Throughput includes HTTP producer time and
completion polling. The sample stream begins before submission and continues
through enqueue and drain, approximately every 50 ms.

Ready-queue sample maxima were 0–2 messages. Maximum sampled aggregate process
RSS ranged from 57,088 to 172,592 KiB across trials. That excludes PostgreSQL,
RabbitMQ, Docker VM, and unrelated host processes. `ps` CPU percentages are
process-lifetime averages, not interval utilization. Sampling can miss brief
peaks and adds measurement overhead.

The lightweight fingerprint workload, sequential producer, and one-event
confirmed dispatcher leave little ready-queue backlog. More workers did not
produce linear throughput scaling; eight-worker throughput was lower in this
local run. These results demonstrate the actual external path and its current
limits, not distributed-cluster capacity, production throughput, or a proven
worker-saturation curve.

## Fault and recovery evidence

The full `go test -race -tags=integration -count=1 -v ./...` suite passed on the
same source. It exercises:

- Concurrent idempotent admission, payload conflict, and capacity rejection.
- Real worker subprocess death before commit and after commit before ack.
- Real dispatcher subprocess death after publish before outbox commit.
- A live broker stop/start while a job is accepted durably.
- Duplicate broker delivery, SQL write rollback, bounded validation retries,
  and a real dead queue.
- Independent dispatcher/worker graceful process shutdown.

The [broker recovery receipt](2026-10-08-recovery.json) records **4.9480235
seconds**, from Compose restart invocation through its health wait and the
accepted outage job's result commit. It is one local recovery sample, not a
general SLO. Its final accounting is 2 accepted, 2 completed, 2 result rows,
zero pending/retrying/failed, and one duplicate after deliberate replay.

## Reproduce

Use the README's clean-clone service setup and environment, then:

```sh
export SCALE_LAB_DOCKER_PROJECT=ajay-durable-local
SCALE_LAB_INTEGRATION=1 go test -race -tags=integration -count=1 -v ./...
go build -o bin/durable ./cmd/durable
go build -o bin/durable-benchmark ./cmd/benchmark
bin/durable-benchmark -bin bin/durable -seed public-durable-v1 \
  -warmup 100 -count 1000 -output artifacts/local/durable-benchmark.json
```

To emit a recovery receipt, additionally set `SCALE_LAB_RECOVERY_RECEIPT` to an
absolute path under the ignored `artifacts/local/` directory. That receipt is
written only after accounting checks and graceful child-process exit pass.
