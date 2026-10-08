# Verified observed-result measurements, 8 October 2026

Source: `6b01125cb71d49fc2fba1cf009aeb7d45e61cab6`, clean at both measurement
starts. Go 1.26.4, Darwin/arm64, 11 logical CPUs; public PostgreSQL/RabbitMQ ran
in the isolated Docker Desktop `ajay-durable-20261008` project. Exact configured
image references and immutable image IDs are in the raw benchmark; Compose
pins upstream manifest digests.

The original run recorded checkout HEAD, but did not capture executable hashes
at measurement time. [Later inspection of the original binaries](2026-10-08-binary-inspection-v2.json)
found both declaring the expected main packages and clean source `6b01125`.
That supports current binary/source correspondence; it cannot retroactively
prove the exact bytes executed during the twelve trials. The raw receipt is
unchanged. Future harness runs require matching clean build metadata and record
worker/harness hashes, with an explicit unverified override for exploration.

## Measured boundary and workload

Latency starts on the client monotonic clock immediately before HTTP submission
and ends when a query first observes the committed completed-job/result join.
It includes API acceptance commit, dispatch, delivery, result commit, and
observation overhead. Polling is approximately 50 ms plus SQL query, resource
sampling, and scheduling time; no fixed upper observation delay is guaranteed
under load. This is not the SQL `completed_at` update timestamp.

Each worker count starts with its own empty PostgreSQL schema, a separate API,
an outbox dispatcher, and 1, 2, 4, or 8 independent worker processes. There are
100 warmup jobs followed by three measured 1,000-job trials with the same
`public-durable-v1` seeded endpoints. Each resource sample requires every
expected PID exactly once and rejects missing or zombie/dead processes.

| Workers | Trial | Jobs/s | Observed p50 ms | Observed p95 ms | Observed p99 ms |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 323.21 | 63.369 | 99.888 | 109.641 |
| 1 | 2 | 307.62 | 81.865 | 124.829 | 133.465 |
| 1 | 3 | 295.23 | 103.089 | 151.079 | 166.296 |
| 2 | 1 | 295.13 | 65.549 | 101.771 | 109.992 |
| 2 | 2 | 291.03 | 81.856 | 125.334 | 146.482 |
| 2 | 3 | 262.73 | 76.807 | 146.456 | 171.072 |
| 4 | 1 | 290.83 | 64.115 | 103.612 | 114.644 |
| 4 | 2 | 269.85 | 61.840 | 99.259 | 116.617 |
| 4 | 3 | 294.83 | 57.412 | 95.428 | 103.088 |
| 8 | 1 | 295.91 | 64.398 | 101.864 | 111.422 |
| 8 | 2 | 300.43 | 84.503 | 128.069 | 136.787 |
| 8 | 3 | 271.69 | 58.366 | 96.425 | 104.036 |

[Raw observed latencies and continuous queue/resource samples](2026-10-08-durable-benchmark-v2.json)
retain all twelve trials. All 12,000 measured jobs completed; no normal-load
retry or duplicate occurred. Raw latencies follow deterministic fixture order.
The four recorded SQL schemas retain the additional 400 warmup results.

These are single-host measurements of a lightweight pure validation function.
The sequential HTTP producer and one confirmed publication at a time limit
load. This run does not demonstrate linear worker scaling or production/cluster
capacity. Resource samples cover API, dispatcher, and workers; PostgreSQL,
RabbitMQ, Docker VM, and unrelated processes are excluded. `ps` CPU percentages
are process-lifetime averages; approximately 50 ms sampling can miss peaks and
its overhead is part of the observed environment.

## Reliability proof

The full `go test -race -tags=integration -count=1 -v ./...` suite passed at
the same clean source. It includes all three real crash boundaries, live broker
restart, duplicate delivery, SQL rollback, idempotency/conflict/capacity,
validation retry exhaustion, actual dead queue, and graceful child-process exit.
The stalled-peer regression was verified failing before the fix, then passing
for both context timeout and cancellation during an AMQP handshake.

[The v2 broker recovery receipt](2026-10-08-recovery-v2.json) measures
**3.401676166 seconds** from Compose restart invocation through health wait and
the outage job's committed result observation. Final accounting: 2 accepted,
2 completed, 2 results, no pending/retrying/failed jobs, and one duplicate after
deliberate replay. This is one local recovery sample, not an SLO.

Reproduce with the [benchmark runbook](../docs/benchmark-runbook.md) and the
README's clean-clone service setup. The original
[pre-fix source snapshot](2026-10-08-durable-results.md) and its raw files remain
unchanged as provenance; its SQL timestamp intervals are not observed
end-to-end committed-result latency and should not be used for that claim.
