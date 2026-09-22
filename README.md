# Distributed Scale Validation Lab

A clean-room Go lab for exploring deterministic workload generation,
at-least-once queue processing, idempotent writes, retries, and failure testing.
It is designed as a public portfolio project and uses synthetic entities only.

## What it demonstrates

- Deterministic endpoint generation from a seed
- A transport-neutral queue contract with ack and nack semantics
- Concurrent workers and idempotency by stable message ID
- Bounded retries with an inspectable dead-letter sink
- A PostgreSQL adapter backed by `database/sql` and pgx
- A RabbitMQ adapter with durable topology, persistent messages, publisher confirms, and a dead-letter exchange
- A small REST control API with health and Prometheus-format metrics endpoints
- Signal-aware API shutdown that drains in-flight HTTP requests within a bounded window
- Injected validation and persistence failures in dependency-free unit tests
- Optional local RabbitMQ and PostgreSQL infrastructure
- A Kubernetes Job with a restricted container security context

## Run locally

Requirements: Go 1.23 or newer. RabbitMQ and PostgreSQL are optional for the
default in-memory workflow; their Go client modules are downloaded normally.

```sh
go test ./...
go run ./cmd/lab -seed public-demo -count 10000 -workers 4
go run ./cmd/api
```

The command prints a JSON summary. A successful run has equal `generated` and
`stored` counts.

The API exposes `POST /api/v1/runs`, `GET /healthz`, and `GET /metrics`. The
request is bounded to 100,000 synthetic entities and 128 workers so a public
review cannot accidentally create an unbounded local workload.

## Optional infrastructure

Start development-only RabbitMQ and PostgreSQL containers:

```sh
docker compose --profile infra up -d
docker compose --profile infra down
```

The executable uses dependency-free memory adapters by default. The RabbitMQ
and PostgreSQL adapters are exercised by an opt-in integration test against the
dedicated Compose profile:

```sh
docker compose --profile infra up -d
SCALE_LAB_INTEGRATION=1 go test -tags=integration ./integration
docker compose --profile infra down
```

The integration path verifies a confirmed publish, manual acknowledgement, and
an idempotent PostgreSQL write. No external-service performance result is
claimed until its environment and artifact are recorded.

The unit suite runs with Go's race detector on every pull request. CI also
starts the dedicated Compose services and executes the external-adapter test.

## Kubernetes

Build and publish an image, replace the placeholder account in
`deployments/kubernetes/job.yaml`, then apply:

```sh
kubectl apply -f deployments/kubernetes/namespace.yaml
kubectl apply -f deployments/kubernetes/job.yaml
```

## Design and verification

- [Architecture and delivery guarantees](docs/architecture.md)
- [Reproducible benchmark runbook](docs/benchmark-runbook.md)
- [PostgreSQL schema](migrations/001_validation_results.sql)

## Clean-room statement

This repository is original demonstration code. It does not contain or derive
from Cisco, Meraki, customer, or other employer code, schemas, identifiers,
configuration, metrics, logs, datasets, or unpublished benchmark results.

## License

MIT
