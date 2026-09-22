# Distributed Scale Validation Lab

A clean-room Go lab for exploring deterministic workload generation,
at-least-once queue processing, idempotent writes, retries, and failure testing.
It is designed as a public portfolio project and uses synthetic entities only.

## What it demonstrates

- Deterministic endpoint generation from a seed
- A transport-neutral queue contract with ack and nack semantics
- Concurrent workers and idempotency by stable message ID
- Bounded retries with an inspectable dead-letter sink
- A PostgreSQL-compatible store implemented with `database/sql`
- A small REST control API with health and Prometheus-format metrics endpoints
- Injected validation and persistence failures in dependency-free unit tests
- Optional local RabbitMQ and PostgreSQL infrastructure
- A Kubernetes Job with a restricted container security context

## Run locally

Requirements: Go 1.22 or newer. No service or third-party Go module is required.

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

The executable uses dependency-free memory adapters by default. The containers
are provided for developing integration adapters; this repository does not claim
that an AMQP adapter or an external-database benchmark is already implemented.

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
