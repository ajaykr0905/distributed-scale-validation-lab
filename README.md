# Distributed Scale Validation Lab

A clean-room Go lab for exploring deterministic workload generation,
at-least-once queue processing, idempotent writes, retries, and failure testing.
It is designed as a public portfolio project and uses synthetic entities only.

## Durable asynchronous backend

The flagship path now runs **separate submission API, outbox dispatcher, and
worker processes** against PostgreSQL and RabbitMQ. PostgreSQL owns acceptance,
attempts, terminal status, and one result per job. Broker delivery is at least
once: a committed result survives worker death before acknowledgement.

- `POST /api/v1/jobs` atomically accepts a synthetic endpoint and its outbox event.
- `Idempotency-Key` returns the original job on identical retries and rejects
  conflicting payloads with HTTP 409.
- `GET /api/v1/jobs/{id}` exposes pending, retrying, completed, or failed state.
- Bounded admission returns HTTP 429; each worker prefetches one delivery.
- Validation retries and dead-letter notification are durable outbox transitions.
- Database failures roll back and requeue without spending the validation budget.
- Worker/dispatcher death and broker restart are exercised against real services.

Start from a clean clone:

```sh
go test -race ./...
go build -o bin/durable ./cmd/durable
docker compose -p ajay-durable-local -f compose.durable.yaml up -d --wait
export SCALE_LAB_POSTGRES_URL='postgres://scale_lab:local-development-only@127.0.0.1:25432/scale_lab?sslmode=disable'
export SCALE_LAB_RABBITMQ_URL='amqp://scale_lab:local-development-only@127.0.0.1:25672/'
```

Run these in three terminals with the same exported environment:

```sh
bin/durable -role api
bin/durable -role dispatcher
bin/durable -role worker
```

Submit a public synthetic job, then use the returned `Location` to inspect it:

```sh
curl -i http://127.0.0.1:8081/api/v1/jobs \
  -H 'Idempotency-Key: public-example-1' \
  -H 'Content-Type: application/json' \
  -d '{"id":"synthetic-example","tenant_id":"synthetic","region":"ap-south","platform":"linux","expected_version":"v1","sequence":0}'
curl http://127.0.0.1:8081/metrics
```

Stop each process with Ctrl-C and stop only this isolated project with
`docker compose -p ajay-durable-local -f compose.durable.yaml down`.
Its named volumes preserve state. Development credentials and localhost ports
are public fixtures; this API has no authentication and is not an Internet
deployment. See [the durable operating contract](docs/durable-backend.md),
[failure walkthrough](docs/recovery-demo.md), and
[external-path measurement runbook](docs/benchmark-runbook.md).

## Original in-memory lab

The original `cmd/lab`, `cmd/api`, and `POST /api/v1/runs` remain compatible.
The 10,000-job run below is an **in-memory functional run**, not an external
RabbitMQ/PostgreSQL throughput claim.

## What the original lab demonstrates

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
