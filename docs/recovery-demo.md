# A 90-second recovery walkthrough

This is a recording script and reproducible evidence route, not a claim that
Ajay has already recorded a video. Show the commit and commands on screen.

1. **0–15 seconds:** Show the API -> outbox -> broker -> independent worker
   diagram and `POST /api/v1/jobs`. Explain that HTTP 202 means PostgreSQL
   acceptance plus its notification have committed.
2. **15–35 seconds:** Show the before-commit subprocess exit test. The job stays
   pending with attempts=0; redelivery produces one completed result.
3. **35–55 seconds:** Show after-commit-before-ack process death. The result is
   already durable; redelivery increments duplicates and preserves one result.
4. **55–75 seconds:** Show the dispatcher after-publish crash and broker-stop
   tests. Outbox restart duplicates safely; an accepted job remains pending during
   the broker outage and completes after reconnection.
5. **75–90 seconds:** Show accounting and measured recovery time. State the local
   single-host limits and link the exact commit, raw benchmark, and test command.

Use only the dedicated public lab services:

```sh
docker compose -p ajay-durable-demo -f compose.durable.yaml up -d --wait
export SCALE_LAB_INTEGRATION=1
export SCALE_LAB_DOCKER_PROJECT=ajay-durable-demo
export SCALE_LAB_POSTGRES_URL='postgres://scale_lab:local-development-only@127.0.0.1:25432/scale_lab?sslmode=disable'
export SCALE_LAB_RABBITMQ_URL='amqp://scale_lab:local-development-only@127.0.0.1:25672/'
go test -race -tags=integration -count=1 -v ./internal/durable
docker compose -p ajay-durable-demo -f compose.durable.yaml down
```

The broker restart test deliberately stops only the named `ajay-durable-*`
Compose project's RabbitMQ service. It compiles and launches independent Go
worker/dispatcher processes, accepts a job while the broker is down, restarts
the broker, checks accounting and duplication, and requires graceful process
exit. Process-death tests use actual helper subprocesses exiting at transaction
boundaries. Test schemas and names are isolated synthetic fixtures.
