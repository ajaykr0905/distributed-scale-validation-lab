# Durable backend operating contract

The backend validates public synthetic endpoints. Its validation is a pure
fingerprint calculation, so repeating it before a transaction commits has no
external side effects.

```text
HTTP submission API -> PostgreSQL job + outbox transaction
                              |
                      outbox dispatcher
                              | publisher confirm
                           RabbitMQ
                              | manual delivery, prefetch 1
                     independent workers
                              | job row lock
                   PostgreSQL result + status transaction
                              | COMMIT
                          broker ACK
```

## Acceptance and idempotency

`POST /api/v1/jobs` accepts one `model.Endpoint` JSON object with an
`Idempotency-Key` of 1–128 printable ASCII characters without spaces. The server
canonicalizes the typed endpoint before hashing it; JSON field order does not
change identity. The job ID is SHA-256 of the key. The payload fingerprint
binds that key to its endpoint fields, including sequence.

The transaction stores the `pending` job and attempt-1 outbox notification.
HTTP 202 and a `Location` are emitted only after commit. A connection failure
can make the client uncertain about acceptance; resubmit with the same key.
Identical reuse returns HTTP 200, including the original attempt policy;
different payload reuse returns 409. Admission serializes only the pending
capacity check with a PostgreSQL advisory transaction lock. Full admission
returns 429 with `Retry-After: 1`. The default maximum unfinished job count is
100,000, configurable with `-capacity`.

`GET /api/v1/jobs/{id}` returns status, attempts, maximum attempts, and acceptance,
update, and completion timestamps. Unknown jobs return 404; SQL failures return
503 without exposing database internals. This is a single-tenant lab; keys are
globally scoped and there is no authentication.

## Publication and processing

The dispatcher selects one due outbox row with `FOR UPDATE SKIP LOCKED`, publishes
a persistent notification to durable RabbitMQ topology, waits for confirmation,
then records `sent_at` and commits. Multiple dispatchers can work concurrently.
A process death after publication but before commit rolls back `sent_at` and
republishes on restart. That ambiguity is expected and tested.

Workers receive only job ID and attempt, then load the canonical payload from
PostgreSQL under `FOR UPDATE`. A successful validation inserts the result and
sets `completed` in one transaction. The broker is acknowledged afterwards.
Terminal redelivery increments the durable duplicate count, without inserting
another result. Delayed old attempts are also acknowledged as duplicates.

Validation errors commit an incremented attempt, a bounded error summary, and
the next outbox event. A job retries at most `-max-attempts` times (default 3,
allowed 1–10 total validation attempts). Retry events become due after
`-retry-delay` (default one second). Exhaustion commits `failed` and a dead
notification. The dead queue carries job ID and final attempt; the durable job
contains the failure summary. Invalid or unknown wire notifications are rejected
into the broker dead queue; they do not alter accepted-job accounting.

A SQL or context failure rolls back the attempt and result. The worker requeues
and pauses 250 ms; infrastructure outages do not consume validation attempts.
There is no guarantee of progress while a dependency is unavailable. Dispatcher
and worker processes reconnect after broker loss, redeclaring topology. Their
10-second operation timeout bounds a stalled transaction. Connection startup
binds cancellation and a maximum five-second deadline to its socket through
TCP, AMQP handshake, and topology setup. The socket is closed on cancellation;
the deadline is cleared only after successful setup. Connection shutdown uses
a one-second close deadline instead of an unbounded channel-close RPC.
One-second reconnect
spacing prevents an unbounded busy loop.

## Accounting, shutdown, and limits

At each committed state:

```text
accepted = pending + retrying + completed + failed
durable results = completed
```

`GET /metrics` exposes these PostgreSQL counts, pending outbox events, retries,
and duplicates in Prometheus text format. They are gauges over the current lab
dataset, not durable Prometheus counters. Querying metrics cannot reconstruct
uncommitted work or broker in-flight deliveries. Worker prefetch=1, bounded
admission, one confirmed outbox publication at a time, and a bounded database
pool make the backpressure boundaries visible.

SIGTERM/Ctrl-C cancels workers and dispatchers; closing a consumer returns any
unacknowledged message to the queue. API shutdown drains in-flight HTTP requests
for up to ten seconds. Schema creation is additive and guarded by an advisory
migration lock. Accepted history and dead events are retained; automated retention,
multi-tenant authorization, HA/failover, and poison-job redrive are out of scope.

The result is exactly one durable row per accepted job under retries, not
exactly-once broker delivery. Scaling results are single-host lab measurements
with declared resource boundaries, not production or distributed-cluster claims.
