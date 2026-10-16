# Worker contracts

Memory queue capacity bounds new publications. Requeued deliveries retain their
own pending slot and cannot block behind new publishers. Retrying preserves the
message ID and payload, increments attempts once, and permits only one successful
settlement per delivery. Pending retries are received before buffered new work.

Already canceled memory queue operations neither publish nor consume ready work.
Memory stores honor cancellation before insertion and duplicate detection, matching
the context contract of the PostgreSQL adapter. Concurrent insertion of the same
message ID has exactly one winner.

RabbitMQ deliveries serialize settlement and reject duplicate settlement. Failed
broker commands leave settlement retryable. Once a retry publication is confirmed,
an acknowledgment retry does not republish it. Retry publication has a ten-second
deadline. Transport failures can still produce duplicates across reconnects, so
stores must remain idempotent by message ID.

Attempt counters are nonnegative signed 32-bit values across adapters. Invalid
RabbitMQ retry headers count as exhausted rather than resetting the retry budget.
Requeue refuses counter overflow without settling the original; dead-letter
accounting saturates at the maximum. Worker budget comparisons do not add to
untrusted counters.

A worker discards an exhausted original only after a configured dead-letter sink
accepts its copy. Transfer failure requeues the original and returns both errors.
Cancellation also requeues rather than dead-lettering a job. A failed acknowledgment
after persistence leaves the stored result available to deduplicate redelivery.

Queue payloads contain exactly one endpoint JSON object with known fields. Message
IDs and endpoint identity, tenant, region, platform, and version must be nonblank;
sequence must be nonnegative. Poison input is dead-lettered without validation or
storage. Exhausted redeliveries attempt dead-letter transfer without validating
again. The basic validator enforces the same endpoint requirements for direct callers.

Successful validator output must name the input endpoint and include nonblank
status and fingerprint plus a nonzero checked time. Both worker paths treat
malformed output as validation failure and apply their normal bounded retry/dead
letter policy. They cannot persist or acknowledge it as successful work.

Dataset generation is bounded to 100,000 endpoints and accepts a context. Existing
Generate callers retain deterministic output. Canceled generation returns no
partial dataset. Runner cancellation covers generation, publication, and worker
completion; canceled runs return an error and no success summary.

Durable broker publications and processing require a lowercase 64-digit hex job
ID and an attempt in 1..10. Events beyond a job's budget or beyond its terminal
attempt are invalid, not duplicates. Stale and terminal redeliveries of actual
attempts increment duplicate accounting without invoking the validator. A broker
opened without consumption rejects Receive immediately.

Durable processing rejects negative retry delay before opening a transaction.
Positive delays round up to milliseconds without overflowing, so small positive
delays cannot silently become immediate retries. Terminal failure notifications
are available immediately rather than waiting for the next-retry delay.
