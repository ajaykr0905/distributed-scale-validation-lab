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
