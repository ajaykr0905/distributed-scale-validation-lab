# Worker contracts

Memory queue capacity bounds new publications. Requeued deliveries retain their
own pending slot and cannot block behind new publishers. Retrying preserves the
message ID and payload, increments attempts once, and permits only one successful
settlement per delivery. Pending retries are received before buffered new work.

Already canceled memory queue operations neither publish nor consume ready work.
Memory stores honor cancellation before insertion and duplicate detection, matching
the context contract of the PostgreSQL adapter. Concurrent insertion of the same
message ID has exactly one winner.
