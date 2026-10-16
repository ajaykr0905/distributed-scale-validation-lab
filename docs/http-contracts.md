# HTTP contracts

These contracts cover the in-memory control API and durable submission API.
Tests use in-process handlers and fake repositories; external PostgreSQL and
RabbitMQ integration tests remain opt-in.

## Run requests

`POST /api/v1/runs` accepts exactly one JSON run options value. Unknown fields,
malformed JSON, additional values, and trailing non-whitespace return 400 before
a run starts or run counters change. Trailing JSON whitespace is allowed.
