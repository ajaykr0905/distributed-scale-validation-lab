# HTTP contracts

These contracts cover the in-memory control API and durable submission API.
Tests use in-process handlers and fake repositories; external PostgreSQL and
RabbitMQ integration tests remain opt-in.

## Run requests

`POST /api/v1/runs` accepts exactly one JSON run options object. Unknown fields,
malformed JSON, additional values, and trailing non-whitespace return 400 before
a run starts or run counters change. Trailing JSON whitespace is allowed.
Top-level `null` and other non-object values return 400. An empty object retains
the existing defaults: 10,000 endpoints, four workers, three attempts, and the
`public-demo` seed.

## Request size

Both POST endpoints limit body reads to 16 KiB, including trailing whitespace.
A detected size overflow returns 413, whether it occurs in the JSON value or
while checking for a second value. Exactly 16 KiB is permitted. This also applies
to streamed bodies without a known content length. Syntax errors found before
the size limit still return 400; the server does not drain malformed input just
to classify its total length.

## Body representation

An omitted `Content-Type` remains supported for existing clients, including the
benchmark. When supplied, it must be a single valid `application/json` media
type, with absent or UTF-8 charset. Multiple values, malformed types, other media
types, and other charsets return 415. Media type and charset casing is ignored.
Bodies must be uncompressed; absent, empty, or `identity` content encoding is
supported. Other or multiple content encodings return 415 before execution or
database admission.

## Idempotency header

Job submission requires exactly one `Idempotency-Key` header value. Missing or
repeated headers return 400, even if repeated values match. The existing key
validation still requires 1–128 printable ASCII characters without spaces.
A comma in a single value remains a literal character in that key; the server
does not split or normalize keys before validation and submission.

## Run cancellation

A run request with an already canceled context returns 408; an expired deadline
returns 504. Neither starts an experiment nor changes run/failure counters.
Cancellation is checked again after body parsing. Cancellation or deadline
errors during execution, including wrapped errors, use the same status codes
and public messages; those attempted executions count as failed runs. Existing
run option validation errors continue to return 422.

## Repository cancellation

Durable endpoints also reject already canceled requests with 408 and expired
deadlines with 504 before accessing the repository. Submission rechecks after
body parsing. Repository errors wrapping cancellation or deadline errors use
the same codes without exposing backend details. Other repository failures
remain 503. A timeout or disconnect can leave commit outcome uncertain; retry a
submission with its original key. A successful repository result is retained
even if cancellation races with its return.

## Public admission errors

Wrapped or joined idempotency conflicts and capacity errors return only the
public sentinel message, with 409 and 429 respectively. Internal wrapper text
is never copied to the response. Capacity responses retain `Retry-After: 1`;
failed submissions do not advertise a job `Location`.

## Freshness and HEAD

Both APIs send `Cache-Control: no-store` on every response, including errors,
unknown routes, and method rejections. Job state, readiness, and metrics must
not be served from a stale intermediary cache; a missing job's 404 must not
become a negatively cached polling result. GET routes also support HEAD through
Go's HTTP server: the response retains its status, JSON or Prometheus content
type, and cache policy, and sends no body. Metrics use
`text/plain; version=0.0.4`; JSON responses use `application/json`.

## Run admission

Each control handler admits at most two concurrent experiments. Additional valid
requests fail immediately with 429 and `Retry-After: 1`, without an unbounded
waiting queue or changes to run/failure/result counters. Body validation and
context checks happen before admission. Slots are released on success, error,
or cancellation. Health and metrics probes remain available while runs are
busy. Admission is per handler instance, not a cross-process quota.

## Repository operation budget

Each durable Submit, Get, and Counts operation receives a context with at most
five seconds to complete. This bounds database pool and query waits independently
of HTTP socket read/write timeouts. A shorter caller deadline and request
cancellation still take precedence, and request context values are preserved.
The derived context is canceled when the handler returns to release timer
resources. Repository implementations must honor context cancellation, as the
PostgreSQL implementation does through its context-aware SQL calls. Deadline
errors return 504 under the repository cancellation contract above.
