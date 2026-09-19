# ADR 0003: HTTP Idempotency and DLT Replay

## Decision

`POST /api/orders` supports an optional `Idempotency-Key`. The order service
stores a request fingerprint and the completed HTTP response. Reusing the key
with the same request replays the stored response; reusing it with different
content returns `409 Conflict`.

DLT replay is a separate operator tool. It replays one selected DLT record only
when `--execute` is supplied.

## Context

HTTP retries and Kafka redelivery solve different problems. HTTP clients need a
stable response for request retries. Kafka consumers need message deduplication
by event identity. DLT replay must be explicit because replay can create another
Kafka delivery.

## Consequences

HTTP idempotency prevents duplicate order creation for client retries. Inbox
tables and domain rules prevent duplicate Kafka side effects. Replay remains
manual and at-least-once; the tool does not delete DLT records or guarantee
exactly-once recovery.
