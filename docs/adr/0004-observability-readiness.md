# ADR 0004: Observability and Readiness

## Decision

Use Spring Boot Actuator, Micrometer and log correlation instead of adding a
tracing backend. Expose only `health`, `metrics` and `prometheus` endpoints.

`order-service` readiness requires its database, not Kafka. `inventory-service`
readiness requires its database and Kafka.

## Context

The order API can persist accepted work to its Outbox while Kafka is temporarily
unavailable. Inventory processing, however, depends on Kafka consumption as its
main role.

## Consequences

Liveness is limited to the application state. Readiness reflects whether each
service can perform its role. Metrics show HTTP behavior, Outbox backlog,
reservation results, idempotency replay/conflict, duplicate Inbox events and DLT
activity. Metrics are process-local observations and reset on restart.
