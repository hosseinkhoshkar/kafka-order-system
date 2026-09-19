# ADR 0002: Order State Policy

## Decision

Orders are created as `PENDING` and can move to `CONFIRMED` or `CANCELLED` from
inventory replies. Matching duplicate final replies are accepted as already
final; conflicting final replies fail.

## Context

Kafka can redeliver replies or deliver invalid/conflicting messages. The order
service must avoid changing a final business result after the first valid final
decision.

## Consequences

The reply consumer locks the order row, validates product and quantity against
the order, and writes the final status in the same transaction as the order
Inbox/event history. A reply that does not match the order or conflicts with an
existing final state is treated as a consumer error and can reach DLT.
