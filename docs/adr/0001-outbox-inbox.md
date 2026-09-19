# ADR 0001: Outbox and Inbox

## Decision

Use database Outbox tables for Kafka publication and Inbox tables for consumer
deduplication in both services.

## Context

The services update PostgreSQL and communicate through Kafka. A direct
database write followed by a Kafka send is a dual-write risk: the database commit
can succeed while the Kafka send fails, or Kafka can receive a message while the
database update is rolled back.

## Consequences

Order creation and inventory decisions commit their business row, audit event
and Outbox row atomically. Scheduled relays publish later with retry, claim
tokens and leases. Consumers record processed `eventId` values in Inbox tables
so repeated Kafka deliveries do not repeat business effects.

This gives at-least-once delivery with idempotent effects, not exactly-once
delivery. Operators still need to inspect and resolve exhausted Outbox `FAILED`
rows.
