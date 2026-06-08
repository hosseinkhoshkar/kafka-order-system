# AGENTS.md

Persistent instructions for future work in this repository.

## Project Stack

- Java 17.
- Spring Boot 3.2.5 with Spring Web, Spring Data JPA, Spring Kafka, Lombok, and PostgreSQL.
- Maven multi-module reactor:
  - `common`: shared Kafka event contracts.
  - `order-service`: order API, order persistence, event store, outbox relay, inventory reply consumer.
  - `inventory-service`: order event consumer, inventory decision logic, event store, inventory reply producer.
- Kafka uses Spring Kafka with JSON serialization/deserialization. There is no Avro/schema registry currently.
- Shared event contracts live under `common/src/main/java/com/example/common/event`.
- Docker Compose provides Zookeeper, Kafka, Kafka UI, two PostgreSQL databases, and pgAdmin.
- Databases are PostgreSQL: `orderdb` on local port `5432`, `inventorydb` on local port `5433`.

## Architecture Rules

- Preserve the existing multi-module Spring Boot architecture unless explicitly asked to redesign it.
- Keep service-specific persistence and infrastructure code inside each service module.
- Put shared Kafka/event contract classes in `common`; do not duplicate event DTOs across services.
- Keep domain/business logic separate from Kafka, HTTP, and database plumbing where practical.
- Prefer constructor injection. Lombok `@RequiredArgsConstructor` is already used and acceptable.
- Avoid unnecessary abstractions and broad refactors.
- Keep changes focused, minimal, and consistent with existing package and naming conventions.
- Do not publish JPA entities as Kafka event payloads; use explicit event DTOs/envelopes.

## Kafka And Event-Driven Rules

- Respect the existing `EventEnvelope` and event type constants in `common`.
- Maintain backward compatibility when changing event contracts; explain the impact before changing public event fields or meanings.
- Treat Kafka delivery as at-least-once: consider duplicate events and idempotency in consumers.
- Handle retries and failures explicitly. Avoid unsafe automatic retries that can repeat side effects.
- Do not silently swallow Kafka publish, consume, deserialization, or serialization errors.
- Consider consumer crash/restart scenarios, poison messages, DLT behavior, and recovery paths.
- Use configured topic names from application config rather than hardcoded topic strings, except where an existing DLT convention is intentionally used.
- Keep Kafka record keys stable and meaningful, typically `orderId`/aggregate id.

## Coding Standards

- Write production-quality Java with clear names and straightforward control flow.
- Prefer readable code over clever code.
- Prefer immutable DTOs/records for shared event contracts where practical.
- Use `BigDecimal` for new money-related contract fields when possible; avoid adding more `Double` money fields.
- Do not introduce new dependencies unless they provide clear value and fit the project.
- Reuse existing services, repositories, DTOs, event types, and patterns before creating new ones.
- Keep comments concise and useful. Avoid preserving or adding corrupted/garbled text.

## Testing Rules

- Add or update tests when behavior changes.
- Unit-test business logic and event translation logic.
- Use integration tests when Kafka, database, transaction, outbox, or recovery behavior matters.
- Run relevant Maven tests after making changes. Root reactor tests can be run with:
  - `.\order-service\mvnw.cmd -f pom.xml test`
- Do not delete or weaken existing tests just to make the build pass.
- Avoid tests that require live local PostgreSQL or Kafka unless they are explicit integration tests.

## Working Workflow

For non-trivial tasks:

1. Analyze the relevant code first.
2. Explain the intended approach.
3. Identify files likely to change.
4. Make the smallest reasonable implementation.
5. Run relevant tests.
6. Review the diff.
7. Summarize what changed and why.

## Safety Rules

- Do not modify unrelated files.
- Do not perform large refactors unless explicitly requested.
- Do not change public APIs or Kafka event contracts without explaining the impact first.
- Do not change Docker Compose, ports, database names, credentials, or infrastructure configuration unnecessarily.
- Do not commit, push, create branches, or rewrite git history unless explicitly requested.
- The working tree may contain user or previous-agent changes; preserve them and work around them.
