# Kafka Order System

Java 17 / Spring Boot demo of an asynchronous order workflow with Kafka and PostgreSQL.
Inventory is currently simulated: quantities above 10 are rejected; all other quantities
are accepted. No real stock is reserved. This baseline does not claim production readiness.

## Modules and flow

- `common`: shared JSON event envelopes and payloads.
- `order-service`: HTTP API, order database, event history, scheduled outbox relay and reply consumer.
- `inventory-service`: order consumer, simulated inventory decision, event history and reply producer.
- `integration-tests`: test-only module; launches both packaged applications with isolated infrastructure.

```text
POST /api/orders -> orderdb (order + history + outbox)
  -> scheduled relay -> orders topic -> inventory-service
  -> inventorydb (history) -> inventory-reply topic
  -> order-service -> orderdb (CONFIRMED or CANCELLED + history)
```

The HTTP response is `201 Created` with `PENDING` and a `Location: /api/orders/{orderId}`
header. The order resource is persisted before responding; inventory confirmation remains
asynchronous. Poll `GET /api/orders/{orderId}` for the final status.

## Prerequisites

- JDK 17 (`JAVA_HOME` pointing to the JDK).
- Docker Engine, or Docker Desktop running Linux containers.
- Internet access for Maven dependencies and container images on the first run.
- Run commands from the repository root. Maven installation is not required.

## Automated verification

Unit tests and test compilation (Docker not required):

```powershell
.\order-service\mvnw.cmd -B -ntp -f pom.xml test
```

Full build, unit tests and integration tests (Docker required):

```powershell
docker info
.\order-service\mvnw.cmd -B -ntp -f pom.xml clean verify
```

Linux/macOS equivalents:

```sh
bash order-service/mvnw -B -ntp -f pom.xml test
bash order-service/mvnw -B -ntp -f pom.xml clean verify
```

`verify` uses Testcontainers 1.21.4 (pinned for compatibility with recent Docker Engines)
to create one Kafka broker and two PostgreSQL databases on
random host ports. It does not use the databases or Kafka from `docker-compose.yml`.
Both executable service jars run as separate Java processes with random HTTP ports.
Processes and test containers are stopped after the suite. Docker absence fails the suite;
integration tests are not silently skipped. Always run the full root reactor so service jars
are packaged before the test module runs.

The integration suite checks:

- Both real Spring applications start and the Order health endpoint responds.
- Quantity `2`: HTTP `PENDING` eventually becomes database `CONFIRMED`.
- Quantity `11`: HTTP `PENDING` eventually becomes database `CANCELLED`.
- Outbox reaches `SENT`; Order creation/final history and Inventory result are persisted.
- The reply preserves the order correlation ID and product ID.
- GET-by-ID returns the final status and preserves the exact decimal price in PostgreSQL.

Order MVC tests also cover invalid inputs, malformed JSON, fractional quantities, 404,
error response privacy, the Location header and exact decimal deserialization.

These tests exercise the baseline happy path and business rejection, not crash recovery,
duplicate delivery or concurrent stock reservation. Invalid input is covered by MVC tests.

Reports: each module's `target/surefire-reports`, plus
`integration-tests/target/failsafe-reports`. Application logs:
`integration-tests/target/application-logs`. CI runs `clean verify` on Java 17 and uploads
these files even when tests fail.

## Run the local demo

Start infrastructure; wait for the readiness checks below before starting applications:

```sh
docker compose up -d
docker compose exec -T postgres-order pg_isready -U admin -d orderdb
docker compose exec -T postgres-inventory pg_isready -U admin -d inventorydb
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list
```

If a check fails during startup, inspect `docker compose logs <service>` and retry the check.
For predictable demo topic creation, run once the broker is ready:

```sh
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic orders --partitions 1 --replication-factor 1
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic inventory-reply --partitions 1 --replication-factor 1
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic orders.DLT --partitions 1 --replication-factor 1
```

Build the jars with unit tests (the full verification command above also builds them):

```powershell
.\order-service\mvnw.cmd -B -ntp -f pom.xml package
```

Start each application in a separate terminal:

```sh
java -jar inventory-service/target/inventory-service-0.0.1-SNAPSHOT.jar
java -jar order-service/target/order-service-0.0.1-SNAPSHOT.jar
```

Services: Order `8081`, Inventory `8082`, Kafka `9092`, Kafka UI `8080`,
Order PostgreSQL `5432`, Inventory PostgreSQL `5433`, pgAdmin `5050`.
The Compose credentials are local-demo defaults. Do not expose this setup publicly.

Create a successful order in PowerShell:

```powershell
$body = @{ productId='product-1'; customerId='customer-1'; quantity=2; price=12.50 } | ConvertTo-Json
$order = Invoke-RestMethod -Method Post -Uri 'http://localhost:8081/api/orders' -ContentType 'application/json' -Body $body
$order
```

Repeat with `quantity=11` to exercise rejection. Bash equivalent:

```sh
curl -H 'Content-Type: application/json' -d '{"productId":"product-1","customerId":"customer-1","quantity":2,"price":12.50}' http://localhost:8081/api/orders
```

Poll the order in PowerShell (using the response from the POST example):

```powershell
Invoke-RestMethod -Uri "http://localhost:8081/api/orders/$($order.orderId)"
```

Allow at least one relay cycle (five seconds). You can also inspect database history:

```sh
docker compose exec -T postgres-order psql -U admin -d orderdb -c "select order_id, quantity, status from orders order by created_at desc limit 10;"
docker compose exec -T postgres-order psql -U admin -d orderdb -c "select aggregate_id, status from outbox_events order by created_at desc limit 10;"
docker compose exec -T postgres-inventory psql -U admin -d inventorydb -c "select aggregate_id, event_type from event_store order by occurred_at desc limit 10;"
```

Stop the Java processes with Ctrl+C, then run `docker compose down`. This preserves the
PostgreSQL volumes. Kafka has no persistent volume in the current Compose configuration.
Integration tests always start with fresh, separate databases; no local volume deletion is needed.

## Current limitations

Real inventory, idempotency, reliable reply publication,
outbox retry/recovery and Saga timeouts remain future work. `event_store` is audit history,
not a full event-sourced system. Hibernate currently manages schema with `ddl-auto:update`.
The two existing application-class unit tests are not context-startup tests; actual startup
is covered by the packaged-application integration suite.

## Order API (stage 2)

- `POST /api/orders`: `201 Created`, `Location` header and an order DTO.
- `GET /api/orders/{orderId}`: `200` with the persisted status; `404` if absent.
- `GET /api/orders/health`: unchanged basic liveness text.
- Errors use `application/problem+json` with `type`, `title`, `status`, `detail` and `instance`.
  Validation errors additionally contain an `errors` map keyed by field name.

Both IDs must be nonblank and at most 255 characters. Quantity must be a positive integer
within Java's Integer range; fractional quantities are rejected, not truncated.
`price` is the positive **unit price in EUR**, with at most 17 integer digits and 2 fractional
digits. This demo uses one implicit currency; the response includes `currency: "EUR"`.
No currency conversion is performed, and total price is not calculated by this endpoint.
Prices travel as JSON numbers and BigDecimal through request, entity, response and the
existing OrderCreatedEvent. Clients should use decimal-aware JSON handling for money.

Compatibility changes: POST previously returned 200 and now returns 201; invalid inputs
previously accepted may now return 400. The response retains existing fields and adds
currency and updatedAt. The Kafka envelope and event field structure remain unchanged.
Confirm the EUR assumption for any pre-existing data; earlier versions did not define currency.

### Existing database caveat

The JPA price column is now `numeric(19,2)` instead of floating point. A fresh test database
uses this mapping. Do not rely on `ddl-auto:update` to safely convert valuable existing data:
back it up and inspect range, extra decimal places and prior floating-point rounding first.
Versioned migrations and an audited conversion are stage 3; this patch does not run any SQL
against your database. Existing values cannot regain precision already lost as doubles.
