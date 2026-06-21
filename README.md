# Kafka Order System

Java 17 / Spring Boot demo of an asynchronous order workflow with Kafka and PostgreSQL.
Inventory is reserved from persisted stock in `inventorydb`. Kafka delivery is treated as
at-least-once: duplicate order events are ignored by inventory Inbox and per-order decision
records, and inventory replies are published through a recoverable Inventory Outbox. This
baseline does not claim production readiness or exactly-once delivery.

## Modules and flow

- `common`: shared JSON event envelopes and payloads.
- `order-service`: HTTP API, order database, event history, scheduled outbox relay and reply consumer.
- `inventory-service`: order consumer, persisted inventory reservation, Inbox, decision log,
  event history and scheduled reply outbox relay.
- `integration-tests`: test-only module; launches both packaged applications with isolated infrastructure.

```text
POST /api/orders -> orderdb (order + history + outbox)
  -> scheduled relay -> orders topic -> inventory-service
  -> inventorydb (Inbox + decision + stock + history + reply outbox)
  -> scheduled relay -> inventory-reply topic
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
- Quantity `2` with enough seeded stock: HTTP `PENDING` eventually becomes database `CONFIRMED`.
- Quantity `11` with stock `20`: confirms, proving the old `quantity > 10` rule is gone.
- Quantity above available stock: HTTP `PENDING` eventually becomes database `CANCELLED`.
- Outbox reaches `SENT`; Order creation/final history and Inventory result are persisted.
- The inventory reply preserves the order correlation ID and product ID.
- GET-by-ID returns the final status and preserves the exact decimal price in PostgreSQL.

Order MVC tests also cover invalid inputs, malformed JSON, fractional quantities, 404,
error response privacy, the Location header and exact decimal deserialization.

Inventory service tests also exercise duplicate delivery, concurrent duplicate delivery,
same-order redelivery, same-order conflict handling, failed-decision stability, concurrent
stock reservation without overselling, transaction rollback, outbox retry and expired-claim
recovery against PostgreSQL/Testcontainers. Invalid input is covered by MVC tests.

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

Seed demo inventory explicitly before creating orders. Production startup does not insert
stock automatically:

```sh
docker compose exec -T postgres-inventory psql -U admin -d inventorydb -c "insert into inventory(product_id, available_quantity, reserved_quantity) values ('product-1', 20, 0) on conflict (product_id) do update set available_quantity = excluded.available_quantity, reserved_quantity = excluded.reserved_quantity;"
```

Create a successful order in PowerShell:

```powershell
$body = @{ productId='product-1'; customerId='customer-1'; quantity=2; price=12.50 } | ConvertTo-Json
$order = Invoke-RestMethod -Method Post -Uri 'http://localhost:8081/api/orders' -ContentType 'application/json' -Body $body
$order
```

Repeat with `quantity=11`; with the stock seed above it succeeds. Use a quantity above
available stock, or an unseeded product, to exercise business rejection. Bash equivalent:

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
docker compose exec -T postgres-inventory psql -U admin -d inventorydb -c "select product_id, available_quantity, reserved_quantity from inventory order by product_id;"
docker compose exec -T postgres-inventory psql -U admin -d inventorydb -c "select order_id, product_id, quantity, status from inventory_reservation_decisions order by decided_at desc limit 10;"
docker compose exec -T postgres-inventory psql -U admin -d inventorydb -c "select aggregate_id, event_type, status from inventory_outbox_events order by created_at desc limit 10;"
```

Stop the Java processes with Ctrl+C, then run `docker compose down`. This preserves the
PostgreSQL volumes. Kafka has no persistent volume in the current Compose configuration.
Integration tests always start with fresh, separate databases; no local volume deletion is needed.

## Current limitations

Inventory reservation is real and guarded by an atomic conditional PostgreSQL update.
Inventory Inbox deduplicates by `eventId`; a duplicate `eventId` with different content is
treated as an invalid message and sent through the configured Kafka error handler/DLT path.
Per-order decisions prevent re-deciding the same order: a new event with the same `orderId`,
`productId` and `quantity` keeps the existing decision, while a different product or
quantity is treated as a conflict and no ordinary inventory reply is emitted.

Inventory replies are stored in `inventory_outbox_events` before publication. The relay uses
bounded batches, claim tokens, leases, exponential backoff and max attempts. Kafka key is the
`orderId`; retry keeps the same reply `eventId`, payload and correlation ID. Expired
`IN_PROGRESS` claims become claimable after the lease. `FAILED` outbox rows require an
operator decision: inspect `last_error` and the stable `payload`; after correcting the cause,
set `status='PENDING'`, clear claim fields, reset or adjust `attempt_count`, and set
`next_attempt_at=now()` in a controlled maintenance window. No automatic replay API is
provided in this stage.

The `order-service` reply consumer is still intentionally limited: it does not yet have its
own Inbox or strict state machine, so duplicate inventory replies can append duplicate order
history and reapply the same terminal status. That hardening belongs to the next stage.
Saga timeouts also remain future work. `event_store` is audit history, not a full
event-sourced system. Flyway manages schema and Hibernate validates it.

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

## Database schema and migrations

Both services use Flyway migrations from their own module:

- `order-service/src/main/resources/db/migration`
- `inventory-service/src/main/resources/db/migration`

Normal application startup runs pending migrations first, then Hibernate validates the
schema with `spring.jpa.hibernate.ddl-auto=validate`. A brand-new empty database is created
from `V1__initial_schema.sql`; follow-up compatibility checks live in later migrations.
Do not edit an applied migration. Add a new versioned migration instead.

The migration index choices are intentionally narrow:

- `outbox_events(status)`: supports the current relay query for pending outbox rows.
- `event_store(aggregate_id, version)`: supports aggregate history reads ordered by version.
- `inventory_inbox_events(event_id)`: prevents duplicate event processing.
- `inventory_reservation_decisions(order_id)`: prevents re-deciding an order.
- `inventory_outbox_events(status, next_attempt_at, created_at, id)` and
  `(status, claimed_until, created_at, id)`: support relay claims and expired lease recovery.

No unique constraint is placed on event version yet because the current version allocation is
not concurrency-safe. `sent_at` remains nullable because pending and failed outbox rows have
not necessarily been sent.

### Empty local databases

For a fresh local demo database, start Compose and then start both applications normally.
Flyway will create the schema; Hibernate will validate it. If startup fails at validation,
inspect the Flyway error before changing application code.

### Existing database adoption

`baseline-on-migrate` is deliberately not enabled in application configuration. To adopt an
existing database that was previously managed by Hibernate `ddl-auto:update`, perform this
manually and only after taking a backup.

Minimum pre-checks for `orderdb`:

```sql
select count(*) from orders where
  order_id is null or product_id is null or customer_id is null
  or quantity is null or price is null or status is null
  or created_at is null or updated_at is null;

select count(*) from orders where quantity <= 0;
select count(*) from orders where price <= 0;
select count(*) from orders where price > 99999999999999999.99;
select count(*) from orders where abs(price::numeric - round(price::numeric, 2)) > 0.0000001;
select status, count(*) from orders group by status;
```

Minimum pre-checks for `inventorydb`:

```sql
select count(*) from inventory where
  product_id is null or available_quantity is null or reserved_quantity is null;
select count(*) from inventory where available_quantity < 0 or reserved_quantity < 0;
```

If the existing order `price` column is floating point, precision already lost by the old
type cannot be recovered. The compatibility migration refuses null, non-positive,
out-of-range and more-than-two-decimal-place prices rather than silently rounding business
data. Resolve any returned rows explicitly before migration.

Manual adoption outline:

1. Stop both applications.
2. Take a database-native backup, for example `pg_dump`, and verify it can be restored.
3. Run the pre-check queries above and resolve any incompatible rows by an explicit
   operational decision.
4. Baseline each existing schema at version `1` using Flyway tooling or a one-off
   maintenance command with the service's migration directory.
5. Run Flyway migrate for that same service.
6. Start the application and confirm Hibernate validation succeeds.

Rollback is backup/restore. Do not treat conversion from `numeric(19,2)` back to floating
point as a lossless rollback; it can lose decimal fidelity.
