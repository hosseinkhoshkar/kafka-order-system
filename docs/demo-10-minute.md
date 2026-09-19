# 10 Minute Demo Guide

Use a local development environment only. Do not run the failure steps against
shared or production infrastructure.

## Prerequisites

```powershell
$env:JAVA_HOME='C:\Users\Hossein\.jdks\corretto-17.0.14'
$env:Path="$env:JAVA_HOME\bin;$env:Path"
docker compose up -d
.\order-service\mvnw.cmd -B -ntp -f pom.xml package
```

Create topics and seed stock:

```powershell
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic orders --partitions 1 --replication-factor 1
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic inventory-reply --partitions 1 --replication-factor 1
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic orders.DLT --partitions 1 --replication-factor 1
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic inventory-reply.DLT --partitions 1 --replication-factor 1
docker compose exec -T postgres-inventory psql -U admin -d inventorydb -c "insert into inventory(product_id, available_quantity, reserved_quantity) values ('demo-product', 20, 0) on conflict (product_id) do update set available_quantity = 20, reserved_quantity = 0;"
```

Start apps in separate terminals:

```powershell
java -jar inventory-service/target/inventory-service-0.0.1-SNAPSHOT.jar
java -jar order-service/target/order-service-0.0.1-SNAPSHOT.jar
```

## 1. Successful Order

```powershell
$body = @{ productId='demo-product'; customerId='demo-customer'; quantity=2; price=12.50 } | ConvertTo-Json
$headers = @{ 'Idempotency-Key' = 'demo-success-001' }
$order = Invoke-RestMethod -Method Post -Uri 'http://localhost:8081/api/orders' -Headers $headers -ContentType 'application/json' -Body $body
$order
Start-Sleep -Seconds 8
Invoke-RestMethod -Uri "http://localhost:8081/api/orders/$($order.orderId)"
```

Expected: initial response is `PENDING`; the later GET becomes `CONFIRMED`.

## 2. Idempotency Replay

```powershell
Invoke-RestMethod -Method Post -Uri 'http://localhost:8081/api/orders' -Headers $headers -ContentType 'application/json' -Body $body
```

Expected: same `orderId` as step 1 and response header `Idempotent-Replay: true`
if inspected with `Invoke-WebRequest`.

## 3. Insufficient Inventory

```powershell
$body = @{ productId='demo-product'; customerId='demo-customer'; quantity=999; price=12.50 } | ConvertTo-Json
$headers = @{ 'Idempotency-Key' = 'demo-insufficient-001' }
$order = Invoke-RestMethod -Method Post -Uri 'http://localhost:8081/api/orders' -Headers $headers -ContentType 'application/json' -Body $body
Start-Sleep -Seconds 8
Invoke-RestMethod -Uri "http://localhost:8081/api/orders/$($order.orderId)"
```

Expected: initial response is `PENDING`; final status becomes `CANCELLED`.

## 4. Controlled Failure and Recovery

Stop only the local Kafka container, then create an order:

```powershell
docker compose stop kafka
$body = @{ productId='demo-product'; customerId='demo-customer'; quantity=1; price=12.50 } | ConvertTo-Json
$headers = @{ 'Idempotency-Key' = 'demo-kafka-down-001' }
$order = Invoke-RestMethod -Method Post -Uri 'http://localhost:8081/api/orders' -Headers $headers -ContentType 'application/json' -Body $body
docker compose exec -T postgres-order psql -U admin -d orderdb -c "select order_id, status from orders where order_id = '$($order.orderId)';"
docker compose exec -T postgres-order psql -U admin -d orderdb -c "select aggregate_id, status, attempt_count from outbox_events where aggregate_id = '$($order.orderId)';"
```

Expected: the order row exists and the Outbox row remains unpublished or retrying.

Restart Kafka:

```powershell
docker compose start kafka
Start-Sleep -Seconds 20
Invoke-RestMethod -Uri "http://localhost:8081/api/orders/$($order.orderId)"
```

Expected: after Kafka and consumers recover, the order can reach `CONFIRMED`.
This is a local demo disruption; do not run it against shared infrastructure.

## 5. Observe Logs, Metrics and DLT/Replay

Health and metrics:

```powershell
Invoke-RestMethod http://localhost:8081/actuator/health/readiness
Invoke-RestMethod http://localhost:8082/actuator/health/readiness
(Invoke-WebRequest http://localhost:8081/actuator/prometheus).Content | Select-String 'order_outbox_events|order_orders_created_total'
```

Optional monitoring:

```powershell
docker compose -f docker-compose.yml -f docker-compose.monitoring.yml --profile monitoring up -d
```

Open Prometheus at http://localhost:9090 and Grafana at http://localhost:3000.
The Grafana login is `admin/admin` for development only.

DLT/replay demo, if you intentionally publish malformed JSON to `orders`:

```powershell
docker compose exec -T kafka bash -lc "echo 'bad-demo:{not-json' | kafka-console-producer --bootstrap-server kafka:29092 --topic orders --property parse.key=true --property key.separator=:"
java -jar .\kafka-replay-tool\target\kafka-replay-tool.jar --bootstrap-servers localhost:9092 --dlt-topic orders.DLT --partition 0 --offset 0
```

Expected: the DLT listener logs metadata for the bad record; the replay command
is a dry run unless `--execute` is added.
