
# FoodPlatform

A distributed, event-driven microservices food ordering platform built with **Go**, **Apache Kafka**, and **PostgreSQL** . The platform implements a **Choreography-based Saga pattern** to maintain eventual consistency across distributed services and uses the **Transactional Outbox pattern** to prevent dual-write anomalies .

---

## Architecture Overview

                  +------------------+
                  |   HTTP Client    |
                  +--------+---------+
                           |
                    POST /orders 
                           v
                 +--------------------+
                 |   order-service    |
                 |  (Port: 8080)      | 
                 +---------+----------+
                           |
                  Kafka: order-events 
                           v
       +-------------------+-------------------+
       |                                       |
       v                                       v

```

+-----------------------+              +-----------------------+
|   inventory-service   |              | notification-service  |
|      (Port: 8081)     |              |      (Port: 8084)     |
+----------+------------+              +-----------------------+
|
Kafka: inventory-events
v
+-----------------------+
|    payment-service    |
|      (Port: 8082)     |
+----------+------------+
|
Kafka: payment-events
|
+-------------------> order-service (evaluates & finalizes)

```

### Services & Responsibilities

| Service | Port | Database | Primary Responsibility |
| :--- | :--- | :--- | :--- |
| **`order-service`**  | `8080`  | `order-db` (`5432`)  | Order intake, state machine transitions (`PENDING`, `AWAITING_PAYMENT`, `CONFIRMED`, `CANCELLED`), and compensation triggers . |
| **`inventory-service`**  | `8081`  | `inventory-db` (`5433`)  | Stock allocation, reservation validation, and stock releases during saga compensation . |
| **`payment-service`**  | `8082`  | `payment-db` (`5434`)  | Payment record handling and simulated payment validation logic (amounts > 1000 fail) . |
| **`notification-service`**  | `8084`  | `notification-db` (`5435`)  | Terminal event sink that persists user alerts on order confirmation or cancellation . |

---

## Core Patterns & Design Decisions

### 1. Choreography-Based Saga
Instead of an orchestrator acting as a single point of failure, services subscribe to topic events and advance state independently :
* **Happy Path**: `OrderCreated` to `InventoryReserved` to `PaymentSucceeded` to `OrderConfirmed` to `Notification Created` .
* **Payment Failure & Compensation**: `PaymentFailed` to `order-service` updates to `CANCELLED` and emits `OrderCancelled` with `ReleaseInventory: true` to `inventory-service` restores reserved stock to `notification-service` records cancellation alert .
* **Inventory Rejection (Early Short-Circuit)**: If requested quantity exceeds stock, `inventory-service` emits `InventoryRejected` to `order-service` updates status to `CANCELLED` to `payment-service` never charges or creates records .

### 2. Transactional Outbox Pattern
To prevent distributed inconsistencies caused by partial failures (dual-write problem), services **never publish directly to Kafka within an incoming HTTP handler or consumer transaction** .
* State mutations and event payloads are committed atomically to their local PostgreSQL instance within the same database transaction (`BEGIN ... COMMIT`) .
* A dedicated background loop polls the local `outbox` table, writes messages to Kafka partitioned by `aggregate_key` (`order_id`), and marks rows as `published = true` .

### 3. Graceful Shutdown & Context Propagation
Each microservice uses `signal.NotifyContext` to trap `SIGINT` (Ctrl+C) and `SIGTERM` (container stop signals) :
* HTTP servers cleanly drain in-flight client requests using `http.Server.Shutdown()` with a 5-second deadline context .
* Kafka reader and publisher polling loops monitor `ctx.Done()`, breaking loops cleanly without terminating mid-transaction or generating unhandled connection errors .
* Database connection pools (`pgxpool.Pool`) are deferred to close only after workers stop .

---

## Tech Stack

* **Language**: Go 1.25.6 
* **Routing & HTTP**: Gin 
* **Messaging**: Apache Kafka (KRaft mode via `apache/kafka:latest`) 
* **Kafka Driver**: `segmentio/kafka-go` 
* **Database**: PostgreSQL 16 (isolated DB per microservice) 
* **Database Driver & Pooling**: `jackc/pgx/v5` & `puddle/v2` 
* **Code Generation**: `sqlc` for type-safe SQL queries 
* **Logging**: Standard library structured logger (`log/slog`) 
* **Containerization**: Docker Compose 

---

## Getting Started

### Prerequisites
* Docker & Docker Compose 
* `curl` 

### Run the Entire Platform
```bash
# 1. Clean previous runs and purge volumes
docker compose down -v 

# 2. Build images and start all services, databases, and Kafka
docker compose build --no-cache 
docker compose up -d 

# 3. Wait for database healthchecks and topic provisioning
sleep 25 
docker compose ps 

```

---

## Verification & Test Scenarios

The included `check.txt` script verifies all distributed execution flows:

### 1. Seed Inventory

```bash
curl -X POST localhost:8081/inventory \
  -H "Content-Type: application/json" \
  -d '{"product_id":1,"quantity":100}' 

```

### 2. Happy Path Test

```bash
curl -X POST localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id":1,"items":[{"product_id":1,"quantity":2,"unit_price":9.99}]}' 

# Verify final status
docker exec -it order-db psql -U wreker -d order-db -c "SELECT id, status FROM orders WHERE id = 1;" 
# Output: CONFIRMED 

```

### 3. Payment Failure & Saga Compensation Test

```bash
# Total amount > 1000 triggers payment failure
curl -X POST localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id":1,"items":[{"product_id":1,"quantity":2,"unit_price":600}]}' 

# Verify compensation
docker exec -it order-db psql -U wreker -d order-db -c "SELECT id, status FROM orders WHERE id = 2;" 
# Output: CANCELLED 

docker exec -it inventory-db psql -U wreker -d inventory-db -c "SELECT available_quantity FROM inventory WHERE product_id = 1;" 
# Output: 98 (stock restored) 

```

### 4. Inventory Rejection (Out of Stock)

```bash
curl -X POST localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id":1,"items":[{"product_id":1,"quantity":9999,"unit_price":9.99}]}' 

# Verify short-circuit
docker exec -it order-db psql -U wreker -d order-db -c "SELECT id, status FROM orders WHERE id = 3;" 
# Output: CANCELLED 

docker exec -it payment-db psql -U wreker -d payment-db -c "SELECT * FROM payments WHERE order_id = 3;" 
# Output: (0 rows) 

```

---

## Incoming

Planned additions and architectural enhancements currently on the roadmap:

* [ ] **Kubernetes Migration with KinD**:
* Containerization deployment manifests (`Deployment`, `Service`, `ConfigMap`, `Secret`).
* Liveness and readiness probes connected to health check endpoints.
* Ingress controller configuration for routing incoming traffic to services.


* [ ] **Outbox Polling Concurrency Locks**:
* Implement PostgreSQL row-level locking (`SELECT ... FOR UPDATE SKIP LOCKED`) in outbox queries to prevent duplicate event dispatches when running multiple service replicas.


* [ ] **Consumer Idempotency Keys**:
* Introduce dedicated `processed_events` deduplication tables to enforce idempotent consumer processing against at-least-once Kafka deliveries.


* [ ] **Distributed Tracing & Observability**:
* Add OpenTelemetry tracing headers across Kafka message envelopes and HTTP requests.
* Integrate Prometheus metrics export and Grafana visualization.


* [ ] **Dead Letter Queues (DLQ)**:
* Add retry topics and dead letter queues for poison-pill messages that fail unmarshaling or business validation.
