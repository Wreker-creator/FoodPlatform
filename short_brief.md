## 1. Master Task Audit: Completed vs. Pending

### Completed (Stage 1 & Core Stage 4)

* **Resilient Kafka Plumbing:** Explicit manual offset commits (`FetchMessage` $\rightarrow$ DB Transaction $\rightarrow$ `CommitMessages`), `FOR UPDATE SKIP LOCKED` outbox polling, two-tier idempotency (`processed_events` table + guarded SQL state updates), and automated poison-pill quarantine to `dead-letter-events`.


* **HTTP RED Telemetry:** Custom Gin middleware (`PromMiddleware`) on all four services tracking `http_requests_total`, `http_request_duration_seconds`, and `http_requests_in_flight`, plus a dedicated HTTP server on `:8084` for `notification-service`.


* **Kafka Consumer Telemetry:** `kafka_events_processed_total` (tracking `ok`, `duplicate` via `ErrDuplicateEvent`, and `dlq`), `kafka_event_processing_duration_seconds`, `kafka_dlq_messages_total`, and the 5-second `kafka_consumer_lag` ticker.
* **Outbox & DB Pool Telemetry:** 5-second tickers for `outbox_unpublished_events_total`, `outbox_oldest_unpublished_age_seconds`, `outbox_publish_failures_total`, and `pgxpool` connection gauges (`db_connections` idle/acquired, `db_connections_max`), backed by the `idx_outbox_unpublished` partial index.


* **Grafana & Prometheus as Code:** Prometheus configured with `dns_sd_configs` for `order-service` replicas and static targets for the remaining services; Grafana configured with anonymous admin access and auto-provisioned datasources/dashboards (`platform.json`).

---

### Pending (Prioritized Action Plan)

1. **Stage 1 Final Polish (Business Counters & Retry Metric):**
* Add business-level counters (`orders_created_total`, `orders_final_status_total`, `payments_total`, `inventory_rejections_total`) to power the Order Funnel dashboard.
* Add the `"retry"` label increment to `EventsProcessedTotal` inside the `for attempt := 1; attempt <= maxRetries` loop in `consumer.go` so transient retries are visible before they succeed or hit the DLQ.




2. **Stage 4 Completion (Prometheus Alert Rules):**
* Create `observability/prometheus/alert_rules.yml` and wire it into `prometheus.yml` for the 5 core alerts: `OutboxOldestAge > 30s`, `DLQMessagesIncrease > 0`, `ConsumerLagRising`, `HTTP5xxRatioHigh`, and `ServiceInstanceDown (up == 0)`.


3. **Stage 2 (Distributed Tracing — OpenTelemetry & Jaeger):**
* Add `jaegertracing/all-in-one` to `docker-compose.yml`.
* Create `internal/telemetry/tracing.go` per service (OTLP exporter, `service.name` resource, W3C `TraceContext` propagator, and clean flush on shutdown).
* Add `otelgin` middleware and `otelpgx` tracer hook to `pgxpool` config.
* **Solve the Outbox Gap:** Add `traceparent TEXT` to the `outbox` tables, store the active span context during `InsertOutboxEvent`, inject it into `[]kafka.Header` via a custom `TextMapCarrier` in `publisher.go`, and extract it in `consumer.go` to parent the consumer and DB spans.




4. **Stage 3 (Correlated Structured Logs):**
* Wrap `slog.NewJSONHandler` with a custom handler that automatically reads the active span from `ctx` and injects `trace_id`, `span_id`, and `service` into every JSON log line.


* Replace remaining `slog.Info`/`slog.Error` calls with `slog.InfoContext`/`slog.ErrorContext`.
* Add a structured `slog` JSON access-log middleware to `gin.New()` routers.



---

## 2. Deep Dive: How Everything Works Together So Far

### Part A: Kafka & Distributed State Mechanics

#### 1. The Transactional Outbox & Publisher Loop (`publisher.go`)

* **Solving the Dual-Write Problem:** When an HTTP handler or consumer updates business tables (like `orders`), it inserts an event into the `outbox` table inside the exact same Postgres transaction. If Postgres commits, the event is guaranteed to exist; if it rolls back, neither the state change nor the event exists.


* **Concurrency via `FOR UPDATE SKIP LOCKED`:** Every 5 seconds, `Start()` triggers `processBatch(ctx)`. It opens a transaction with `p.Pool.Begin(ctx)` and runs `GetUnpublishedEvents`. Because the SQL query uses `FOR UPDATE SKIP LOCKED`, when 3 replicas of `order-service` poll `order-db` simultaneously, Postgres locks the batch of rows assigned to Replica 1 and allows Replica 2 and 3 to immediately skip those locked rows and grab the next unpublished rows without blocking or double-publishing.


* **Partition Ordering & Batch Halting:** Each outbox event is published with `Key: []byte(e.AggregateKey)` (the `order_id`). Kafka hashes the key so all events for Order `1` always land on the exact same partition in strict chronological order. If `p.PublishRaw` fails on row 1, the loop increments `telemetry.OutboxPublishFailures` and executes `break` immediately. If it continued to row 2, a later event for the same order could be published before the earlier one, corrupting downstream state.



#### 2. Consumer Delivery Guarantees & Context Lifecycles (`consumer.go`)

* **At-Least-Once via Manual Commits:** Instead of `ReadMessage` (which auto-commits the offset the moment the message is read off the wire), `Start()` calls `c.reader.FetchMessage(ctx)`. The offset is only committed at the very bottom of the loop via `c.reader.CommitMessages(commitCtx, msg)` *after* `c.Read(ctx, msg)` has committed its Postgres transaction.


* **Decoupled Shutdown Contexts:** Both `defer tx.Rollback(context.Background())` in `Read()` and `commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)` in `Start()` intentionally avoid using the parent `ctx`. When Docker sends `SIGTERM`, `ctx` is immediately canceled. If `tx.Rollback` or `CommitMessages` used that canceled `ctx`, the driver would abort the network call mid-flight, leaving dangling locks in Postgres or uncommitted offsets in Kafka. Also, calling `cancel()` explicitly right after `CommitMessages` (instead of `defer cancel()`) prevents context structs from leaking in memory across infinite loop iterations.



#### 3. Two-Tier Defense & Sentinel Error Routing

Inside `Read(ctx, msg)`, your consumer protects consistency on two distinct axes inside a single database transaction:

1. **Idempotency Keys (Protects Actions/Math):** `qtx.InsertProcessedEvent(ctx, envelope.EventID)` runs `INSERT ... ON CONFLICT DO NOTHING`. If `rowsAffected == 0`, the exact `event_id` (e.g., `"order-1"`) was already processed.


2. **Guarded State Updates (Protects State Machine):** `qtx.UpdateOrderStatusGuarded` executes `UPDATE orders SET status = $2 WHERE id = $1 AND status = $3`. If an out-of-order `InventoryReserved` event arrives after an order is already `CONFIRMED`, `WHERE status = 'PENDING'` fails to match, returning `rowsAffected == 0`.



* **Why `ErrDuplicateEvent` Matters:** Both checks return `ErrDuplicateEvent`. In `Start()`, `if processErr == nil || errors.Is(processErr, ErrDuplicateEvent)` breaks out of the 3-attempt retry loop immediately so the consumer doesn't waste 600ms of backoff retrying a known duplicate, and records `status="duplicate"` instead of polluting `status="ok"` or routing valid state rejections to the DLQ.

#### 4. Dead Letter Queue (`routeToDlq`)

* **Quarantine Mechanics:** Every service initializes a shared `dlqWriter` pointing to `kafka:9094` on the `dead-letter-events` topic using `&kafkago.LeastBytes{}` to balance payloads across partitions.


* **Two Entry Paths:**
1. *Unrecoverable Syntax Error:* If `json.Unmarshal(msg.Value, &envelope)` fails at the top of `Read()`, retrying won't fix broken JSON—it calls `c.routeToDlq` immediately and returns `nil` so the offset commits and unblocks the partition.


2. *Exhausted Retries:* If a downstream error persists across all `maxRetries := 3` attempts (with `attempt * 200ms` backoff), `Start()` catches `processErr != nil`, calls `c.routeToDlq`, increments `DLQMessagesTotal`, and commits the offset to prevent Head-of-Line blocking.





---

### Part B: Docker Networking & Infrastructure Mechanics

| Component / Config | How It Works Under the Hood | Why We Engineered It This Way |
| --- | --- | --- |
| **Dual Kafka Listeners (`9092` vs `9094`)** | Microservices connect to `kafka:9094`, while CLI commands inside the broker use `localhost:9092`.

 | Kafka requires separate advertised listeners for internal container-to-container bridge traffic (`kafka:9094`) versus local/host loopbacks (`localhost:9092`).

 |
| **Embedded Docker DNS (`127.0.0.11:53`)** | Every container resolves service names (like `kafka` or `order-service`) via Docker's internal DNS resolver at `127.0.0.11`. | When you ran `docker compose stop kafka`, Docker removed `kafka`'s A-record from `127.0.0.11`, causing `kafka-go` readers to log `lookup kafka on 127.0.0.11:53: no such host` until the container restarted. |
| **Multi-Replica Scaling & `dns_sd_configs**` | `order-service` omits `container_name` and binds port range `9080-9082:8080`. Prometheus queries `order-service` with `type: A`. | Hardcoding `container_name` blocks `docker compose up -d --scale order-service=3`. Using `dns_sd_configs` (`type: A`) returns all 3 container IPs (`172.19.0.x:8080`) from `127.0.0.11` so Prometheus scrapes every replica individually.

 |
| **Provisioning Mounts (`/etc/grafana/provisioning`)** | Bind-mounts local YAML and JSON files into the Grafana container on startup. | Because `docker compose down -v` destroys named volumes on every clean test run, UI-created dashboards are wiped. Bind-mounting from the host repo guarantees dashboards survive `down -v`.

 |

---

### Part C: Telemetry & Database Performance Mechanics

#### 1. Pull-Based Scraping & The Notification HTTP Server (`main.go`)

Prometheus cannot read metrics from Kafka or process memory—it requires an HTTP target. Even though `notification-service` is primarily an event consumer on `notification-order-group` (with a single `GET /notifications/:id` lookup route), running the `http.Server` on `:8084` exposes `/metrics` via `promhttp.Handler()` so Prometheus can scrape its consumer lag, DLQ count, and DB pool stats alongside the other three services.

#### 2. High-Cardinality Prevention (`c.FullPath()`)

In `PromMiddleware()`, the `route` label uses `c.FullPath()` rather than `c.Request.URL.Path`. When a client requests `GET /notifications/1`, `c.Request.URL.Path` is `"/notifications/1"`, whereas `c.FullPath()` returns the route template `"/notifications/:id"`. Using raw paths or attaching `order_id` as a label would create a brand-new Prometheus time series for every single order, exhausting Prometheus RAM (cardinality explosion).

#### 3. `kafka-go` Reader Stats Trap (`c.reader.Stats().Lag`)

In `consumer.go`, the 5-second background ticker calls `stats := c.reader.Stats()` and only extracts `stats.Lag`. In `segmentio/kafka-go`, calling `.Stats()` resets internal counter fields (like `Messages` and `Bytes`) back to zero on every invocation, which corrupts Prometheus counters if read on a ticker. `Lag`, however, is a point-in-time snapshot (Last Offset minus Current Offset) and is safe to expose as a `GaugeVec`.

#### 4. The Outbox Ticker & Partial Index (`idx_outbox_unpublished`)

Every 5 seconds, the publisher's telemetry goroutine runs two queries against Postgres:

1. `SELECT count(*) FROM outbox WHERE published = false` (`outbox_unpublished_events_total`)
2. `SELECT COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(created_at))), 0) FROM outbox WHERE published = false` (`outbox_oldest_unpublished_age_seconds`)

Because published rows in `outbox` are never deleted (only updated to `published = true`), an unindexed table with 1,000,000 rows and 0 unpublished rows would still force Postgres to scan all 1,000,000 rows every 5 seconds. The partial index (`CREATE INDEX idx_outbox_unpublished ON outbox (created_at) WHERE published = false`) indexes *only* the rows where `published = false`. When the outbox is caught up, the B-tree index contains **0 entries**, making both Prometheus queries instantaneous $O(1)$ lookups.

---

Are there any specific parts of the Kafka consumer/publisher flow, Docker DNS scaling, or Prometheus metrics mechanics you want to drill into deeper, or shall we knock out **Step 1 of the pending list** (adding the `"retry"` label and business counters) before wiring up Alert Rules and Jaeger?