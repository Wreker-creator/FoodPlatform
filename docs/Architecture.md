# Distributed Architecture Patterns & Study Guide

This document outlines the core distributed systems concepts implemented in this microservices platform. It serves as both system documentation and a practical study guide for backend engineering interviews.

---

## 1. The Choreography Saga Pattern

*   **What it is:** A sequence of local database transactions across multiple microservices. Instead of a central controller telling everyone what to do, each service updates its own database and publishes an event to a message broker (Kafka) to trigger the next step.
*   **What it protects:** Data consistency across independent databases.
*   **Why we absolutely need it:** In a Database-per-Service architecture, we cannot use a traditional ACID transaction to lock tables across Order, Inventory, and Payment databases. If Inventory succeeds but Payment fails, we need a mechanism to tell Inventory and Order to "roll back" via **compensating transactions** (e.g., releasing reserved stock and marking the order as `CANCELLED`).
*   **The Golden Rule:** If you can't lock it across a network, you must be able to compensate for it.

### 💡 Common Interview Questions:
*   **Q: What is the difference between Saga Choreography and Saga Orchestration?**
    *   *A:* Choreography is decentralized—services react to events like a dance. Orchestration relies on a central "manager" service that explicitly sends commands to other services.
*   **Q: How do you handle a failure in the middle of a Saga?**
    *   *A:* The service that experiences the failure publishes a "Failure" event. Upstream services consume this event and execute compensating transactions to reverse their previous actions.

---

## 2. The Transactional Outbox Pattern

*   **What it is:** Saving business state (e.g., an Order) AND an event payload to an `outbox` table within the **exact same ACID database transaction**. A separate background worker constantly polls this outbox table and publishes the messages to Kafka.
*   **What it protects:** Guaranteed message delivery ("At-Least-Once" semantics).
*   **Why we absolutely need it:** To solve the **Dual-Write Problem**. If we save data to Postgres and then try to publish to Kafka in two separate steps, a network blip could cause the Kafka publish to fail. The database would have the new order, but downstream services would never know about it. The Outbox guarantees the intent to publish is saved safely alongside the business data.
*   **The Golden Rule:** Never write to a database and publish to a network broker in two separate steps without an outbox.

### 💡 Common Interview Questions:
*   **Q: Describe the Dual-Write Problem.**
    *   *A:* It occurs when a system needs to update a database and send a message to a message broker. If the system crashes between the two operations, the system is left in an inconsistent state.
*   **Q: How does the Outbox Pattern guarantee message delivery?**
    *   *A:* Because the event is secured in a relational database using ACID transactions. A background worker can infinitely retry publishing the event to the broker until it receives a successful acknowledgment.

---

## 3. Concurrency Locks (`FOR UPDATE SKIP LOCKED`)

*   **What it is:** Database row-level locking used by background workers. When fetching data, it locks the rows it reads and explicitly skips any rows currently locked by another process.
*   **What it protects:** Horizontal scaling and duplicate processing.
*   **Why we absolutely need it:** If we scale the `order-service` to 3 replicas, all 3 background publishers will poll the same `outbox` table simultaneously. Without `SKIP LOCKED`, all 3 replicas would grab the exact same pending messages, resulting in 3x duplicates being sent to Kafka. `SKIP LOCKED` lets the replicas peacefully share the queue—Replica 1 takes rows 1-5, Replica 2 skips those and takes 6-10.
*   **The Golden Rule:** Let the database handle the locking so your application can scale statelessly.

### 💡 Common Interview Questions:
*   **Q: How do you prevent multiple worker pods from processing the same database row?**
    *   *A:* By using SQL locking mechanisms like `FOR UPDATE SKIP LOCKED`, which allows multiple workers to consume rows from a table as a queue without blocking each other.
*   **Q: Why not just use `FOR UPDATE` without `SKIP LOCKED`?**
    *   *A:* `FOR UPDATE` will cause other workers to block and wait for the lock to be released, destroying concurrent throughput. `SKIP LOCKED` allows them to instantly bypass locked rows and process the next available work.

---

## 4. Event Idempotency (Idempotency Keys)

*   **What it is:** A database table (`processed_events`) that simply records the unique `event_id` of every Kafka message a service processes. The service checks this table before processing a message.
*   **What it protects:** ACTIONS (Math, External APIs, Emails).
*   **Why we absolutely need it:** Kafka uses "At-Least-Once" delivery, meaning duplicates *will* happen.
    *   *Inventory (Math):* `stock = stock - 2`. If a duplicate arrives, the database will happily subtract 2 again, resulting in lost virtual stock. The idempotency table catches the duplicate `event_id` and stops the transaction.
    *   *External APIs:* You cannot roll back an email or a Stripe charge. We must verify the `event_id` before talking to the outside world to prevent double-charging a customer.
*   **The Golden Rule:** Protect "WHAT IT DOES" (Actions & Math) with Idempotency Keys.

### 💡 Common Interview Questions:
*   **Q: How do you handle at-least-once delivery duplicates in Kafka?**
    *   *A:* By designing idempotent consumers. We extract a unique idempotency key (like an event ID) from the message and store it in a database upon successful processing. If a message with the same ID arrives again, we skip it.
*   **Q: How do you prevent double-charging a user's credit card in a distributed system?**
    *   *A:* The client must provide a unique Idempotency Key (like a UUID) with the payment request. The payment service checks this key against its database before making the API call to Stripe.

---

## 5. State-Based Guarded Updates

*   **What it is:** Adding a strict condition (e.g., `WHERE status = 'EXPECTED_STATE'`) to SQL `UPDATE` statements, and combining it with application logic that checks `rowsAffected`. If 0 rows are affected, the application safely ignores the event.
*   **What it protects:** STATES (State Machines) and Out-of-Order messages.
*   **Why we absolutely need it:** Network delays cause race conditions. If an order goes `PENDING` -> `CANCELLED` (due to failed inventory), and a delayed `PaymentSuccess` event arrives 10 seconds later, we shouldn't update the cancelled order to `CONFIRMED`. The database guard (`WHERE status = 'PENDING'`) says, "Wait, this order is already cancelled," and returns `rowsAffected = 0`.
*   **The Golden Rule:** Protect "WHAT IT IS" (State) with Guarded Updates.

### 💡 Common Interview Questions:
*   **Q: How do you handle out-of-order events in an event-driven architecture?**
    *   *A:* By implementing strict state machines in the database using Guarded Updates, or by using version numbers (Optimistic Concurrency Control) in the SQL queries to reject stale data.
*   **Q: Why not just `SELECT` the state, check it in Go code, and then run the `UPDATE`?**
    *   *A:* This creates a Race Condition (Time-of-Check to Time-of-Use). Another concurrent process could change the state in the database between your `SELECT` and your `UPDATE`. Guarded Updates do the check and the write atomically.

---

## 6. Dead Letter Queues (DLQ)

*   **What it is:** A dedicated Kafka topic (`dead-letter-events`) that acts as a quarantine zone for messages that permanently fail processing.
*   **What it protects:** Queue flow and Data Retention.
*   **Why we absolutely need it:** Systems will encounter "Poison Pills" (e.g., malformed JSON, hard DB constraint violations). Without a DLQ, a consumer has two bad choices:
    1.  *Infinite Retries:* Blocking the partition forever (Head-of-Line Blocking), stopping all valid orders behind it.
    2.  *Silent Drop:* Logging an error and skipping the message, resulting in permanent data loss.
    The DLQ allows us to quarantine the broken message, unblock the queue, and manually inspect/replay the data later.
*   **The Golden Rule:** Never silently drop a message, and never block a queue infinitely.

### 💡 Common Interview Questions:
*   **Q: What is Head-of-Line Blocking and how do you solve it in Kafka?**
    *   *A:* It occurs when a consumer gets stuck infinitely retrying a bad message, blocking all subsequent messages in that partition. It is solved by implementing a maximum retry limit and routing the bad message to a Dead Letter Queue.
*   **Q: What information should be included in a DLQ payload?**
    *   *A:* The original raw message bytes, the original topic/key, a timestamp of the failure, and the specific error string/reason for the failure so engineers can debug it.

---

## 7. Distributed Observability & RED Metrics

* **What it is:** Exposing standard application metrics (Rate, Errors, Duration) via HTTP for a time-series database (Prometheus) to scrape, and visualizing them (Grafana).
* **What it protects:** Mean Time To Recovery (MTTR) and system visibility.
* **Why we absolutely need it:** In a distributed system, relying on raw logs to detect a microservice bottleneck is too slow. By standardizing identical metric names across all services (e.g., `http_requests_total`), Prometheus can use `job` labels to instantly aggregate global throughput, or isolate error rates per microservice.
* **The Golden Rule:** Keep metric names identical across services; let labels differentiate the source.

### 💡 Common Interview Questions:

* **Q: How do you monitor the health of a distributed microservice architecture?**
* *A:* By tracking RED metrics (Rate, Errors, Duration) for synchronous APIs and Consumer Lag for asynchronous background workers, utilizing a pull-based time-series database like Prometheus.


* **Q: Why use a Histogram metric instead of calculating the average processing time in your code?**
* *A:* Averages hide outliers. A histogram places data into predefined time buckets, allowing you to calculate the 95th or 99th percentile (P95/P99) to see the actual latency experienced by the slowest requests.



---

## 8. Telemetry-Driven Database Optimization (Partial Indexes)

* **What it is:** Creating a SQL index that only tracks a subset of rows based on a specific condition (e.g., `CREATE INDEX ... WHERE published = false`).
* **What it protects:** Database CPU and operational stability during constant background polling.
* **Why we absolutely need it:** To monitor "Outbox Lag," a background ticker must constantly query the database for unpublished events. Because the outbox is an append-only ledger that grows infinitely, `SELECT count(*) WHERE published = false` will eventually trigger a full table scan, pegging CPU at 100%. The partial index ensures the lookup remains $O(1)$ and automatically self-cleans as rows are published.
* **The Golden Rule:** Never run continuous aggregate queries on growing transactional tables without a partial index.

### 💡 Common Interview Questions:

* **Q: How do you calculate queue lag in a Postgres-based outbox table without taking down the database?**
* *A:* By applying a Partial Index on the un-processed state. The database only indexes the handful of pending rows, allowing the telemetry poller to count them or find the oldest timestamp in sub-millisecond time.



---

## 9. Safe Offset Management & Context Lifecycles

* **What it is:** Explicitly separating the fetching of Kafka messages from the committing of their offsets, and strictly managing Go context lifecycles during system shutdowns.
* **What it protects:** Data loss (dropping messages) and memory leaks.
* **Why we absolutely need it:**
* *Offsets:* If you use auto-commit (or `ReadMessage`), Kafka might advance the offset before your database transaction finishes. If the DB transaction fails, the message is permanently lost. Using `FetchMessage` ensures the offset only advances manually *after* the DB commit.
* *Contexts:* Using `defer cancel()` inside an infinite consumer loop creates a massive memory leak because `defer` only executes when the outer function returns, causing contexts to pile up endlessly. Additionally, during a Graceful Shutdown (SIGTERM), the original request context is cancelled. If a DB rollback relies on that cancelled context, the rollback will fail. You must use `context.Background()` for the rollback defer.


* **The Golden Rule:** Never commit an offset until the data is safely on disk, and never put a `defer` inside an infinite loop.

### 💡 Common Interview Questions:

* **Q: How do you guarantee at-least-once processing in a Kafka consumer?**
* *A:* By disabling auto-commit, using `FetchMessage` to read the event, executing the database transaction, and only calling `CommitMessages` manually once the DB transaction successfully commits.


* **Q: What is a common memory leak in Go consumer loops and how do you fix it?**
* *A:* Placing `defer cancel()` inside an infinite `for` loop. It should be replaced with explicit `cancel()` calls at the end of the iteration, or by wrapping the loop's body in an inner function where `defer` can safely execute upon return.


* **Q: Why might a deferred database rollback fail during a pod shutdown?**
* *A:* Because the context passed to it is already cancelled by the shutdown signal. Rollbacks must be executed using a fresh `context.Background()` to ensure they process regardless of the shutdown state.



---

## 10. Sentinel Errors & Structured Control Flow

* **What it is:** Defining explicit, globally checkable errors (e.g., `var ErrDuplicateEvent = errors.New(...)`) to signal precise business conditions, and handling errors strictly at system boundaries.
* **What it protects:** Telemetry accuracy and CPU waste (by preventing infinite retry loops).
* **Why we absolutely need it:** If a consumer identifies a duplicate event and simply returns `nil`, Prometheus counts it as a standard "success." If it returns a generic error, the system applies a 3-retry backoff policy, wasting CPU on an event we already intentionally decided to skip. A sentinel error allows the consumer loop to immediately `break` the retry cycle while explicitly incrementing the `"duplicate"` Prometheus label.
* **The Golden Rule:** Wrap errors with context at the source, log them only once at the top boundary, and use Sentinel Errors for distinct business logic branching.

### 💡 Common Interview Questions:

* **Q: What are Sentinel Errors in Go and when should you use them?**
* *A:* Sentinel errors are pre-declared package-level error variables. You use them when the caller needs to use `errors.Is()` to branch its logic based on the specific type of failure (e.g., distinguishing a transient database timeout from a known duplicate idempotency match).


* **Q: In the Outbox publisher, what happens if Kafka is down and the publish attempt fails?**
* *A:* The code must log the error and immediately `break` the batch loop to preserve strict FIFO ordering, preventing subsequent messages in the batch from jumping ahead in the queue.

## Synthesis: Why Microservices Require Multiple Patterns

To build a bulletproof microservice, you must combine these tools. Guarded Updates protect the **State** (ensuring transitions are valid), while Idempotency Keys protect the **Actions** (ensuring math and API calls only happen once). Finally, the DLQ ensures that when the unexpected inevitably happens, your system bends instead of breaking.