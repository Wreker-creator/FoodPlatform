Here is the documentation formatted for GitHub. You can save this as `docs/DLQ_ARCHITECTURE.md` or add it to your `README.md`.

```markdown
# Dead Letter Queue (DLQ) Architecture

## What is a DLQ?
A Dead Letter Queue (DLQ) is a dedicated Kafka topic (in our case, `dead-letter-events`) that acts as a quarantine zone for failed messages. If a message cannot be processed successfully, it is routed here rather than being dropped or retried infinitely.

## Why We Added It
In an event-driven architecture, you will inevitably encounter **"Poison Pills"**—messages that constantly fail due to:
1. **Malformed Data:** Invalid JSON payloads or schema mismatches.
2. **Persistent Failures:** Hard database constraint violations or 3rd-party API outages.

Without a DLQ, a consumer facing a poison pill has two bad options:
* **Infinite Retries (Head-of-Line Blocking):** The consumer gets stuck in a loop trying to process the bad message. The partition halts, and all valid orders behind the poison pill are blocked.
* **Silent Drops (Data Loss):** The consumer logs an error and skips the message to keep the queue moving. The order is permanently lost with no trace.

**The DLQ solves this:** It quarantines the broken message, commits the offset on the main topic, and keeps the assembly line moving. Developers can later monitor the DLQ, inspect the broken payloads, fix the bug, and replay the events.

## Where It Belongs
**Every microservice that acts as a Kafka consumer must implement the DLQ.** 
In this platform, it is required in:
- `order-service` (consumers: inventory-events, payment-events)
- `inventory-service` (consumer: order-events)
- `payment-service` (consumer: order-events)
- `notification-service` (consumer: order-events)

## Implementation Pattern
The implementation follows a standardized 4-step pattern across all microservices:

### 1. The Shared Struct
Each service defines a struct to wrap the broken message with its metadata and failure reason.
```go
type DLQMessage struct {
    OriginalTopic string    `json:"original_topic"`
    OriginalKey   string    `json:"original_key"`
    OriginalValue string    `json:"original_value"`
    ErrorReason   string    `json:"error_reason"`
    FailedAt      time.Time `json:"failed_at"`
}

```

### 2. Initialization (`main.go`)

A `kafka.Writer` is initialized at the application entry point and configured to use a `LeastBytes` balancer to ensure even distribution across DLQ partitions. It is then injected into the consumers.

```go
dlqWriter := &kafkago.Writer{
    Addr:     kafkago.TCP("kafka:9094"),
    Topic:    "dead-letter-events",
    Balancer: &kafkago.LeastBytes{},
}
defer dlqWriter.Close()

```

### 3. The Routing Helper (`consumer.go`)

The consumer provides a helper method to format the `DLQMessage` and publish it to the `dead-letter-events` topic.

### 4. The Loop Integration (`consumer.go`)

The DLQ is triggered in two specific scenarios within the consumer read loop:

1. **Instant Failure:** `json.Unmarshal` fails. (The payload is unreadable; retries are useless).
2. **Exhausted Retries:** The 3x retry loop completes without success.

**Crucial Step:** After successfully writing to the DLQ, the consumer *must* commit the offset of the original message to the Kafka broker. This confirms the poison pill has been handled and unblocks the partition.

```

```