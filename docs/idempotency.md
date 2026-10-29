# Why We Need Both: Guarded Updates & Idempotency Keys


Kafka uses "At-Least-Once" delivery. This guarantees we never lose a message, but it means Kafka WILL occasionally 
send us exact duplicates (e.g., if a pod crashes right after processing but before telling Kafka it's done).

Here is how we protect the system from those duplicates:
## 1. Guarded Updates (What we already built)
*   **What it is:** Adding `WHERE status = 'EXPECTED_STATE'` to our SQL updates.
*   **What it protects:** STATES (State Machines).
*   **Why it works for Orders:** An order goes `PENDING` -> `AWAITING_PAYMENT`. If a duplicate "Payment Succeeded" event arrives, 
    the database says, "Wait, this order is already Paid, rowsAffected = 0."
*   **The blind spot:** It cannot protect against math operations or external APIs.

## 2. Idempotency Keys / processed_events (What we are building now)
*   **What it is:** A database table that simply records the unique `event_id` of every Kafka message we process.
*   **What it protects:** ACTIONS (Math, 3rd Party APIs, Emails).
*   **Why we absolutely need it:**
    *   **Inventory (Math):** `stock = stock - 2`. If a duplicate arrives, a guarded update can't catch it. The database will 
    happily subtract 2 again. The idempotency table catches the duplicate `event_id` and stops it.
    *   **Payments & Emails (External):** You can't rollback an email or a Stripe charge. We must check the `processed_events` 
    table BEFORE we talk to the outside world to prevent double-charging or spamming the customer.

## The Golden Rule
*   Guarded Updates protect "WHAT IT IS" (State).
*   Idempotency Keys protect "WHAT IT DOES" (Actions & Math).
*   To build a bulletproof microservice, you need BOTH.