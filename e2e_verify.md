**What it is**

`e2e_verify.sh` is one bash script that acts as a test runner. It spins up your whole stack, sends real requests, deliberately breaks things, and checks after each step that the system behaved correctly. It prints PASS or FAIL for each check and a summary at the end. The word "pipeline" usually means a CI system like GitHub Actions. This script isn't one, but you could call it from one later, because it exits with a non-zero code when anything fails.

**Where to run it**

- **Not in cmd.exe or PowerShell.** The script uses bash syntax (`$(...)`, `<<<`, `{1..5}`), and your original commands already did too.
- **On your Mac:** open Terminal.
- **On your Windows desktop:** use WSL2 or Git Bash, with Docker Desktop running and WSL integration on.
- **On Windows, line endings can break it.** If you see `\r: command not found`, run `dos2unix e2e_verify.sh` first.

```bash
cd /path/to/FoodPlatform        # the folder with docker-compose.yml
bash e2e_verify.sh              # or: chmod +x e2e_verify.sh && ./e2e_verify.sh
```

**Before you run it:**
- Section 1 runs `docker compose down -v`, which wipes all your database volumes.
- A full run takes several minutes, mostly waiting on rebuilds and chaos recovery.
- Optionally, `RUN_OUTBOX_ALERT=1 bash e2e_verify.sh` adds the slow OutboxStuck alert test, about 3 more minutes.
- Open Grafana at `localhost:3000` while it runs to watch the outbox and Kafka panels react.

**What happens, in order**

1. **Rebuild and health (sections 1–2):** it rebuilds everything, waits until Prometheus sees all four services, checks the consumers and publishers started and the Grafana dashboard exists, then seeds 100 units of stock.
2. **Order paths (3–5):**
   - Happy path: order confirmed, stock 78.
   - Not enough stock: order cancelled.
   - Payment failure: the order is cancelled and the stock is restored by the compensating transaction.
3. **Defensive behaviour (6–7):** it injects duplicate events, out-of-order events and corrupt messages. It checks that stock isn't deducted twice, stale events are rejected, and bad messages land in the dead-letter queue.
4. **Concurrency (8):** it scales `order-service` to 3 replicas, fires 5 orders in parallel across their ports, and checks exactly 5 units were deducted with no duplicate publishes.
5. **Infrastructure chaos (9, 15):** it stops Kafka and confirms orders still succeed through the outbox, then drains once Kafka returns. It also stops `payment-service` and watches consumer lag spike and recover.
6. **Telemetry checks (10–14):**
   - Request rate and DB connections.
   - Retry and DLQ metrics.
   - P95 latency and route-label cardinality.
   - The business funnel counters.
   - Alert rules.
7. **Extra edge cases (16–17):** an oversell race for the last units, invalid input, an unknown product, and the 1000.00 payment boundary.
8. **Wrap-up (18–19):** a log scan for panics, then scale back to 1 replica.

**Reading the result**

Each check prints `PASS` or `FAIL (expected … | got …)`, and the last lines show the totals. A FAIL doesn't stop the run, so you get the full picture in one go. The first FAIL in a chain is usually the real problem, and later ones are often fallout from it.

For the first run, I'd skim the output top to bottom. A mismatch on a metric label or log string is more likely a naming difference than a real bug, and those are quick to fix in the script.