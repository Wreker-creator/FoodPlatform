#!/usr/bin/env bash
# ==============================================================================
# FoodPlatform end-to-end verification & chaos suite
#
# Run from the directory containing docker-compose.yml:   ./e2e_verify.sh
# Optional:  RUN_OUTBOX_ALERT=1 ./e2e_verify.sh   (also fires the slow OutboxStuck alert test)
#
# Assumptions (a mismatch shows up as a FAIL, not a silent pass):
#   - order-service publishes a host port from a range: ports are discovered at runtime and
#     concurrent orders are spread across every replica's port
#   - Fresh start (section 1 wipes volumes) so order IDs are sequential from 1
#   - Container names == DB names: order-db, inventory-db, payment-db, notification-db
#   - Prometheus jobs: order-service, inventory-service, payment-service, notification-service
#   - Metric label for status-style counters is `status`; HTTP route label is `route`
#   - Payment rule: fails when total amount > 1000
#   - No jq/python needed: only bash, curl, awk, grep, sed
# ==============================================================================
set -uo pipefail   # deliberately NOT -e: keep going and report everything at the end

ORDER_URL_FALLBACK=http://localhost:9080   # only used if port discovery finds nothing
ORDER_URL=$ORDER_URL_FALLBACK
ORDER_URLS=()
INV_URL=http://localhost:8081
NOTIF_URL=http://localhost:8084
PROM=http://localhost:9090
GRAFANA=http://localhost:3000
RUN_OUTBOX_ALERT=${RUN_OUTBOX_ALERT:-0}
PASS=0
FAIL=0

# If the run is aborted mid-chaos, never leave Kafka / payment-service stopped.
cleanup() { docker compose start kafka payment-service >/dev/null 2>&1 || true; }
trap cleanup EXIT

# ------------------------------------------------------------------------------
# Assertion helpers
# ------------------------------------------------------------------------------
section() { printf '\n==============================================================================\n== %s\n==============================================================================\n' "$1"; }
pass() { echo "  PASS  $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL  $1   (expected: $2 | got: $3)"; FAIL=$((FAIL + 1)); }

expect_eq() { if [[ "$3" == "$2" ]]; then pass "$1"; else fail "$1" "$2" "$3"; fi; }
expect_ge() { if [[ "$3" =~ ^[0-9]+$ ]] && (( $3 >= $2 )); then pass "$1"; else fail "$1" ">= $2" "$3"; fi; }
expect_2xx() { if [[ "$2" =~ ^2 ]]; then pass "$1 (HTTP $2)"; else fail "$1" "HTTP 2xx" "HTTP $2"; fi; }
expect_approx() { # name expected actual
  if [[ -n "$3" ]] && awk -v a="$3" -v b="$2" 'BEGIN { d = a - b; if (d < 0) d = -d; exit !(d < 0.005) }'; then pass "$1"; else fail "$1" "~$2" "$3"; fi
}

# wait_for <description> <timeout_s> <command...>   (polls every 2s; replaces fixed sleeps)
wait_for() {
  local desc=$1 timeout=$2 end
  shift 2
  end=$((SECONDS + timeout))
  until "$@" >/dev/null 2>&1; do
    if (( SECONDS >= end )); then echo "  ....  timed out waiting for: $desc"; return 1; fi
    sleep 2
  done
}

# ------------------------------------------------------------------------------
# Docker / Postgres / Kafka helpers
# ------------------------------------------------------------------------------
db()     { docker exec -i "$1" psql -U wreker -d "$1" -tAc "$2" 2>/dev/null | tr -d '\r' | sed '/^$/d'; }
dbshow() { echo "-- $1: $2"; docker exec -i "$1" psql -U wreker -d "$1" -c "$2"; }

log_count()    { local out; out=$(docker compose logs "$1" 2>&1); grep -ciE "$2" <<<"$out"; }
log_has()      { local out; out=$(docker compose logs "$1" 2>&1); grep -qiE "$2" <<<"$out"; }
log_count_ge() { (( $(log_count "$1" "$2") >= $3 )); }

kproduce() { # topic message
  docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic "$1" <<<"$2" >/dev/null 2>&1
}
kconsume_all() { # topic
  docker exec -i kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "$1" --from-beginning --timeout-ms 5000 2>/dev/null
}
# Event IDs seen more than once on a topic (ignores the IDs this suite injects on purpose)
kafka_dup_event_ids() {
  kconsume_all "$1" | grep -o '"event_id":"[^"]*"' \
    | grep -vE '"event_id":"(order-1|inventory-999|poison-pill-1)"' | sort | uniq -d | wc -l | tr -d ' '
}
total_consumer_lag() {
  docker exec -i kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups 2>/dev/null \
    | awk '$6 ~ /^[0-9]+$/ { s += $6 } END { print s + 0 }'
}

replica_count() { docker compose ps --status running -q order-service | wc -l | tr -d ' '; }
replicas_is()   { [[ "$(replica_count)" == "$1" ]]; }

# Discover the host port each running order-service replica got; ORDER_URL = first replica.
refresh_order_urls() {
  local id port
  ORDER_URLS=()
  for id in $(docker compose ps -q order-service); do
    port=$(docker port "$id" | head -1 | grep -oE '[0-9]+$')
    [[ -n "$port" ]] && ORDER_URLS+=("http://localhost:$port")
  done
  if (( ${#ORDER_URLS[@]} == 0 )); then ORDER_URLS=("$ORDER_URL_FALLBACK"); ORDER_URL=$ORDER_URL_FALLBACK; return 1; fi
  ORDER_URL=${ORDER_URLS[0]}
}
order_ports_are() { refresh_order_urls && (( ${#ORDER_URLS[@]} == $1 )); }

# ------------------------------------------------------------------------------
# Domain helpers
# ------------------------------------------------------------------------------
post_order() { # customer product qty unit_price [base_url] -> prints HTTP status code
  curl -s -o /dev/null -w '%{http_code}' -X POST "${5:-$ORDER_URL}/orders" -H "Content-Type: application/json" \
    -d "{\"customer_id\":$1,\"items\":[{\"product_id\":$2,\"quantity\":$3,\"unit_price\":$4}]}"
}
# Fire N identical orders in parallel; prints how many responses were NOT 2xx
fire_concurrent() { # n qty unit_price
  local n=$1 qty=$2 price=$3 f i url
  f=$(mktemp)
  for ((i = 0; i < n; i++)); do
    url=${ORDER_URLS[$((i % ${#ORDER_URLS[@]}))]}   # round-robin across replica ports
    ( c=$(post_order 1 1 "$qty" "$price" "$url"); echo "$c" >> "$f" ) &
  done
  wait
  grep -vc '^2' "$f"
  rm -f "$f"
}

order_status()  { db order-db "SELECT status FROM orders WHERE id = $1;"; }
order_is()      { [[ "$(order_status "$1")" == "$2" ]]; }
last_order_id() { db order-db "SELECT coalesce(max(id),0) FROM orders;"; }
stock()         { db inventory-db "SELECT available_quantity FROM inventory WHERE product_id = 1;"; }
stock_is()      { [[ "$(stock)" == "$1" ]]; }
notif_count()   { db notification-db "SELECT count(*) FROM notifications WHERE order_id = $1;"; }
notif_ge()      { (( $(notif_count "$1") >= $2 )); }
range_confirmed() { [[ "$(db order-db "SELECT count(*) FROM orders WHERE id BETWEEN $1 AND $2 AND status = 'CONFIRMED';")" == "$3" ]]; }

outbox_unpub_zero() { [[ "$(db "$1" "SELECT count(*) FROM outbox WHERE aggregate_key = '$2' AND published = false;")" == "0" ]]; }
outbox_drained() { # db aggregate_key
  wait_for "$1 outbox drained for key $2" 30 outbox_unpub_zero "$1" "$2"
  expect_ge "$1: outbox has events for key $2" 1 "$(db "$1" "SELECT count(*) FROM outbox WHERE aggregate_key = '$2';")"
  expect_eq "$1: all outbox events for key $2 published" 0 "$(db "$1" "SELECT count(*) FROM outbox WHERE aggregate_key = '$2' AND published = false;")"
}

# ------------------------------------------------------------------------------
# Prometheus helpers
# ------------------------------------------------------------------------------
prom() { curl -sG --data-urlencode "query=$1" "$PROM/api/v1/query"; }
prom_val() { # first sample value of an instant query, or "none"
  local v
  v=$(prom "$1" | grep -oE '"value":\[[^]]*\]' | head -1 | sed -E 's/.*,"([^"]*)"\]$/\1/')
  echo "${v:-none}"
}
prom_check() { # query op value  (op: == >= > < <=)
  local v; v=$(prom_val "$1")
  [[ "$v" != "none" && "$v" != "NaN" ]] && awk -v a="$v" -v b="$3" "BEGIN { exit !(a + 0 $2 b + 0) }"
}
prom_show() { echo "-- PromQL: $1"; prom "$1"; echo; }
expect_prom() { # name timeout query op value
  local name=$1 timeout=$2 query=$3 op=$4 want=$5
  wait_for "$name" "$timeout" prom_check "$query" "$op" "$want"
  if prom_check "$query" "$op" "$want"; then pass "$name"; else fail "$name" "$op $want" "$(prom_val "$query")"; fi
}
expect_prom_none() { # name query  (series must NOT exist / be zero)
  local v; v=$(prom_val "$2")
  if [[ "$v" == "none" || "$v" == "0" ]]; then pass "$1"; else fail "$1" "no series" "$v"; fi
}

grafana_has_dashboard() { local out; out=$(curl -s "$GRAFANA/api/search"); grep -q "Platform Health" <<<"$out"; }

JOBS=(order-service inventory-service payment-service notification-service)

# ==============================================================================
# 1. FULL CLEAN REBUILD, BOOTSTRAP & OBSERVABILITY HEALTH CHECK
# ==============================================================================
section "1. CLEAN REBUILD, BOOTSTRAP & OBSERVABILITY HEALTH"
docker compose down -v
docker compose build --no-cache
docker compose up -d

for j in "${JOBS[@]}"; do
  expect_prom "Prometheus target healthy: $j" 120 "min(up{job=\"$j\"})" == 1
done
wait_for "order-service host port published" 60 refresh_order_urls
echo "  info  order-service endpoint(s): ${ORDER_URLS[*]}"
docker compose ps
docker compose logs kafka-init

for svc in order-service inventory-service payment-service; do
  wait_for "$svc consumer log" 60 log_has "$svc" "consumer starting"
  expect_ge "$svc consumer started" 1 "$(log_count "$svc" 'consumer starting')"
  expect_ge "$svc publisher started" 1 "$(log_count "$svc" 'publisher starting')"
done
wait_for "notification-service consumer log" 60 log_has notification-service "consumer starting"
expect_ge "notification-service consumer started" 1 "$(log_count notification-service 'consumer starting')"

wait_for "Grafana dashboard provisioned" 60 grafana_has_dashboard
if grafana_has_dashboard; then pass "Grafana dashboard 'Platform Health & Kafka Pipeline' provisioned"; else fail "Grafana dashboard provisioned" "Platform Health..." "missing"; fi

# ==============================================================================
# 2. SEED INITIAL INVENTORY
# ==============================================================================
section "2. SEED INVENTORY"
expect_2xx "seed product 1 (100 units)" "$(curl -s -o /dev/null -w '%{http_code}' -X POST "$INV_URL/inventory" -H "Content-Type: application/json" -d '{"product_id":1,"quantity":100}')"
wait_for "stock = 100" 15 stock_is 100
expect_eq "initial stock" 100 "$(stock)"

# ==============================================================================
# 3. PATH 1 (POSITIVE): Order 1 = 22 x 9.99 = 219.78 (< 1000)
#    Expect: CONFIRMED, stock 78, payment row, notification, all outboxes drained
# ==============================================================================
section "3. PATH 1: HAPPY PATH"
expect_2xx "create order 1" "$(post_order 1 1 22 9.99)"
wait_for "order 1 CONFIRMED" 45 order_is 1 CONFIRMED
expect_eq "order 1 status" CONFIRMED "$(order_status 1)"
expect_approx "order 1 total_amount" 219.78 "$(db order-db "SELECT total_amount FROM orders WHERE id = 1;")"
wait_for "stock = 78" 15 stock_is 78
expect_eq "stock after order 1" 78 "$(stock)"
expect_eq "payment row for order 1" 1 "$(db payment-db "SELECT count(*) FROM payments WHERE order_id = 1;")"
echo "  info  payment status: $(db payment-db "SELECT status FROM payments WHERE order_id = 1;")"
wait_for "notification for order 1" 30 notif_ge 1 1
expect_eq "notification rows for order 1" 1 "$(notif_count 1)"

for d in order-db inventory-db payment-db; do outbox_drained "$d" 1; done
for d in order-db inventory-db payment-db notification-db; do dbshow "$d" "SELECT * FROM processed_events;"; done

# ==============================================================================
# 4. PATH 2 (NEGATIVE): Order 2 = 500 units > 78 available
#    Expect: InventoryRejected -> CANCELLED, stock unchanged, cancellation notification
# ==============================================================================
section "4. PATH 2: INSUFFICIENT INVENTORY"
expect_2xx "create order 2" "$(post_order 1 1 500 5.00)"
wait_for "order 2 CANCELLED" 45 order_is 2 CANCELLED
expect_eq "order 2 status" CANCELLED "$(order_status 2)"
expect_eq "stock unchanged after rejection" 78 "$(stock)"
wait_for "notification for order 2" 30 notif_ge 2 1
expect_ge "cancellation notification for order 2" 1 "$(notif_count 2)"

# ==============================================================================
# 5. PATH 3 (NEGATIVE): Order 3 = 10 x 150.00 = 1500 (> 1000)
#    Flow: reserve (78->68) -> payment FAILED -> CANCELLED -> OrderCancelled(ReleaseInventory)
#          -> inventory compensates (68->78)
# ==============================================================================
section "5. PATH 3: PAYMENT FAILURE & SAGA COMPENSATION"
expect_2xx "create order 3" "$(post_order 1 1 10 150.00)"
wait_for "order 3 CANCELLED" 60 order_is 3 CANCELLED
expect_eq "order 3 status" CANCELLED "$(order_status 3)"
expect_eq "payment status for order 3" FAILED "$(db payment-db "SELECT status FROM payments WHERE order_id = 3;")"
wait_for "stock compensated to 78" 45 stock_is 78
expect_eq "stock restored by compensating transaction" 78 "$(stock)"
expect_ge "OrderCancelled event in order-db outbox" 1 "$(db order-db "SELECT count(*) FROM outbox WHERE aggregate_key = '3' AND event_type = 'OrderCancelled';")"
wait_for "notification for order 3" 30 notif_ge 3 1
expect_ge "cancellation notification for order 3" 1 "$(notif_count 3)"

# ==============================================================================
# 6. PATH 4 (NEGATIVE): IDEMPOTENCY & GUARDED STATE MACHINE
# ==============================================================================
section "6. PATH 4: IDEMPOTENCY & STALE-EVENT GUARD"
# A: duplicate OrderCreated (event_id "order-1")
kproduce order-events '{"event_id":"order-1","event_type":"OrderCreated","payload":{"order_id":1,"customer_id":1,"items":[{"product_id":1,"quantity":22,"unit_price":9.99}]}}'
wait_for "inventory idempotency log" 30 log_has inventory-service "idempotency"
expect_ge "inventory logged an idempotency skip" 1 "$(log_count inventory-service 'idempotency')"
expect_eq "stock NOT deducted twice" 78 "$(stock)"

# C: stale out-of-order InventoryReserved for already-CONFIRMED order 1
kproduce inventory-events '{"event_id":"inventory-999","event_type":"InventoryReserved","payload":{"order_id":1}}'
wait_for "order-service stale log" 30 log_has order-service "stale"
expect_ge "order-service rejected stale event" 1 "$(log_count order-service 'stale')"
expect_eq "order 1 still CONFIRMED" CONFIRMED "$(order_status 1)"

expect_prom "duplicate events counted (status=duplicate)" 45 'sum(kafka_events_processed_total{status="duplicate"})' '>=' 1

# ==============================================================================
# 7. PATH 5 (NEGATIVE): POISON PILL & DLQ
# ==============================================================================
section "7. PATH 5: POISON PILL & DEAD LETTER QUEUE"
kproduce inventory-events 'THIS_IS_CORRUPT_RAW_BYTES_NOT_JSON'
kproduce inventory-events '{"event_id":"poison-pill-1","event_type":"InventoryReserved","payload":"NOT_AN_OBJECT_CAUSES_RETRY_AND_DLQ"}'

wait_for "both poison pills routed to DLQ" 60 log_count_ge order-service "routed poison pill to DLQ" 2
expect_ge "order-service retry warnings" 1 "$(log_count order-service 'retrying message')"
expect_ge "order-service DLQ routings (Case A + Case B)" 2 "$(log_count order-service 'routed poison pill to DLQ')"

dlq_msgs=$(kconsume_all dead-letter-events)
echo "$dlq_msgs"
expect_ge "dead-letter-events topic holds quarantined messages" 2 "$(grep -c . <<<"$dlq_msgs")"
expect_prom "kafka_dlq_messages_total >= 2" 45 'sum(kafka_dlq_messages_total)' '>=' 2

# ==============================================================================
# 8. PATH 6 (CONCURRENCY): MULTI-REPLICA FOR UPDATE SKIP LOCKED
#    5 orders x 1 unit: 78 -> 73, zero duplicate publishes
#    NOTE: replicas stay at 3 until the very end so per-replica counters survive for section 13.
# ==============================================================================
section "8. PATH 6: MULTI-REPLICA CONCURRENCY"
docker compose up -d --scale order-service=3
wait_for "3 order-service replicas" 60 replicas_is 3
expect_eq "order-service replicas running (if FAIL: container_name or a fixed port blocks --scale)" 3 "$(replica_count)"
wait_for "3 distinct order-service ports" 30 order_ports_are 3
expect_eq "distinct order-service host ports discovered" 3 "${#ORDER_URLS[@]}"
echo "  info  order-service endpoints: ${ORDER_URLS[*]}"
docker compose ps

expect_eq "5 concurrent orders: non-2xx responses" 0 "$(fire_concurrent 5 1 5.00)"
wait_for "orders 4-8 CONFIRMED" 90 range_confirmed 4 8 5
expect_eq "orders 4-8 CONFIRMED" 5 "$(db order-db "SELECT count(*) FROM orders WHERE id BETWEEN 4 AND 8 AND status = 'CONFIRMED';")"
wait_for "stock = 73" 30 stock_is 73
expect_eq "stock dropped by exactly 5" 73 "$(stock)"
for d in order-db inventory-db payment-db; do outbox_drained "$d" 4; done
expect_eq "no event_id published twice on order-events" 0 "$(kafka_dup_event_ids order-events)"
expect_eq "no event_id published twice on inventory-events" 0 "$(kafka_dup_event_ids inventory-events)"
expect_prom "Prometheus discovered all 3 order-service replicas" 90 'count(up{job="order-service"} == 1)' == 3

# ==============================================================================
# 9. PATH 7 (INFRA CHAOS): KAFKA DOWN -> OUTBOX BACKLOG & SELF-HEAL
#    Watch http://localhost:3000 "Platform Health & Kafka Pipeline"
# ==============================================================================
section "9. PATH 7: KAFKA DOWN -> OUTBOX BACKLOG"
docker compose stop kafka
expect_2xx "order accepted while Kafka is down (ACID outbox)" "$(post_order 1 1 1 10.00)"
KAFKA_DOWN_ORDER=$(last_order_id)
echo "  info  order $KAFKA_DOWN_ORDER status while Kafka down: $(order_status "$KAFKA_DOWN_ORDER")"

expect_prom "outbox backlog >= 1" 45 'sum(outbox_unpublished_events_total)' '>=' 1
expect_prom "oldest unpublished outbox event > 10s" 60 'max(outbox_oldest_unpublished_age_seconds)' '>' 10
expect_prom "outbox publish failures recorded" 45 'sum(outbox_publish_failures_total)' '>=' 1

docker compose start kafka
expect_prom "outbox drained to 0 after Kafka returns" 150 'sum(outbox_unpublished_events_total) or vector(0)' == 0
expect_prom "oldest-unpublished age back to ~0" 60 'max(outbox_oldest_unpublished_age_seconds) or vector(0)' '<=' 5
wait_for "order $KAFKA_DOWN_ORDER CONFIRMED" 90 order_is "$KAFKA_DOWN_ORDER" CONFIRMED
expect_eq "order created during outage eventually CONFIRMED" CONFIRMED "$(order_status "$KAFKA_DOWN_ORDER")"

# ==============================================================================
# 10. RED METRICS LOAD BURST
# ==============================================================================
section "10. RED METRICS LOAD BURST"
bad=0
for i in {1..50}; do
  for url in "$ORDER_URL/orders?customer_id=1" "$INV_URL/inventory" "$NOTIF_URL/notifications/1"; do
    c=$(curl -s -o /dev/null -w '%{http_code}' "$url")
    [[ "$c" =~ ^2 ]] || bad=$((bad + 1))
  done
done
expect_eq "150 GET requests: non-2xx responses" 0 "$bad"
expect_prom "HTTP request rate > 0" 60 'sum(rate(http_requests_total[5m]))' '>' 0
expect_prom "db_connections reported" 30 'count(db_connections)' '>=' 1
prom_show 'db_connections'

# ==============================================================================
# 11. NOTIFICATION SERVICE IDEMPOTENCY: replay a real event_id, expect no new rows
# ==============================================================================
section "11. NOTIFICATION IDEMPOTENCY"
REPLAY_ID=$(db notification-db "SELECT event_id FROM processed_events ORDER BY processed_at ASC LIMIT 1;")
if [[ -z "$REPLAY_ID" ]]; then
  fail "found a processed event_id to replay" "non-empty" "empty"
else
  echo "  info  replaying event_id: $REPLAY_ID"
  notifs_before=$(notif_count 1)
  events_before=$(db notification-db "SELECT count(*) FROM processed_events;")
  kproduce order-events "{\"event_id\":\"${REPLAY_ID}\",\"event_type\":\"OrderConfirmed\",\"payload\":{\"order_id\":1}}"
  wait_for "notification idempotency log" 30 log_has notification-service "idempotency|duplicate"
  expect_ge "notification-service logged an idempotency skip" 1 "$(log_count notification-service 'idempotency|duplicate')"
  expect_eq "notification rows for order 1 unchanged" "$notifs_before" "$(notif_count 1)"
  expect_eq "processed_events rows unchanged" "$events_before" "$(db notification-db "SELECT count(*) FROM processed_events;")"
fi

# ==============================================================================
# 12. RETRY / DLQ METRICS, P95 LATENCY & CARDINALITY HYGIENE
# ==============================================================================
section "12. RETRY/DLQ METRICS, P95 LATENCY, CARDINALITY"
prom_show 'sum by (job) (kafka_events_processed_total{status="retry"})'
prom_show 'sum by (job) (kafka_events_processed_total{status="dlq"})'
expect_prom "retry attempts recorded (Case B = 3)" 45 'sum(kafka_events_processed_total{status="retry"})' '>=' 3
expect_prom "DLQ outcomes recorded (Case A + B)" 45 'sum(kafka_events_processed_total{status="dlq"})' '>=' 2
expect_prom "kafka_dlq_messages_total >= 2" 45 'sum(kafka_dlq_messages_total)' '>=' 2

P95_QUERY='histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket[10m])))'
prom_show 'histogram_quantile(0.95, sum by (job, le) (rate(http_request_duration_seconds_bucket[10m])))'
p95=$(prom_val "$P95_QUERY")
echo "  info  overall P95 latency: ${p95}s"
if [[ "$p95" != "none" && "$p95" != "NaN" ]]; then pass "P95 latency is computable"; else fail "P95 latency is computable" "a number" "$p95"; fi

expect_prom "route label is templated: /notifications/:id exists" 30 'count(http_requests_total{job="notification-service", route="/notifications/:id"})' '>=' 1
expect_prom_none "cardinality hygiene: no raw IDs in route labels (any service)" 'count(http_requests_total{route=~".*/[0-9]+.*"})'

# ==============================================================================
# 13. BUSINESS FUNNEL METRICS & DOMAIN OWNERSHIP  (orders 1-9 exist at this point)
#   created 9 | CONFIRMED 7 | CANCELLED 2 | inventory rejections 1 | payments OK 7, FAILED 1
#   stock = 100 - 22 - 0 - 10 + 10 - 5 - 1 = 72
#   (payment-service has NOT been restarted yet, so its in-memory counters are intact)
# ==============================================================================
section "13. BUSINESS FUNNEL METRICS"
expect_prom "orders_created_total = 9" 45 'sum(orders_created_total)' == 9
expect_prom "orders_final_status_total CONFIRMED = 7" 45 'sum(orders_final_status_total{status="CONFIRMED"})' == 7
expect_prom "orders_final_status_total CANCELLED = 2" 45 'sum(orders_final_status_total{status="CANCELLED"})' == 2
expect_prom "inventory_rejections_total = 1" 45 'sum(inventory_rejections_total)' == 1
expect_prom "payments_processed_total SUCCEEDED = 7" 45 'sum(payments_processed_total{status="SUCCEEDED"})' == 7
expect_prom "payments_processed_total FAILED = 1" 45 'sum(payments_processed_total{status="FAILED"})' == 1

expect_prom_none "orders_created_total only under order-service" 'count(orders_created_total{job!="order-service"})'
expect_prom_none "inventory_rejections_total only under inventory-service" 'count(inventory_rejections_total{job!="inventory-service"})'
expect_prom_none "payments_processed_total only under payment-service" 'count(payments_processed_total{job!="payment-service"})'

# Ground truth from the databases (metrics must agree with these)
expect_eq "DB: total orders" 9 "$(db order-db "SELECT count(*) FROM orders;")"
expect_eq "DB: CONFIRMED orders" 7 "$(db order-db "SELECT count(*) FROM orders WHERE status = 'CONFIRMED';")"
expect_eq "DB: CANCELLED orders" 2 "$(db order-db "SELECT count(*) FROM orders WHERE status = 'CANCELLED';")"
expect_eq "DB: FAILED payments" 1 "$(db payment-db "SELECT count(*) FROM payments WHERE status = 'FAILED';")"
expect_eq "DB: final stock for product 1" 72 "$(stock)"

# ==============================================================================
# 14. PROMETHEUS ALERT RULES & ALERT STATE
# ==============================================================================
section "14. ALERT RULES"
rules_json=$(curl -s "$PROM/api/v1/rules")
grep -oE '"name":"[^"]+"' <<<"$rules_json" | sort -u
expect_eq "alert rules loaded from alert_rules.yml" 5 "$(grep -o '"type":"alerting"' <<<"$rules_json" | wc -l | tr -d ' ')"
echo "-- currently active alerts:"; curl -s "$PROM/api/v1/alerts"; echo
# Uses the built-in ALERTS series so it still passes if the alert already resolved after Path 5
expect_prom "DLQMessageDetected fired after the poison pills" 90 'max(max_over_time(ALERTS{alertname="DLQMessageDetected",alertstate="firing"}[1h]))' '>=' 1

if [[ "$RUN_OUTBOX_ALERT" == "1" ]]; then
  echo "  info  RUN_OUTBOX_ALERT=1 -> testing OutboxStuck (threshold >30s + 10s 'for')"
  docker compose stop kafka
  stuck_code=$(post_order 1 1 1 5.00)
  expect_2xx "order accepted while Kafka down (alert test)" "$stuck_code"
  expect_prom "OutboxStuck alert fires" 150 'max(max_over_time(ALERTS{alertname="OutboxStuck",alertstate="firing"}[10m]))' '>=' 1
  docker compose start kafka
  expect_prom "outbox drained after alert test" 150 'sum(outbox_unpublished_events_total) or vector(0)' == 0
else
  echo "  skip  OutboxStuck alert test (set RUN_OUTBOX_ALERT=1 to run; adds ~3 min)"
fi

# ==============================================================================
# 15. PATH 8 (INFRA CHAOS): PAYMENT SERVICE DOWN -> CONSUMER LAG SPIKE
#     (runs AFTER the funnel check: restarting payment-service resets its counters)
# ==============================================================================
section "15. PATH 8: PAYMENT SERVICE DOWN -> CONSUMER LAG"
lag_baseline=$(total_consumer_lag)
echo "  info  baseline total consumer lag: $lag_baseline"
docker compose stop payment-service
expect_2xx "create order while payment-service is down" "$(post_order 1 1 1 10.00)"
PAY_DOWN_ORDER=$(last_order_id)
wait_for "order $PAY_DOWN_ORDER AWAITING_PAYMENT" 45 order_is "$PAY_DOWN_ORDER" AWAITING_PAYMENT
expect_eq "order waits in AWAITING_PAYMENT" AWAITING_PAYMENT "$(order_status "$PAY_DOWN_ORDER")"
lag_down=$(total_consumer_lag)
echo "  info  total consumer lag while payment-service down: $lag_down"
if [[ "$lag_down" =~ ^[0-9]+$ ]] && (( lag_down > lag_baseline )); then pass "consumer lag spiked above baseline"; else fail "consumer lag spiked above baseline" "> $lag_baseline" "$lag_down"; fi

docker compose start payment-service
wait_for "order $PAY_DOWN_ORDER CONFIRMED" 90 order_is "$PAY_DOWN_ORDER" CONFIRMED
expect_eq "order completes after payment-service returns" CONFIRMED "$(order_status "$PAY_DOWN_ORDER")"
lag_drained() { (( $(total_consumer_lag) <= lag_baseline )); }
wait_for "lag drained" 60 lag_drained
lag_after=$(total_consumer_lag)
if (( lag_after <= lag_baseline )); then pass "consumer lag drained back to baseline"; else fail "consumer lag drained" "<= $lag_baseline" "$lag_after"; fi

# ==============================================================================
# 16. OVERSELL: concurrent orders competing for the last units of stock
#     N orders x 20 units where N*20 > stock. Exactly floor(stock/20) may succeed; stock never < 0.
# ==============================================================================
section "16. OVERSELL / STOCK CONTENTION"
OS_QTY=20
OS_BASE=$(last_order_id)
OS_STOCK_BEFORE=$(stock)
OS_N=$(( OS_STOCK_BEFORE / OS_QTY + 3 ))
OS_EXPECT_OK=$(( OS_STOCK_BEFORE / OS_QTY ))
echo "  info  stock=$OS_STOCK_BEFORE, firing $OS_N orders x $OS_QTY units"

expect_eq "oversell burst: non-2xx responses" 0 "$(fire_concurrent "$OS_N" "$OS_QTY" 5.00)"
os_terminal() { [[ "$(db order-db "SELECT count(*) FROM orders WHERE id > $OS_BASE AND status IN ('CONFIRMED','CANCELLED');")" == "$OS_N" ]]; }
wait_for "all $OS_N oversell orders terminal" 120 os_terminal
os_ok=$(db order-db "SELECT count(*) FROM orders WHERE id > $OS_BASE AND status = 'CONFIRMED';")
expect_eq "exactly floor(stock/qty) orders confirmed" "$OS_EXPECT_OK" "$os_ok"
expect_eq "remaining orders cancelled" $(( OS_N - OS_EXPECT_OK )) "$(db order-db "SELECT count(*) FROM orders WHERE id > $OS_BASE AND status = 'CANCELLED';")"
os_stock_is() { stock_is $(( OS_STOCK_BEFORE - OS_QTY * os_ok )); }
wait_for "stock settled" 30 os_stock_is
expect_eq "stock = before - qty*confirmed (never negative)" $(( OS_STOCK_BEFORE - OS_QTY * os_ok )) "$(stock)"

# ==============================================================================
# 17. INPUT VALIDATION, UNKNOWN PRODUCT & PAYMENT BOUNDARY
# ==============================================================================
section "17. VALIDATION & BOUNDARY CASES"
expect_4xx() { # name json
  local code
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$ORDER_URL/orders" -H "Content-Type: application/json" -d "$2")
  if [[ "$code" =~ ^4 ]]; then pass "$1 (HTTP $code)"; else fail "$1" "HTTP 4xx" "HTTP $code"; fi
}
ids_before=$(last_order_id)
expect_4xx "malformed JSON rejected"   '{"customer_id":1,'
expect_4xx "empty items rejected"      '{"customer_id":1,"items":[]}'
expect_4xx "zero quantity rejected"    '{"customer_id":1,"items":[{"product_id":1,"quantity":0,"unit_price":5.00}]}'
expect_4xx "negative quantity rejected" '{"customer_id":1,"items":[{"product_id":1,"quantity":-3,"unit_price":5.00}]}'
expect_4xx "negative price rejected"   '{"customer_id":1,"items":[{"product_id":1,"quantity":1,"unit_price":-5.00}]}'
expect_eq "invalid requests created no orders" "$ids_before" "$(last_order_id)"

# Unknown product: API accepts, inventory must reject, order CANCELLED, stock untouched
stock_b=$(stock)
expect_2xx "create order for unknown product 9999" "$(post_order 1 9999 1 5.00)"
UNKNOWN_ORDER=$(last_order_id)
wait_for "unknown-product order CANCELLED" 60 order_is "$UNKNOWN_ORDER" CANCELLED
expect_eq "unknown-product order status" CANCELLED "$(order_status "$UNKNOWN_ORDER")"
expect_eq "stock untouched by unknown-product order" "$stock_b" "$(stock)"

# Payment threshold boundary (rule: amount > 1000 fails)
expect_2xx "create order totalling exactly 1000.00" "$(post_order 1 1 1 1000.00)"
BOUND_OK=$(last_order_id)
expect_2xx "create order totalling 1000.01" "$(post_order 1 1 1 1000.01)"
BOUND_FAIL=$(last_order_id)
wait_for "boundary order $BOUND_OK CONFIRMED" 60 order_is "$BOUND_OK" CONFIRMED
wait_for "boundary order $BOUND_FAIL CANCELLED" 60 order_is "$BOUND_FAIL" CANCELLED
expect_eq "total = 1000.00 is accepted" CONFIRMED "$(order_status "$BOUND_OK")"
expect_eq "total = 1000.01 is declined" CANCELLED "$(order_status "$BOUND_FAIL")"
expect_eq "net stock effect of boundary pair = -1 (declined order compensated)" $(( stock_b - 1 )) "$(stock)"

# ==============================================================================
# 18. FINAL LOG SWEEP
#     panic/fatal must be zero. Plain "error" lines are expected (poison pills, Kafka outage) -> info only.
# ==============================================================================
section "18. FINAL LOG SWEEP"
for svc in "${JOBS[@]}"; do
  out=$(docker compose logs "$svc" 2>&1)
  expect_eq "$svc: no panic/fatal in logs" 0 "$(grep -ciE 'panic|fatal' <<<"$out")"
  echo "  info  $svc: $(grep -ciE 'error|critical' <<<"$out") error/critical lines (review the last few below)"
  grep -iE 'error|critical' <<<"$out" | tail -3
done

# ==============================================================================
# 19. SCALE BACK DOWN TO CLEAN BASELINE
# ==============================================================================
section "19. SCALE BACK TO BASELINE"
docker compose up -d --scale order-service=1
wait_for "1 order-service replica" 60 replicas_is 1
expect_eq "order-service replicas after scale-down" 1 "$(replica_count)"
refresh_order_urls
echo "  info  order-service endpoint after scale-down: $ORDER_URL"

# ------------------------------------------------------------------------------
printf '\n==============================================================================\n'
echo "RESULT: $PASS passed, $FAIL failed"
echo "=============================================================================="
exit $(( FAIL > 0 ? 1 : 0 ))