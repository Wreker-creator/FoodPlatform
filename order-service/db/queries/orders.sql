-- name: CreateOrder :one
INSERT INTO orders (
    customer_id,
    status,
    total_amount
)
VALUES ($1, $2, $3)
RETURNING *;

-- name: CreateOrderItem :one
INSERT INTO order_items (
    order_id,
    product_id,
    quantity,
    unit_price
)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: GetOrderItemsByOrderId :many
SELECT *
FROM order_items
WHERE order_id = $1;

-- name: GetOrderByID :one
SELECT *
FROM orders
WHERE id = $1;


-- name: UpdateOrderStatus :exec
UPDATE orders
SET status = $2
WHERE id = $1;


-- name: ListOrdersByCustomer :many
SELECT *
FROM orders
WHERE customer_id = $1
ORDER BY created_at DESC;