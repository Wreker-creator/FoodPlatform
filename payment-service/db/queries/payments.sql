-- name: CreatePayment :one
INSERT INTO payments(
    order_id,
    status,
    amount,
    payment_mode
)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: GetPaymentByID :one
SELECT * FROM payments WHERE id = $1;

-- name: GetPaymentsByOrderID :many
SELECT * FROM payments WHERE order_id = $1;

-- name: UpdatePaymentStatus :one
UPDATE payments
SET status = $2
WHERE id = $1
RETURNING *;