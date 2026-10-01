-- name: CreateNotification :one
INSERT INTO notifications (order_id, message)
VALUES ($1, $2) RETURNING *;

-- name: GetNotificationsByOrderId :many
SELECT * FROM notifications WHERE order_id = $1 ORDER BY created_at DESC;