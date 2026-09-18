-- name: InsertOutboxEvent :one
INSERT INTO outbox (aggregate_key, event_type, payload)
VALUES ($1, $2, $3)
RETURNING *;

-- name: GetUnpublishedOutboxEvents :many
SELECT * FROM outbox WHERE published = FALSE ORDER BY created_at ASC;

-- name: MarkOutboxEventPublished :exec
UPDATE outbox SET published = TRUE WHERE id = $1;