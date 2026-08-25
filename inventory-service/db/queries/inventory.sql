-- name: CreateInventory :one
INSERT INTO inventory (
    product_id,
    available_quantity
)
VALUES ($1, $2)
RETURNING *;

-- name: GetInventoryByProductId :one
SELECT * 
FROM inventory
WHERE product_id = $1;

-- name: DecrementInventory :execrows
UPDATE inventory
SET available_quantity = available_quantity - $2
WHERE product_id = $1
AND available_quantity >= $2;

-- name: IncrementInventory :exec
UPDATE inventory
SET available_quantity = available_quantity + $2
WHERE product_id = $1;