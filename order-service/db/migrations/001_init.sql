
CREATE TABLE orders(
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    status VARCHAR(50) NOT NULL CHECK (status IN ('PENDING','AWAITING_PAYMENT','CONFIRMED','CANCELLED')),
    total_amount DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_items(
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES orders(id),
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- this function is called by the trigger on every UPDATE.
-- NEW refers to the row with the incoming updated values.
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW; -- RETURN NEW is required — it tells Postgres to proceed with the update
END;
$$ LANGUAGE plpgsql;

-- the trigger fires BEFORE each row UPDATE so updated_at is set before the row is written.
CREATE TRIGGER set_updated_at
BEFORE UPDATE ON orders
FOR EACH ROW
EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER set_updated_at
BEFORE UPDATE ON order_items
FOR EACH ROW
EXECUTE FUNCTION update_updated_at();