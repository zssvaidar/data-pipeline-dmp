-- Transactional store: the orders table written by the API.
CREATE TABLE IF NOT EXISTS orders (
    order_id    TEXT PRIMARY KEY,
    customer_id TEXT           NOT NULL,
    amount      NUMERIC(12, 2) NOT NULL CHECK (amount > 0),
    created_at  TIMESTAMPTZ    NOT NULL
);
CREATE INDEX IF NOT EXISTS orders_customer_id_idx ON orders (customer_id);
