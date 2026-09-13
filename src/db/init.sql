-- Схема базы заказов.

CREATE TABLE IF NOT EXISTS orders (
    id          BIGSERIAL PRIMARY KEY,
    order_id    TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    amount      NUMERIC(12, 2) NOT NULL,
    items       JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS orders_order_id_idx ON orders (order_id);
CREATE INDEX IF NOT EXISTS orders_created_at_idx ON orders (created_at DESC);
