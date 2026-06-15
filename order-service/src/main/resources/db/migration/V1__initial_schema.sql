CREATE TABLE orders (
    order_id VARCHAR(255) PRIMARY KEY,
    product_id VARCHAR(255) NOT NULL,
    customer_id VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    price NUMERIC(19, 2) NOT NULL,
    status VARCHAR(255) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL,
    updated_at TIMESTAMP(6) NOT NULL,
    CONSTRAINT chk_orders_quantity_positive CHECK (quantity > 0),
    CONSTRAINT chk_orders_price_positive CHECK (price > 0),
    CONSTRAINT chk_orders_status CHECK (
        status IN ('PENDING', 'CONFIRMED', 'CANCELLED', 'INVENTORY_RESERVED', 'INVENTORY_FAILED')
    )
);

CREATE TABLE outbox_events (
    id VARCHAR(255) PRIMARY KEY,
    aggregate_id VARCHAR(255) NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload TEXT NOT NULL,
    status VARCHAR(255) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL,
    sent_at TIMESTAMP(6),
    CONSTRAINT chk_outbox_events_status CHECK (status IN ('PENDING', 'SENT', 'FAILED'))
);

CREATE INDEX idx_outbox_events_status ON outbox_events (status);

CREATE TABLE event_store (
    id VARCHAR(255) PRIMARY KEY,
    aggregate_id VARCHAR(255) NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload TEXT NOT NULL,
    version INTEGER NOT NULL,
    occurred_at TIMESTAMP(6) NOT NULL,
    CONSTRAINT chk_event_store_version_positive CHECK (version > 0)
);

CREATE INDEX idx_event_store_aggregate_version ON event_store (aggregate_id, version);
