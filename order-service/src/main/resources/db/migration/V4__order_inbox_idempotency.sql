CREATE TABLE IF NOT EXISTS order_inbox_events (
    event_id VARCHAR(255) PRIMARY KEY,
    event_type VARCHAR(255) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,
    received_at TIMESTAMP(6) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_order_inbox_aggregate
    ON order_inbox_events (aggregate_id);

CREATE TABLE IF NOT EXISTS order_idempotency_keys (
    idempotency_key VARCHAR(255) PRIMARY KEY,
    endpoint VARCHAR(255) NOT NULL,
    request_fingerprint VARCHAR(64) NOT NULL,
    fingerprint_version INTEGER NOT NULL,
    order_id VARCHAR(255),
    response_status INTEGER,
    response_location VARCHAR(512),
    response_body TEXT,
    created_at TIMESTAMP(6) NOT NULL,
    completed_at TIMESTAMP(6)
);

CREATE INDEX IF NOT EXISTS idx_order_idempotency_order
    ON order_idempotency_keys (order_id)
    WHERE order_id IS NOT NULL;
