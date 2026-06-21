CREATE TABLE inventory_inbox_events (
    event_id VARCHAR(255) PRIMARY KEY,
    event_type VARCHAR(255) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,
    received_at TIMESTAMP(6) NOT NULL
);

CREATE TABLE inventory_reservation_decisions (
    order_id VARCHAR(255) PRIMARY KEY,
    source_event_id VARCHAR(255) NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    status VARCHAR(32) NOT NULL,
    failure_reason VARCHAR(512),
    response_event_id VARCHAR(255) NOT NULL,
    decided_at TIMESTAMP(6) NOT NULL,
    CONSTRAINT uk_inventory_decisions_source_event UNIQUE (source_event_id),
    CONSTRAINT uk_inventory_decisions_response_event UNIQUE (response_event_id),
    CONSTRAINT chk_inventory_decision_quantity_positive CHECK (quantity > 0),
    CONSTRAINT chk_inventory_decision_status CHECK (status IN ('RESERVED', 'FAILED'))
);

CREATE TABLE inventory_outbox_events (
    id VARCHAR(255) PRIMARY KEY,
    aggregate_id VARCHAR(255) NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload TEXT NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL,
    sent_at TIMESTAMP(6),
    attempt_count INTEGER NOT NULL,
    next_attempt_at TIMESTAMP(6) NOT NULL,
    last_error VARCHAR(1024),
    claim_token VARCHAR(255),
    claimed_by VARCHAR(255),
    claimed_until TIMESTAMP(6),
    CONSTRAINT chk_inventory_outbox_status CHECK (status IN ('PENDING', 'IN_PROGRESS', 'SENT', 'FAILED')),
    CONSTRAINT chk_inventory_outbox_attempt_count_non_negative CHECK (attempt_count >= 0)
);

CREATE INDEX idx_inventory_inbox_aggregate ON inventory_inbox_events (aggregate_id);
CREATE INDEX idx_inventory_decisions_product ON inventory_reservation_decisions (product_id);
CREATE INDEX idx_inventory_outbox_claim_available
    ON inventory_outbox_events (status, next_attempt_at, created_at, id);
CREATE INDEX idx_inventory_outbox_claim_expired
    ON inventory_outbox_events (status, claimed_until, created_at, id);
CREATE INDEX idx_inventory_outbox_claim_token
    ON inventory_outbox_events (claim_token)
    WHERE claim_token IS NOT NULL;
