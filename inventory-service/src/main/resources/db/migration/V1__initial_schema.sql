CREATE TABLE inventory (
    product_id VARCHAR(255) PRIMARY KEY,
    available_quantity INTEGER NOT NULL,
    reserved_quantity INTEGER NOT NULL,
    CONSTRAINT chk_inventory_available_non_negative CHECK (available_quantity >= 0),
    CONSTRAINT chk_inventory_reserved_non_negative CHECK (reserved_quantity >= 0)
);

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
