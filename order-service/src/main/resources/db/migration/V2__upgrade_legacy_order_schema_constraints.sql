DO $$
DECLARE
    price_type text;
BEGIN
    SELECT data_type INTO price_type
    FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = 'orders'
      AND column_name = 'price';

    IF price_type IN ('double precision', 'real') THEN
        IF EXISTS (
            SELECT 1 FROM orders
            WHERE price IS NULL
               OR price <= 0
               OR price > 99999999999999999.99
               OR abs(price::numeric - round(price::numeric, 2)) > 0.0000001
        ) THEN
            RAISE EXCEPTION 'Cannot migrate orders.price to numeric(19,2): values must be non-null, positive, within numeric(19,2), and have no more than two decimal places. Back up the database and resolve incompatible rows explicitly.';
        END IF;

        ALTER TABLE orders
            ALTER COLUMN price TYPE NUMERIC(19, 2) USING round(price::numeric, 2);
    ELSIF price_type = 'numeric' THEN
        ALTER TABLE orders
            ALTER COLUMN price TYPE NUMERIC(19, 2);
    END IF;

    IF EXISTS (
        SELECT 1 FROM orders
        WHERE order_id IS NULL OR product_id IS NULL OR customer_id IS NULL
           OR quantity IS NULL OR price IS NULL OR status IS NULL
           OR created_at IS NULL OR updated_at IS NULL
    ) THEN
        RAISE EXCEPTION 'Cannot enforce NOT NULL constraints on orders: existing rows contain null values.';
    END IF;

    IF EXISTS (SELECT 1 FROM orders WHERE quantity <= 0) THEN
        RAISE EXCEPTION 'Cannot add orders quantity constraint: existing rows contain non-positive quantity.';
    END IF;

    IF EXISTS (SELECT 1 FROM orders WHERE price <= 0) THEN
        RAISE EXCEPTION 'Cannot add orders price constraint: existing rows contain non-positive price.';
    END IF;

    IF EXISTS (
        SELECT 1 FROM orders
        WHERE status NOT IN ('PENDING', 'CONFIRMED', 'CANCELLED', 'INVENTORY_RESERVED', 'INVENTORY_FAILED')
    ) THEN
        RAISE EXCEPTION 'Cannot add orders status constraint: existing rows contain unsupported status values.';
    END IF;
END $$;

ALTER TABLE orders ALTER COLUMN product_id SET NOT NULL;
ALTER TABLE orders ALTER COLUMN customer_id SET NOT NULL;
ALTER TABLE orders ALTER COLUMN quantity SET NOT NULL;
ALTER TABLE orders ALTER COLUMN price SET NOT NULL;
ALTER TABLE orders ALTER COLUMN status SET NOT NULL;
ALTER TABLE orders ALTER COLUMN created_at SET NOT NULL;
ALTER TABLE orders ALTER COLUMN updated_at SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_orders_quantity_positive') THEN
        ALTER TABLE orders ADD CONSTRAINT chk_orders_quantity_positive CHECK (quantity > 0);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_orders_price_positive') THEN
        ALTER TABLE orders ADD CONSTRAINT chk_orders_price_positive CHECK (price > 0);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_orders_status') THEN
        ALTER TABLE orders ADD CONSTRAINT chk_orders_status CHECK (
            status IN ('PENDING', 'CONFIRMED', 'CANCELLED', 'INVENTORY_RESERVED', 'INVENTORY_FAILED')
        );
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM outbox_events
        WHERE id IS NULL OR aggregate_id IS NULL OR aggregate_type IS NULL
           OR event_type IS NULL OR payload IS NULL OR status IS NULL OR created_at IS NULL
    ) THEN
        RAISE EXCEPTION 'Cannot enforce NOT NULL constraints on outbox_events: existing rows contain null values.';
    END IF;

    IF EXISTS (SELECT 1 FROM outbox_events WHERE status NOT IN ('PENDING', 'SENT', 'FAILED')) THEN
        RAISE EXCEPTION 'Cannot add outbox status constraint: existing rows contain unsupported status values.';
    END IF;
END $$;

ALTER TABLE outbox_events ALTER COLUMN aggregate_id SET NOT NULL;
ALTER TABLE outbox_events ALTER COLUMN aggregate_type SET NOT NULL;
ALTER TABLE outbox_events ALTER COLUMN event_type SET NOT NULL;
ALTER TABLE outbox_events ALTER COLUMN payload SET NOT NULL;
ALTER TABLE outbox_events ALTER COLUMN status SET NOT NULL;
ALTER TABLE outbox_events ALTER COLUMN created_at SET NOT NULL;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_outbox_events_status') THEN
        ALTER TABLE outbox_events ADD CONSTRAINT chk_outbox_events_status CHECK (status IN ('PENDING', 'SENT', 'FAILED'));
    END IF;
END $$;
DO $$
BEGIN
    IF to_regclass('idx_outbox_events_status') IS NULL THEN
        CREATE INDEX idx_outbox_events_status ON outbox_events (status);
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM event_store
        WHERE id IS NULL OR aggregate_id IS NULL OR aggregate_type IS NULL
           OR event_type IS NULL OR payload IS NULL OR version IS NULL OR occurred_at IS NULL
    ) THEN
        RAISE EXCEPTION 'Cannot enforce NOT NULL constraints on event_store: existing rows contain null values.';
    END IF;

    IF EXISTS (SELECT 1 FROM event_store WHERE version <= 0) THEN
        RAISE EXCEPTION 'Cannot add event_store version constraint: existing rows contain non-positive version.';
    END IF;
END $$;

ALTER TABLE event_store ALTER COLUMN aggregate_id SET NOT NULL;
ALTER TABLE event_store ALTER COLUMN aggregate_type SET NOT NULL;
ALTER TABLE event_store ALTER COLUMN event_type SET NOT NULL;
ALTER TABLE event_store ALTER COLUMN payload SET NOT NULL;
ALTER TABLE event_store ALTER COLUMN version SET NOT NULL;
ALTER TABLE event_store ALTER COLUMN occurred_at SET NOT NULL;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_event_store_version_positive') THEN
        ALTER TABLE event_store ADD CONSTRAINT chk_event_store_version_positive CHECK (version > 0);
    END IF;
END $$;
DO $$
BEGIN
    IF to_regclass('idx_event_store_aggregate_version') IS NULL THEN
        CREATE INDEX idx_event_store_aggregate_version ON event_store (aggregate_id, version);
    END IF;
END $$;
