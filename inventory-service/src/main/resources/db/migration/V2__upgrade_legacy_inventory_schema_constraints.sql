DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM inventory
        WHERE product_id IS NULL OR available_quantity IS NULL OR reserved_quantity IS NULL
    ) THEN
        RAISE EXCEPTION 'Cannot enforce NOT NULL constraints on inventory: existing rows contain null values.';
    END IF;

    IF EXISTS (
        SELECT 1 FROM inventory
        WHERE available_quantity < 0 OR reserved_quantity < 0
    ) THEN
        RAISE EXCEPTION 'Cannot add inventory quantity constraints: existing rows contain negative quantities.';
    END IF;
END $$;

ALTER TABLE inventory ALTER COLUMN available_quantity SET NOT NULL;
ALTER TABLE inventory ALTER COLUMN reserved_quantity SET NOT NULL;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_inventory_available_non_negative') THEN
        ALTER TABLE inventory ADD CONSTRAINT chk_inventory_available_non_negative CHECK (available_quantity >= 0);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_inventory_reserved_non_negative') THEN
        ALTER TABLE inventory ADD CONSTRAINT chk_inventory_reserved_non_negative CHECK (reserved_quantity >= 0);
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
