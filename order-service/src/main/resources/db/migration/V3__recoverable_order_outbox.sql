ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS attempt_count INTEGER;
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMP(6);
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS last_error VARCHAR(1024);
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS claim_token VARCHAR(255);
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS claimed_by VARCHAR(255);
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS claimed_until TIMESTAMP(6);

UPDATE outbox_events
SET attempt_count = 0
WHERE attempt_count IS NULL;

UPDATE outbox_events
SET next_attempt_at = created_at
WHERE next_attempt_at IS NULL
  AND status IN ('PENDING', 'FAILED');

UPDATE outbox_events
SET next_attempt_at = COALESCE(sent_at, created_at)
WHERE next_attempt_at IS NULL
  AND status = 'SENT';

ALTER TABLE outbox_events ALTER COLUMN attempt_count SET NOT NULL;
ALTER TABLE outbox_events ALTER COLUMN next_attempt_at SET NOT NULL;

ALTER TABLE outbox_events DROP CONSTRAINT IF EXISTS chk_outbox_events_status;
ALTER TABLE outbox_events ADD CONSTRAINT chk_outbox_events_status
    CHECK (status IN ('PENDING', 'IN_PROGRESS', 'SENT', 'FAILED'));

ALTER TABLE outbox_events ADD CONSTRAINT chk_outbox_events_attempt_count_non_negative
    CHECK (attempt_count >= 0);

CREATE INDEX IF NOT EXISTS idx_outbox_events_claim_available
    ON outbox_events (status, next_attempt_at, created_at, id);

CREATE INDEX IF NOT EXISTS idx_outbox_events_claim_expired
    ON outbox_events (status, claimed_until, created_at, id);

CREATE INDEX IF NOT EXISTS idx_outbox_events_claim_token
    ON outbox_events (claim_token)
    WHERE claim_token IS NOT NULL;
