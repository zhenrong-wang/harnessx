ALTER TABLE harness_core.event_outbox
    ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz NOT NULL DEFAULT now();

DROP INDEX IF EXISTS harness_core.event_outbox_pending;
CREATE INDEX event_outbox_pending
    ON harness_core.event_outbox (next_attempt_at, occurred_at, id)
    WHERE delivered_at IS NULL;
