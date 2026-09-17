-- Migration 000002: Revert outbox leasing and retry scheduling columns
DROP INDEX IF EXISTS idx_outbox_pending;
ALTER TABLE outbox_events DROP COLUMN IF EXISTS last_error;
ALTER TABLE outbox_events DROP COLUMN IF EXISTS locked_until;
ALTER TABLE outbox_events DROP COLUMN IF EXISTS next_attempt_at;
