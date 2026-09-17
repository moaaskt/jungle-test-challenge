-- Rollback Migration 000004: Remover suporte ao worker de resolução

DROP INDEX IF EXISTS idx_wager_tx_pending_ref;
DROP INDEX IF EXISTS idx_wager_tx_pending_attempt;

ALTER TABLE wager_transactions
  DROP COLUMN IF EXISTS attempts,
  DROP COLUMN IF EXISTS next_attempt_at;
