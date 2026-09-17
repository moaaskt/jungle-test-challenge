-- Migration 000004: Suporte ao worker de resolução de referências pendentes (Seção 7 do desafio)

-- Índice para busca eficiente de PENDING_REFERENCE pelo par (provider, external_reference_id)
-- Usado na resolução inline (Momento A) e no worker periódico (Momento B)
CREATE INDEX IF NOT EXISTS idx_wager_tx_pending_ref
  ON wager_transactions(provider_id, external_reference_id)
  WHERE status = 'PENDING_REFERENCE';

-- Colunas para rastrear tentativas e agendamento do worker de resolução
ALTER TABLE wager_transactions
  ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;

-- Índice para o worker varrer pendências elegíveis por next_attempt_at
CREATE INDEX IF NOT EXISTS idx_wager_tx_pending_attempt
  ON wager_transactions(next_attempt_at)
  WHERE status = 'PENDING_REFERENCE' AND next_attempt_at IS NOT NULL;
