-- Migration 000003: Add inbox received_at and consumer uniqueness
ALTER TABLE inbox_messages ADD COLUMN IF NOT EXISTS received_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE inbox_messages ADD COLUMN IF NOT EXISTS consumer_name VARCHAR(100) NOT NULL DEFAULT 'wager-consumer';

-- Atualiza unicidade para (consumer_name, message_id) conforme Seção 6.5 do desafio
ALTER TABLE inbox_messages DROP CONSTRAINT IF EXISTS inbox_messages_message_id_key;
CREATE UNIQUE INDEX IF NOT EXISTS uk_inbox_consumer_message ON inbox_messages(consumer_name, message_id);
