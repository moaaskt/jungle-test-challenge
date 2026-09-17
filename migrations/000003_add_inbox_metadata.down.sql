-- Migration 000003: Revert inbox metadata and consumer uniqueness
DROP INDEX IF EXISTS uk_inbox_consumer_message;
ALTER TABLE inbox_messages ADD CONSTRAINT inbox_messages_message_id_key UNIQUE (message_id);
ALTER TABLE inbox_messages DROP COLUMN IF EXISTS consumer_name;
ALTER TABLE inbox_messages DROP COLUMN IF EXISTS received_at;
