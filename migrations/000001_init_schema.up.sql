-- Migrations UP: Initial Schema for Jungle Gaming Distributed Betting Challenge

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- 1. Wallets
CREATE TABLE IF NOT EXISTS wallets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    player_id VARCHAR(255) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    balance BIGINT NOT NULL CHECK (balance >= 0),
    version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uk_wallets_player_currency UNIQUE (player_id, currency)
);

CREATE INDEX IF NOT EXISTS idx_wallets_player_id ON wallets(player_id);

-- 2. Wager Transactions
CREATE TABLE IF NOT EXISTS wager_transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    origin VARCHAR(20) NOT NULL CHECK (origin IN ('INTERNAL', 'EXTERNAL')),
    external_id VARCHAR(255),
    provider_id VARCHAR(255), -- Nullable para operações internas (ex: OPENING)
    idempotency_key VARCHAR(255),
    payload_hash VARCHAR(64),
    wallet_id UUID NOT NULL REFERENCES wallets(id),
    player_id VARCHAR(255) NOT NULL,
    round_id VARCHAR(255),
    game_id VARCHAR(255),
    type VARCHAR(50) NOT NULL CHECK (type IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    amount BIGINT NOT NULL,
    currency VARCHAR(3) NOT NULL,
    status VARCHAR(50) NOT NULL CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    reference_id UUID REFERENCES wager_transactions(id),
    external_reference_id VARCHAR(255),
    error_code VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uk_wager_tx_provider_external UNIQUE (provider_id, external_id),
    CONSTRAINT chk_wager_origin_external CHECK (
        origin = 'INTERNAL' OR (provider_id IS NOT NULL AND external_id IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_wager_tx_idempotency ON wager_transactions(idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_wager_tx_wallet_created ON wager_transactions(wallet_id, created_at);
CREATE INDEX IF NOT EXISTS idx_wager_tx_status ON wager_transactions(status) WHERE status IN ('PENDING', 'PENDING_REFERENCE');
CREATE INDEX IF NOT EXISTS idx_wager_tx_external_ref ON wager_transactions(provider_id, external_reference_id) WHERE external_reference_id IS NOT NULL;

-- 3. Wallet Ledger Entries (Append-only)
CREATE TABLE IF NOT EXISTS wallet_ledger_entries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wallet_id UUID NOT NULL REFERENCES wallets(id),
    transaction_id UUID NOT NULL REFERENCES wager_transactions(id),
    type VARCHAR(20) NOT NULL CHECK (type IN ('DEBIT', 'CREDIT')),
    amount BIGINT NOT NULL CHECK (amount >= 0),
    balance_before BIGINT NOT NULL CHECK (balance_before >= 0),
    balance_after BIGINT NOT NULL CHECK (balance_after >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uk_ledger_wallet_tx UNIQUE (wallet_id, transaction_id),
    CONSTRAINT chk_ledger_balance_math CHECK (
        (type = 'CREDIT' AND balance_after = balance_before + amount) OR
        (type = 'DEBIT' AND balance_after = balance_before - amount)
    )
);

CREATE INDEX IF NOT EXISTS idx_ledger_wallet_created ON wallet_ledger_entries(wallet_id, created_at);

-- Trigger de Imutabilidade para wallet_ledger_entries (bloqueia UPDATE e DELETE)
CREATE OR REPLACE FUNCTION prevent_ledger_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries is append-only: updates and deletes are forbidden'
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_ledger_mutation ON wallet_ledger_entries;
CREATE TRIGGER trg_prevent_ledger_mutation
BEFORE UPDATE OR DELETE ON wallet_ledger_entries
FOR EACH ROW
EXECUTE FUNCTION prevent_ledger_mutation();

-- 4. Idempotency Records
CREATE TABLE IF NOT EXISTS idempotency_records (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key VARCHAR(255) NOT NULL UNIQUE,
    payload_hash VARCHAR(64) NOT NULL,
    response_status INT NOT NULL,
    response_body JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 5. Outbox Events
CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED')),
    retry_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_status_created ON outbox_events(status, created_at);

-- 6. Inbox Messages
CREATE TABLE IF NOT EXISTS inbox_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id VARCHAR(255) NOT NULL UNIQUE,
    source VARCHAR(100) NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
