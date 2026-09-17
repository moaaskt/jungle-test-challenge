package domain_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
	"github.com/moaaskt/jungle-test-challenge/pkg/money"
)

func TestOutbox_WagerTransactionProcessed(t *testing.T) {
	walletID := uuid.New()
	playerID := "player-123"
	provID := "provider-abc"
	extID := "ext-tx-999"
	amt, err := money.New(2500, "BRL")
	if err != nil {
		t.Fatalf("unexpected money error: %v", err)
	}

	tx, err := domain.NewWagerTransaction(
		domain.OriginExternal,
		walletID,
		playerID,
		&provID,
		&extID,
		domain.TransactionTypeBet,
		amt,
		"BRL",
	)
	if err != nil {
		t.Fatalf("unexpected tx error: %v", err)
	}
	tx.Process()

	correlationID := "idem-key-777"
	var causationID *string = nil // Regra da Fase 6: nil

	outboxEv, err := domain.NewWagerTransactionProcessedOutboxEvent(&tx, correlationID, causationID)
	if err != nil {
		t.Fatalf("failed to create outbox event: %v", err)
	}

	if outboxEv.EventType != domain.EventTypeWagerTransactionProcessed {
		t.Errorf("expected event type %s, got %s", domain.EventTypeWagerTransactionProcessed, outboxEv.EventType)
	}
	if outboxEv.Status != domain.OutboxStatusPending {
		t.Errorf("expected status PENDING, got %s", outboxEv.Status)
	}
	if outboxEv.RetryCount != 0 {
		t.Errorf("expected retry count 0, got %d", outboxEv.RetryCount)
	}

	// Verificar o Envelope
	var envelope domain.EventEnvelope
	if err := json.Unmarshal(outboxEv.Payload, &envelope); err != nil {
		t.Fatalf("failed to unmarshal payload: %v", err)
	}

	if envelope.EventID != outboxEv.ID {
		t.Errorf("envelope eventId %s != outbox event id %s", envelope.EventID, outboxEv.ID)
	}
	if envelope.EventType != domain.EventTypeWagerTransactionProcessed {
		t.Errorf("envelope eventType mismatch: %s", envelope.EventType)
	}
	if envelope.CorrelationID != correlationID {
		t.Errorf("expected correlationId %s, got %s", correlationID, envelope.CorrelationID)
	}
	if envelope.CausationID != nil {
		t.Errorf("expected causationId nil, got %v", envelope.CausationID)
	}
	if envelope.Version != 1 {
		t.Errorf("expected version 1, got %d", envelope.Version)
	}

	// Valida parsing do timestamp RFC 3339
	if _, err := time.Parse(time.RFC3339Nano, envelope.OccurredAt); err != nil {
		t.Errorf("invalid occurredAt RFC 3339 format: %v", err)
	}

	// Valida dados específicos
	var data domain.WagerTransactionProcessedData
	if err := json.Unmarshal(envelope.Data, &data); err != nil {
		t.Fatalf("failed to unmarshal event data: %v", err)
	}

	if data.Amount != "25.00" {
		t.Errorf("expected amount '25.00', got '%s'", data.Amount)
	}
	if data.Currency != "BRL" {
		t.Errorf("expected currency 'BRL', got '%s'", data.Currency)
	}
	if data.Status != "PROCESSED" {
		t.Errorf("expected status PROCESSED, got '%s'", data.Status)
	}
}

func TestOutbox_WalletBalanceChanged(t *testing.T) {
	walletID := uuid.New()
	txID := uuid.New()
	m, _ := money.New(8000, "BRL")
	before, _ := money.New(10000, "BRL")
	after, _ := money.New(2000, "BRL")

	correlationID := "idem-key-balance-change"
	var causationID *string = nil

	outboxEv, err := domain.NewWalletBalanceChangedOutboxEvent(
		walletID,
		txID,
		"DEBIT",
		m,
		before,
		after,
		2,
		correlationID,
		causationID,
	)
	if err != nil {
		t.Fatalf("failed to create wallet balance changed event: %v", err)
	}

	var envelope domain.EventEnvelope
	if err := json.Unmarshal(outboxEv.Payload, &envelope); err != nil {
		t.Fatalf("failed to unmarshal envelope: %v", err)
	}

	if envelope.AggregateID != walletID.String() {
		t.Errorf("expected aggregateId %s, got %s", walletID.String(), envelope.AggregateID)
	}

	var data domain.WalletBalanceChangedData
	if err := json.Unmarshal(envelope.Data, &data); err != nil {
		t.Fatalf("failed to unmarshal data: %v", err)
	}

	if data.WalletID != walletID || data.TransactionID != txID {
		t.Errorf("IDs mismatch in data")
	}
	if data.Direction != "DEBIT" {
		t.Errorf("expected DEBIT, got %s", data.Direction)
	}
	if data.Money.Amount != "80.00" || data.Money.Currency != "BRL" {
		t.Errorf("unexpected money payload: %+v", data.Money)
	}
	if data.BalanceBefore != "100.00" || data.BalanceAfter != "20.00" {
		t.Errorf("balance amounts mismatch: before %s, after %s", data.BalanceBefore, data.BalanceAfter)
	}
	if data.WalletVersion != 2 {
		t.Errorf("expected walletVersion 2, got %d", data.WalletVersion)
	}
}

func TestOutbox_WagerTransactionRejected(t *testing.T) {
	walletID := uuid.New()
	playerID := "player-rej"
	provID := "provider-abc"
	extID := "ext-tx-rej"
	amt, _ := money.New(8000, "BRL")

	tx, _ := domain.NewWagerTransaction(
		domain.OriginExternal,
		walletID,
		playerID,
		&provID,
		&extID,
		domain.TransactionTypeBet,
		amt,
		"BRL",
	)
	tx.Reject("insufficient_funds")

	correlationID := "idem-key-rej"
	outboxEv, err := domain.NewWagerTransactionRejectedOutboxEvent(&tx, correlationID, nil)
	if err != nil {
		t.Fatalf("failed to create outbox event: %v", err)
	}

	var envelope domain.EventEnvelope
	if err := json.Unmarshal(outboxEv.Payload, &envelope); err != nil {
		t.Fatalf("failed to unmarshal envelope: %v", err)
	}

	var data domain.WagerTransactionRejectedData
	if err := json.Unmarshal(envelope.Data, &data); err != nil {
		t.Fatalf("failed to unmarshal data: %v", err)
	}

	if data.ErrorCode != "insufficient_funds" {
		t.Errorf("expected errorCode insufficient_funds, got %s", data.ErrorCode)
	}
	if data.Amount != "80.00" {
		t.Errorf("expected amount 80.00, got %s", data.Amount)
	}
}
