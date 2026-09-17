package domain

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/moaaskt/jungle-test-challenge/pkg/money"
)

type OutboxStatus string

const (
	OutboxStatusPending   OutboxStatus = "PENDING"
	OutboxStatusPublished OutboxStatus = "PUBLISHED"
	OutboxStatusFailed    OutboxStatus = "FAILED"
)

const (
	EventTypeWagerTransactionProcessed        = "WagerTransactionProcessed"
	EventTypeWagerTransactionRejected         = "WagerTransactionRejected"
	EventTypeWalletBalanceChanged             = "WalletBalanceChanged"
	EventTypeWagerTransactionPendingReference = "WagerTransactionPendingReference"
)

// OutboxEvent representa o registro persistido na tabela outbox_events.
type OutboxEvent struct {
	ID            uuid.UUID
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       json.RawMessage
	Status        OutboxStatus
	RetryCount    int
	NextAttemptAt time.Time
	LockedUntil   *time.Time
	LastError     *string
	CreatedAt     time.Time
	ProcessedAt   *time.Time
}

// EventEnvelope é o envelope padronizado para todos os eventos da plataforma conforme Seção 11 da especificação.
type EventEnvelope struct {
	EventID       uuid.UUID       `json:"eventId"`
	EventType     string          `json:"eventType"`
	AggregateID   string          `json:"aggregateId"`
	CorrelationID string          `json:"correlationId"`
	CausationID   *string         `json:"causationId,omitempty"`
	OccurredAt    string          `json:"occurredAt"` // RFC 3339 UTC
	Version       int             `json:"version"`
	Data          json.RawMessage `json:"data"`
}

// MoneyPayload representa o formato de valor monetário serializado em eventos.
type MoneyPayload struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// WagerTransactionProcessedData é o payload do evento WagerTransactionProcessed.
type WagerTransactionProcessedData struct {
	TransactionID uuid.UUID `json:"transactionId"`
	WalletID      uuid.UUID `json:"walletId"`
	PlayerID      string    `json:"playerId"`
	ProviderID    *string   `json:"providerId,omitempty"`
	ExternalID    *string   `json:"externalId,omitempty"`
	Type          string    `json:"type"`
	Amount        string    `json:"amount"`
	Currency      string    `json:"currency"`
	RoundID       *string   `json:"roundId,omitempty"`
	GameID        *string   `json:"gameId,omitempty"`
	Status        string    `json:"status"`
	OccurredAt    string    `json:"occurredAt"`
}

// WagerTransactionRejectedData é o payload do evento WagerTransactionRejected.
type WagerTransactionRejectedData struct {
	TransactionID uuid.UUID `json:"transactionId"`
	WalletID      uuid.UUID `json:"walletId"`
	PlayerID      string    `json:"playerId"`
	ProviderID    *string   `json:"providerId,omitempty"`
	ExternalID    *string   `json:"externalId,omitempty"`
	Type          string    `json:"type"`
	Amount        string    `json:"amount"`
	Currency      string    `json:"currency"`
	ErrorCode     string    `json:"errorCode"`
	OccurredAt    string    `json:"occurredAt"`
}

// WalletBalanceChangedData é o payload do evento WalletBalanceChanged.
type WalletBalanceChangedData struct {
	WalletID      uuid.UUID    `json:"walletId"`
	TransactionID uuid.UUID    `json:"transactionId"`
	Direction     string       `json:"direction"` // DEBIT ou CREDIT
	Money         MoneyPayload `json:"money"`
	BalanceBefore string       `json:"balanceBefore"`
	BalanceAfter  string       `json:"balanceAfter"`
	WalletVersion int          `json:"walletVersion"`
}

// WagerTransactionPendingReferenceData é o payload do evento WagerTransactionPendingReference.
type WagerTransactionPendingReferenceData struct {
	TransactionID                  uuid.UUID `json:"transactionId"`
	WalletID                       uuid.UUID `json:"walletId"`
	PlayerID                       string    `json:"playerId"`
	ProviderID                     *string   `json:"providerId,omitempty"`
	ExternalID                     *string   `json:"externalId,omitempty"`
	ReferenceExternalTransactionID *string   `json:"referenceExternalTransactionId,omitempty"`
	Type                           string    `json:"type"`
	Amount                         string    `json:"amount"`
	Currency                       string    `json:"currency"`
	Status                         string    `json:"status"`
}

// NewWagerTransactionProcessedOutboxEvent cria um evento de outbox para WagerTransactionProcessed.
func NewWagerTransactionProcessedOutboxEvent(
	tx *WagerTransaction,
	correlationID string,
	causationID *string,
) (*OutboxEvent, error) {
	if tx == nil {
		return nil, fmt.Errorf("transaction cannot be nil")
	}

	occurredAt := time.Now().UTC()
	occurredAtStr := occurredAt.Format(time.RFC3339Nano)

	data := WagerTransactionProcessedData{
		TransactionID: tx.ID,
		WalletID:      tx.WalletID,
		PlayerID:      tx.PlayerID,
		ProviderID:    tx.ProviderID,
		ExternalID:    tx.ExternalID,
		Type:          string(tx.Type),
		Amount:        tx.Amount.FormattedAmount(),
		Currency:      tx.Currency,
		RoundID:       tx.RoundID,
		GameID:        tx.GameID,
		Status:        string(tx.Status),
		OccurredAt:    occurredAtStr,
	}

	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event data: %w", err)
	}

	eventID := uuid.New()
	envelope := EventEnvelope{
		EventID:       eventID,
		EventType:     EventTypeWagerTransactionProcessed,
		AggregateID:   tx.ID.String(),
		CorrelationID: correlationID,
		CausationID:   causationID,
		OccurredAt:    occurredAtStr,
		Version:       1,
		Data:          dataBytes,
	}

	payloadBytes, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event envelope: %w", err)
	}

	return &OutboxEvent{
		ID:            eventID,
		AggregateType: "WagerTransaction",
		AggregateID:   tx.ID.String(),
		EventType:     EventTypeWagerTransactionProcessed,
		Payload:       payloadBytes,
		Status:        OutboxStatusPending,
		RetryCount:    0,
		NextAttemptAt: occurredAt,
		CreatedAt:     occurredAt,
	}, nil
}

// NewWagerTransactionRejectedOutboxEvent cria um evento de outbox para WagerTransactionRejected.
func NewWagerTransactionRejectedOutboxEvent(
	tx *WagerTransaction,
	correlationID string,
	causationID *string,
) (*OutboxEvent, error) {
	if tx == nil {
		return nil, fmt.Errorf("transaction cannot be nil")
	}

	occurredAt := time.Now().UTC()
	occurredAtStr := occurredAt.Format(time.RFC3339Nano)

	errCode := ""
	if tx.FailureCode != nil {
		errCode = *tx.FailureCode
	}

	data := WagerTransactionRejectedData{
		TransactionID: tx.ID,
		WalletID:      tx.WalletID,
		PlayerID:      tx.PlayerID,
		ProviderID:    tx.ProviderID,
		ExternalID:    tx.ExternalID,
		Type:          string(tx.Type),
		Amount:        tx.Amount.FormattedAmount(),
		Currency:      tx.Currency,
		ErrorCode:     errCode,
		OccurredAt:    occurredAtStr,
	}

	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event data: %w", err)
	}

	eventID := uuid.New()
	envelope := EventEnvelope{
		EventID:       eventID,
		EventType:     EventTypeWagerTransactionRejected,
		AggregateID:   tx.ID.String(),
		CorrelationID: correlationID,
		CausationID:   causationID,
		OccurredAt:    occurredAtStr,
		Version:       1,
		Data:          dataBytes,
	}

	payloadBytes, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event envelope: %w", err)
	}

	return &OutboxEvent{
		ID:            eventID,
		AggregateType: "WagerTransaction",
		AggregateID:   tx.ID.String(),
		EventType:     EventTypeWagerTransactionRejected,
		Payload:       payloadBytes,
		Status:        OutboxStatusPending,
		RetryCount:    0,
		NextAttemptAt: occurredAt,
		CreatedAt:     occurredAt,
	}, nil
}

// NewWalletBalanceChangedOutboxEvent cria um evento de outbox para WalletBalanceChanged.
func NewWalletBalanceChangedOutboxEvent(
	walletID uuid.UUID,
	transactionID uuid.UUID,
	direction string,
	m money.Money,
	before money.Money,
	after money.Money,
	walletVersion int,
	correlationID string,
	causationID *string,
) (*OutboxEvent, error) {
	occurredAt := time.Now().UTC()
	occurredAtStr := occurredAt.Format(time.RFC3339Nano)

	data := WalletBalanceChangedData{
		WalletID:      walletID,
		TransactionID: transactionID,
		Direction:     direction,
		Money: MoneyPayload{
			Amount:   m.FormattedAmount(),
			Currency: m.Currency(),
		},
		BalanceBefore: before.FormattedAmount(),
		BalanceAfter:  after.FormattedAmount(),
		WalletVersion: walletVersion,
	}

	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event data: %w", err)
	}

	eventID := uuid.New()
	envelope := EventEnvelope{
		EventID:       eventID,
		EventType:     EventTypeWalletBalanceChanged,
		AggregateID:   walletID.String(),
		CorrelationID: correlationID,
		CausationID:   causationID,
		OccurredAt:    occurredAtStr,
		Version:       1,
		Data:          dataBytes,
	}

	payloadBytes, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event envelope: %w", err)
	}

	return &OutboxEvent{
		ID:            eventID,
		AggregateType: "Wallet",
		AggregateID:   walletID.String(),
		EventType:     EventTypeWalletBalanceChanged,
		Payload:       payloadBytes,
		Status:        OutboxStatusPending,
		RetryCount:    0,
		NextAttemptAt: occurredAt,
		CreatedAt:     occurredAt,
	}, nil
}

// NewWagerTransactionPendingReferenceOutboxEvent cria um evento de outbox para WagerTransactionPendingReference.
func NewWagerTransactionPendingReferenceOutboxEvent(
	tx *WagerTransaction,
	correlationID string,
	causationID *string,
) (*OutboxEvent, error) {
	if tx == nil {
		return nil, fmt.Errorf("transaction cannot be nil")
	}

	occurredAt := time.Now().UTC()
	occurredAtStr := occurredAt.Format(time.RFC3339Nano)

	data := WagerTransactionPendingReferenceData{
		TransactionID:                  tx.ID,
		WalletID:                       tx.WalletID,
		PlayerID:                       tx.PlayerID,
		ProviderID:                     tx.ProviderID,
		ExternalID:                     tx.ExternalID,
		ReferenceExternalTransactionID: tx.ReferenceExternalTransactionID,
		Type:                           string(tx.Type),
		Amount:                         tx.Amount.FormattedAmount(),
		Currency:                       tx.Currency,
		Status:                         string(tx.Status),
	}

	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event data: %w", err)
	}

	eventID := uuid.New()
	envelope := EventEnvelope{
		EventID:       eventID,
		EventType:     EventTypeWagerTransactionPendingReference,
		AggregateID:   tx.ID.String(),
		CorrelationID: correlationID,
		CausationID:   causationID,
		OccurredAt:    occurredAtStr,
		Version:       1,
		Data:          dataBytes,
	}

	payloadBytes, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event envelope: %w", err)
	}

	return &OutboxEvent{
		ID:            eventID,
		AggregateType: "WagerTransaction",
		AggregateID:   tx.ID.String(),
		EventType:     EventTypeWagerTransactionPendingReference,
		Payload:       payloadBytes,
		Status:        OutboxStatusPending,
		RetryCount:    0,
		NextAttemptAt: occurredAt,
		CreatedAt:     occurredAt,
	}, nil
}
