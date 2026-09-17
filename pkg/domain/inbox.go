package domain

import (
	"time"

	"github.com/google/uuid"
)

// InboxRecord representa uma mensagem processada e persistida pelo padrão Inbox.
type InboxRecord struct {
	ID           uuid.UUID
	MessageID    string
	ConsumerName string
	Source       string
	PayloadHash  string
	ReceivedAt   time.Time
	ProcessedAt  time.Time
}

// NewInboxRecord instancia um novo InboxRecord com timestamps atuais e ID gerado.
func NewInboxRecord(messageID, consumerName, source, payloadHash string) InboxRecord {
	now := time.Now()
	return InboxRecord{
		ID:           uuid.New(),
		MessageID:    messageID,
		ConsumerName: consumerName,
		Source:       source,
		PayloadHash:  payloadHash,
		ReceivedAt:   now,
		ProcessedAt:  now,
	}
}
