package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
)

// ErrInboxDuplicateMessage indica que a mensagem já foi processada por este consumidor.
var ErrInboxDuplicateMessage = errors.New("duplicate message in inbox for consumer")

// InboxRepository gerencia a consulta e gravação transacional das mensagens da inbox.
type InboxRepository interface {
	Get(ctx context.Context, tx pgx.Tx, consumerName, messageID string) (*domain.InboxRecord, error)
	Insert(ctx context.Context, tx pgx.Tx, record *domain.InboxRecord) error
}

type pgxInboxRepository struct{}

func NewInboxRepository() InboxRepository {
	return &pgxInboxRepository{}
}

// Get busca um registro de inbox por consumidor e message_id dentro da transação SQL fornecida.
func (r *pgxInboxRepository) Get(ctx context.Context, tx pgx.Tx, consumerName, messageID string) (*domain.InboxRecord, error) {
	query := `
		SELECT id, message_id, consumer_name, source, payload_hash, received_at, processed_at
		FROM inbox_messages
		WHERE consumer_name = $1 AND message_id = $2
	`
	var record domain.InboxRecord
	err := tx.QueryRow(ctx, query, consumerName, messageID).Scan(
		&record.ID,
		&record.MessageID,
		&record.ConsumerName,
		&record.Source,
		&record.PayloadHash,
		&record.ReceivedAt,
		&record.ProcessedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // Not found
		}
		return nil, fmt.Errorf("failed to get inbox message: %w", err)
	}

	return &record, nil
}

// Insert grava um novo registro de inbox na transação fornecida.
func (r *pgxInboxRepository) Insert(ctx context.Context, tx pgx.Tx, record *domain.InboxRecord) error {
	query := `
		INSERT INTO inbox_messages (
			id, message_id, consumer_name, source, payload_hash, received_at, processed_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7
		)
	`
	_, err := tx.Exec(
		ctx,
		query,
		record.ID,
		record.MessageID,
		record.ConsumerName,
		record.Source,
		record.PayloadHash,
		record.ReceivedAt,
		record.ProcessedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrInboxDuplicateMessage
		}
		return fmt.Errorf("failed to insert inbox message: %w", err)
	}

	return nil
}
