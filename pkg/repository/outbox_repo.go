package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
)

// OutboxRepository gerencia a persistência, reivindicação concorrente e status dos eventos de outbox.
type OutboxRepository interface {
	Insert(ctx context.Context, tx pgx.Tx, event *domain.OutboxEvent) error
	ClaimPending(ctx context.Context, batchSize int, leaseDuration time.Duration) ([]*domain.OutboxEvent, error)
	MarkPublished(ctx context.Context, id uuid.UUID) error
	MarkFailed(ctx context.Context, id uuid.UUID, errReason string, nextAttempt time.Time, isFinal bool) error
	GetLag(ctx context.Context) (pendingCount int64, oldestPendingAge time.Duration, err error)
}

type pgxOutboxRepository struct {
	pool *pgxpool.Pool
}

func NewOutboxRepository(pool *pgxpool.Pool) OutboxRepository {
	return &pgxOutboxRepository{
		pool: pool,
	}
}

// Insert grava o evento atomicamente na transação fornecida.
func (r *pgxOutboxRepository) Insert(ctx context.Context, tx pgx.Tx, event *domain.OutboxEvent) error {
	query := `
		INSERT INTO outbox_events (
			id, aggregate_type, aggregate_id, event_type, payload, status, retry_count, next_attempt_at, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9
		)
	`
	_, err := tx.Exec(
		ctx,
		query,
		event.ID,
		event.AggregateType,
		event.AggregateID,
		event.EventType,
		event.Payload,
		event.Status,
		event.RetryCount,
		event.NextAttemptAt,
		event.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to insert outbox event: %w", err)
	}
	return nil
}

// ClaimPending reivindica de forma concorrente e atômica um lote de eventos pendentes usando FOR UPDATE SKIP LOCKED.
func (r *pgxOutboxRepository) ClaimPending(ctx context.Context, batchSize int, leaseDuration time.Duration) ([]*domain.OutboxEvent, error) {
	if batchSize <= 0 {
		batchSize = 10
	}
	leaseSeconds := int(leaseDuration.Seconds())
	if leaseSeconds <= 0 {
		leaseSeconds = 30
	}

	query := `
		WITH claimed AS (
			SELECT id
			FROM outbox_events
			WHERE status = 'PENDING'
			  AND next_attempt_at <= NOW()
			  AND (locked_until IS NULL OR locked_until < NOW())
			ORDER BY created_at ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox_events o
		SET locked_until = NOW() + ($2 || ' seconds')::interval,
		    retry_count = retry_count + 1
		FROM claimed
		WHERE o.id = claimed.id
		RETURNING o.id, o.aggregate_type, o.aggregate_id, o.event_type, o.payload, o.status,
		          o.retry_count, o.next_attempt_at, o.locked_until, o.last_error, o.created_at, o.processed_at
	`

	rows, err := r.pool.Query(ctx, query, batchSize, fmt.Sprintf("%d", leaseSeconds))
	if err != nil {
		return nil, fmt.Errorf("failed to claim pending outbox events: %w", err)
	}
	defer rows.Close()

	var events []*domain.OutboxEvent
	for rows.Next() {
		var ev domain.OutboxEvent
		var statusStr string
		err := rows.Scan(
			&ev.ID,
			&ev.AggregateType,
			&ev.AggregateID,
			&ev.EventType,
			&ev.Payload,
			&statusStr,
			&ev.RetryCount,
			&ev.NextAttemptAt,
			&ev.LockedUntil,
			&ev.LastError,
			&ev.CreatedAt,
			&ev.ProcessedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan claimed outbox event: %w", err)
		}
		ev.Status = domain.OutboxStatus(statusStr)
		events = append(events, &ev)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading claimed outbox events: %w", err)
	}

	return events, nil
}

// MarkPublished marca o evento como publicado e libera a trava de lease.
func (r *pgxOutboxRepository) MarkPublished(ctx context.Context, id uuid.UUID) error {
	query := `
		UPDATE outbox_events
		SET status = 'PUBLISHED',
		    processed_at = NOW(),
		    locked_until = NULL
		WHERE id = $1
	`
	_, err := r.pool.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to mark outbox event as published: %w", err)
	}
	return nil
}

// MarkFailed registra o erro, agenda a próxima tentativa e opcionalmente transiciona para FAILED definitivo.
func (r *pgxOutboxRepository) MarkFailed(ctx context.Context, id uuid.UUID, errReason string, nextAttempt time.Time, isFinal bool) error {
	query := `
		UPDATE outbox_events
		SET status = CASE WHEN $4::boolean THEN 'FAILED' ELSE status END,
		    next_attempt_at = $3,
		    last_error = $2,
		    locked_until = NULL
		WHERE id = $1
	`
	_, err := r.pool.Exec(ctx, query, id, errReason, nextAttempt, isFinal)
	if err != nil {
		return fmt.Errorf("failed to mark outbox event as failed: %w", err)
	}
	return nil
}

// GetLag calcula o backlog de eventos pendentes e a idade do evento mais antigo aguardando publicação.
func (r *pgxOutboxRepository) GetLag(ctx context.Context) (int64, time.Duration, error) {
	query := `
		SELECT 
			COUNT(*),
			COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(created_at))), 0)
		FROM outbox_events
		WHERE status = 'PENDING'
	`
	var pendingCount int64
	var ageSeconds float64

	err := r.pool.QueryRow(ctx, query).Scan(&pendingCount, &ageSeconds)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get outbox lag: %w", err)
	}

	oldestAge := time.Duration(ageSeconds * float64(time.Second))
	return pendingCount, oldestAge, nil
}
