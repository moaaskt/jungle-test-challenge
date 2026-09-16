package repository

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
)

var ErrIdempotencyKeyConflict = errors.New("idempotency key constraint violation")

type IdempotencyRepository interface {
	GetByKey(ctx context.Context, tx pgx.Tx, key string) (*domain.IdempotencyRecord, error)
	Insert(ctx context.Context, tx pgx.Tx, record *domain.IdempotencyRecord) error
}

type postgresIdempotencyRepository struct{}

func NewIdempotencyRepository() IdempotencyRepository {
	return &postgresIdempotencyRepository{}
}

func (r *postgresIdempotencyRepository) GetByKey(ctx context.Context, tx pgx.Tx, key string) (*domain.IdempotencyRecord, error) {
	query := `
		SELECT id, key, payload_hash, response_status, response_body, created_at
		FROM idempotency_records
		WHERE key = $1
	`
	var record domain.IdempotencyRecord
	err := tx.QueryRow(ctx, query, key).Scan(
		&record.ID,
		&record.Key,
		&record.PayloadHash,
		&record.ResponseStatus,
		&record.ResponseBody,
		&record.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // Not found is not an error
		}
		return nil, err
	}

	return &record, nil
}

func (r *postgresIdempotencyRepository) Insert(ctx context.Context, tx pgx.Tx, record *domain.IdempotencyRecord) error {
	query := `
		INSERT INTO idempotency_records (key, payload_hash, response_status, response_body)
		VALUES ($1, $2, $3, $4)
	`
	_, err := tx.Exec(ctx, query, record.Key, record.PayloadHash, record.ResponseStatus, record.ResponseBody)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" { // 23505 is unique_violation
			return ErrIdempotencyKeyConflict
		}
		return err
	}
	return nil
}
