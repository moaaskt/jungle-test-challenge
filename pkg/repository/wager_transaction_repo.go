package repository

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
	"github.com/moaaskt/jungle-test-challenge/pkg/money"
)

var ErrProviderExternalConflict = errors.New("provider external transaction id conflict")

// WagerTransactionRepository define a interface de persistência para as transações de apostas.
type WagerTransactionRepository interface {
	Insert(ctx context.Context, tx pgx.Tx, wt *domain.WagerTransaction) error
	GetByProviderAndExternalID(ctx context.Context, tx pgx.Tx, providerID, externalID string) (*domain.WagerTransaction, error)
}

type pgxWagerTransactionRepository struct{}

func NewWagerTransactionRepository() WagerTransactionRepository {
	return &pgxWagerTransactionRepository{}
}

func (r *pgxWagerTransactionRepository) Insert(ctx context.Context, tx pgx.Tx, wt *domain.WagerTransaction) error {
	query := `
		INSERT INTO wager_transactions (
			id, origin, external_id, provider_id, idempotency_key, payload_hash, 
			wallet_id, player_id, round_id, game_id, type, amount, currency, 
			status, reference_id, external_reference_id, error_code, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, 
			$7, $8, $9, $10, $11, $12, $13, 
			$14, $15, $16, $17, $18, $19
		)
	`
	_, err := tx.Exec(ctx, query,
		wt.ID,
		wt.Origin,
		wt.ExternalID,
		wt.ProviderID,
		wt.IdempotencyKey,
		wt.PayloadHash,
		wt.WalletID,
		wt.PlayerID,
		wt.RoundID,
		wt.GameID,
		wt.Type,
		wt.Amount.Amount(),
		wt.Currency,
		wt.Status,
		wt.ReferenceID,
		wt.ReferenceExternalTransactionID,
		wt.FailureCode,
		wt.CreatedAt,
		wt.UpdatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "uk_wager_tx_provider_external" {
			return ErrProviderExternalConflict
		}
		return err
	}
	return nil
}

func (r *pgxWagerTransactionRepository) GetByProviderAndExternalID(ctx context.Context, tx pgx.Tx, providerID, externalID string) (*domain.WagerTransaction, error) {
	query := `
		SELECT id, origin, external_id, provider_id, idempotency_key, payload_hash,
		       wallet_id, player_id, round_id, game_id, type, amount, currency,
		       status, reference_id, external_reference_id, error_code, created_at, updated_at
		FROM wager_transactions
		WHERE provider_id = $1 AND external_id = $2
	`
	row := tx.QueryRow(ctx, query, providerID, externalID)

	var (
		wt                             domain.WagerTransaction
		rawAmount                      int64
		origin                         string
		txType                         string
		status                         string
		externalIDPtr, providerIDPtr   *string
		idemKeyPtr, payloadHashPtr     *string
		roundIDPtr, gameIDPtr          *string
		refIDPtr                       *uuid.UUID
		refExtIDPtr                    *string
		errCodePtr                     *string
	)

	err := row.Scan(
		&wt.ID,
		&origin,
		&externalIDPtr,
		&providerIDPtr,
		&idemKeyPtr,
		&payloadHashPtr,
		&wt.WalletID,
		&wt.PlayerID,
		&roundIDPtr,
		&gameIDPtr,
		&txType,
		&rawAmount,
		&wt.Currency,
		&status,
		&refIDPtr,
		&refExtIDPtr,
		&errCodePtr,
		&wt.CreatedAt,
		&wt.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	moneyAmt, err := money.New(rawAmount, wt.Currency)
	if err != nil {
		return nil, err
	}

	txResult := domain.RehydrateWagerTransaction(
		wt.ID,
		domain.Origin(origin),
		wt.WalletID,
		wt.PlayerID,
		providerIDPtr,
		externalIDPtr,
		idemKeyPtr,
		payloadHashPtr,
		roundIDPtr,
		gameIDPtr,
		externalIDPtr,
		refExtIDPtr,
		refIDPtr,
		domain.TransactionType(txType),
		moneyAmt,
		wt.Currency,
		domain.TransactionStatus(status),
		errCodePtr,
		wt.CreatedAt,
		wt.UpdatedAt,
	)

	return &txResult, nil
}

