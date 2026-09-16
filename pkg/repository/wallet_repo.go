package repository

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
	"github.com/moaaskt/jungle-test-challenge/pkg/money"
)

// ErrNotFound indica que a entidade solicitada não foi encontrada no banco.
var ErrNotFound = errors.New("entity not found")

// WalletRepository define a interface de persistência para Wallet.
type WalletRepository interface {
	GetByID(ctx context.Context, tx pgx.Tx, id uuid.UUID) (*domain.Wallet, error)
	GetByPlayerAndCurrency(ctx context.Context, tx pgx.Tx, playerID, currency string) (*domain.Wallet, error)
	Insert(ctx context.Context, tx pgx.Tx, w *domain.Wallet) error
	UpdateBalance(ctx context.Context, tx pgx.Tx, w *domain.Wallet) error
}

type pgxWalletRepository struct{}

func NewWalletRepository() WalletRepository {
	return &pgxWalletRepository{}
}

func (r *pgxWalletRepository) GetByID(ctx context.Context, tx pgx.Tx, id uuid.UUID) (*domain.Wallet, error) {
	query := `
		SELECT id, player_id, currency, balance, version, created_at, updated_at
		FROM wallets
		WHERE id = $1
	`
	return r.scanRow(ctx, tx.QueryRow(ctx, query, id))
}

func (r *pgxWalletRepository) GetByPlayerAndCurrency(ctx context.Context, tx pgx.Tx, playerID, currency string) (*domain.Wallet, error) {
	query := `
		SELECT id, player_id, currency, balance, version, created_at, updated_at
		FROM wallets
		WHERE player_id = $1 AND currency = $2
	`
	return r.scanRow(ctx, tx.QueryRow(ctx, query, playerID, currency))
}

func (r *pgxWalletRepository) scanRow(ctx context.Context, row pgx.Row) (*domain.Wallet, error) {
	var id uuid.UUID
	var playerID, currency string
	var balanceAmount int64
	var version int
	var createdAt, updatedAt time.Time

	err := row.Scan(&id, &playerID, &currency, &balanceAmount, &version, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	balance := money.MustNew(balanceAmount, currency)
	w := domain.RehydrateWallet(id, playerID, currency, balance, version, createdAt, updatedAt)
	return &w, nil
}

func (r *pgxWalletRepository) Insert(ctx context.Context, tx pgx.Tx, w *domain.Wallet) error {
	query := `
		INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	_, err := tx.Exec(ctx, query,
		w.ID,
		w.PlayerID,
		w.Currency,
		w.Balance().Amount(),
		w.Version,
		w.CreatedAt,
		w.UpdatedAt,
	)
	return err
}

func (r *pgxWalletRepository) UpdateBalance(ctx context.Context, tx pgx.Tx, w *domain.Wallet) error {
	query := `
		UPDATE wallets
		SET balance = $1, version = $2, updated_at = $3
		WHERE id = $4 AND version = $5
	`
	// Usamos Optimistic Locking: asseguramos que o DB só atualiza se a versão ainda for a versão esperada menos 1 (antes de w.Debit/Credit).
	// A entidade no objeto go já foi incrementada. Assim verificamos version - 1 no WHERE.
	expectedVersion := w.Version - 1

	cmdTag, err := tx.Exec(ctx, query,
		w.Balance().Amount(),
		w.Version,
		w.UpdatedAt,
		w.ID,
		expectedVersion,
	)
	if err != nil {
		return err
	}

	if cmdTag.RowsAffected() == 0 {
		return errors.New("optimistic lock failed or wallet not found")
	}

	return nil
}
