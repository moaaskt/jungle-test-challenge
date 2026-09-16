package repository

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
)

// LedgerRepository define a interface de persistência para as entradas no ledger.
type LedgerRepository interface {
	Insert(ctx context.Context, tx pgx.Tx, entry *domain.WalletLedgerEntry) error
}

type pgxLedgerRepository struct{}

func NewLedgerRepository() LedgerRepository {
	return &pgxLedgerRepository{}
}

func (r *pgxLedgerRepository) Insert(ctx context.Context, tx pgx.Tx, entry *domain.WalletLedgerEntry) error {
	query := `
		INSERT INTO wallet_ledger_entries (
			id, wallet_id, transaction_id, type, amount, balance_before, balance_after, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8
		)
	`
	_, err := tx.Exec(ctx, query,
		entry.ID,
		entry.WalletID,
		entry.TransactionID,
		entry.Type,
		entry.Amount.Amount(),
		entry.BalanceBefore.Amount(),
		entry.BalanceAfter.Amount(),
		entry.CreatedAt,
	)
	return err
}
