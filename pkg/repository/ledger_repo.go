package repository

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
	"github.com/moaaskt/jungle-test-challenge/pkg/money"
)

// LedgerCursor representa o estado de paginação keyset para o ledger.
// Codificado como base64 opaco conforme Seção 9 do desafio.
type LedgerCursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

// EncodeCursor serializa um LedgerCursor para uma string base64 opaca.
func EncodeCursor(c LedgerCursor) string {
	raw := fmt.Sprintf("%s#%s", c.CreatedAt.Format(time.RFC3339Nano), c.ID.String())
	return base64.StdEncoding.EncodeToString([]byte(raw))
}

// DecodeCursor deserializa uma string base64 opaca em um LedgerCursor.
func DecodeCursor(encoded string) (*LedgerCursor, error) {
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}
	parts := strings.SplitN(string(decoded), "#", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid cursor format: expected 'timestamp#uuid'")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid cursor timestamp: %w", err)
	}
	id, err := uuid.Parse(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid cursor uuid: %w", err)
	}
	return &LedgerCursor{CreatedAt: createdAt, ID: id}, nil
}

// LedgerRepository define a interface de persistência para as entradas no ledger.
type LedgerRepository interface {
	Insert(ctx context.Context, tx pgx.Tx, entry *domain.WalletLedgerEntry) error
	GetByWalletID(ctx context.Context, tx pgx.Tx, walletID uuid.UUID, currency string, cursor *LedgerCursor, limit int) ([]*domain.WalletLedgerEntry, *LedgerCursor, error)
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

// GetByWalletID retorna lançamentos do ledger com keyset pagination (cursor opaco).
// Busca N+1 registros; se houver N+1, há próxima página.
// O parâmetro currency é necessário para reconstituir os tipos money.Money.
func (r *pgxLedgerRepository) GetByWalletID(ctx context.Context, tx pgx.Tx, walletID uuid.UUID, currency string, cursor *LedgerCursor, limit int) ([]*domain.WalletLedgerEntry, *LedgerCursor, error) {
	var rows pgx.Rows
	var err error

	if cursor != nil {
		query := `
			SELECT id, wallet_id, transaction_id, type, amount, balance_before, balance_after, created_at
			FROM wallet_ledger_entries
			WHERE wallet_id = $1
			  AND (created_at, id) > ($2, $3)
			ORDER BY created_at ASC, id ASC
			LIMIT $4
		`
		rows, err = tx.Query(ctx, query, walletID, cursor.CreatedAt, cursor.ID, limit+1)
	} else {
		query := `
			SELECT id, wallet_id, transaction_id, type, amount, balance_before, balance_after, created_at
			FROM wallet_ledger_entries
			WHERE wallet_id = $1
			ORDER BY created_at ASC, id ASC
			LIMIT $2
		`
		rows, err = tx.Query(ctx, query, walletID, limit+1)
	}
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var entries []*domain.WalletLedgerEntry
	for rows.Next() {
		var (
			id            uuid.UUID
			wID           uuid.UUID
			txID          uuid.UUID
			entryType     string
			amount        int64
			balanceBefore int64
			balanceAfter  int64
			createdAt     time.Time
		)

		if err := rows.Scan(&id, &wID, &txID, &entryType, &amount, &balanceBefore, &balanceAfter, &createdAt); err != nil {
			return nil, nil, err
		}

		entry := domain.RehydrateLedgerEntry(
			id, wID, txID,
			domain.LedgerEntryType(entryType),
			money.MustNew(amount, currency),
			money.MustNew(balanceBefore, currency),
			money.MustNew(balanceAfter, currency),
			createdAt,
		)
		entries = append(entries, &entry)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	// Determinar se há próxima página
	var nextCursor *LedgerCursor
	if len(entries) > limit {
		entries = entries[:limit]
		last := entries[limit-1]
		nextCursor = &LedgerCursor{
			CreatedAt: last.CreatedAt,
			ID:        last.ID,
		}
	}

	return entries, nextCursor, nil
}
