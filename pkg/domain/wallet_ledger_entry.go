package domain

import (
	"time"

	"github.com/google/uuid"
	"github.com/moaaskt/jungle-test-challenge/pkg/money"
)

// WalletLedgerEntry representa um registro imutável e append-only de uma
// movimentação financeira no ledger de uma carteira.
// Uma vez criado, não pode ser alterado nem removido (enforced por trigger no banco).
type WalletLedgerEntry struct {
	ID            uuid.UUID
	WalletID      uuid.UUID
	TransactionID uuid.UUID
	Type          LedgerEntryType
	Amount        money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
	CreatedAt     time.Time
}

// LedgerEntryType indica se a movimentação é de débito ou crédito.
type LedgerEntryType string

const (
	LedgerEntryTypeDebit  LedgerEntryType = "DEBIT"
	LedgerEntryTypeCredit LedgerEntryType = "CREDIT"
)

// NewDebitEntry cria um registro de débito para o ledger.
// O chamador (Wallet.Debit) já validou saldo e moeda antes de invocar este construtor.
func NewDebitEntry(walletID, transactionID uuid.UUID, amount, balanceBefore, balanceAfter money.Money) WalletLedgerEntry {
	return WalletLedgerEntry{
		ID:            uuid.New(),
		WalletID:      walletID,
		TransactionID: transactionID,
		Type:          LedgerEntryTypeDebit,
		Amount:        amount,
		BalanceBefore: balanceBefore,
		BalanceAfter:  balanceAfter,
		CreatedAt:     time.Now(),
	}
}

// NewCreditEntry cria um registro de crédito para o ledger.
// O chamador (Wallet.Credit) já validou moeda antes de invocar este construtor.
func NewCreditEntry(walletID, transactionID uuid.UUID, amount, balanceBefore, balanceAfter money.Money) WalletLedgerEntry {
	return WalletLedgerEntry{
		ID:            uuid.New(),
		WalletID:      walletID,
		TransactionID: transactionID,
		Type:          LedgerEntryTypeCredit,
		Amount:        amount,
		BalanceBefore: balanceBefore,
		BalanceAfter:  balanceAfter,
		CreatedAt:     time.Now(),
	}
}

// RehydrateLedgerEntry reconstrói uma WalletLedgerEntry a partir de dados persistidos.
// Não aplica regras de negócio nem gera IDs — assume que os dados vêm de uma fonte confiável (banco).
func RehydrateLedgerEntry(
	id, walletID, transactionID uuid.UUID,
	entryType LedgerEntryType,
	amount, balanceBefore, balanceAfter money.Money,
	createdAt time.Time,
) WalletLedgerEntry {
	return WalletLedgerEntry{
		ID:            id,
		WalletID:      walletID,
		TransactionID: transactionID,
		Type:          entryType,
		Amount:        amount,
		BalanceBefore: balanceBefore,
		BalanceAfter:  balanceAfter,
		CreatedAt:     createdAt,
	}
}
