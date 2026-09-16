package domain

import (
	"time"

	"github.com/google/uuid"
	"github.com/moaaskt/jungle-test-challenge/pkg/money"
)

// Wallet é o agregado raiz que representa a carteira financeira de um jogador.
// O saldo (balance) é encapsulado como campo privado e só pode ser mutado
// através dos métodos Debit e Credit, que aplicam todas as invariantes de negócio.
type Wallet struct {
	ID        uuid.UUID
	PlayerID  string
	Currency  string
	balance   money.Money // privado — só muda via Debit/Credit
	Version   int
	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewWallet cria uma nova carteira com saldo zero e versão 1.
// Usado na criação de uma carteira nova no sistema (ex: operação OPENING).
func NewWallet(playerID, currency string) (Wallet, error) {
	balance, err := money.Zero(currency)
	if err != nil {
		return Wallet{}, err
	}

	now := time.Now()
	return Wallet{
		ID:        uuid.New(),
		PlayerID:  playerID,
		Currency:  currency,
		balance:   balance,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

// RehydrateWallet reconstrói um Wallet a partir de dados persistidos no banco.
// Não aplica regras de criação, não gera UUID, não valida moeda.
// Assume que os dados vêm de uma fonte confiável (banco de dados).
func RehydrateWallet(
	id uuid.UUID,
	playerID, currency string,
	balance money.Money,
	version int,
	createdAt, updatedAt time.Time,
) Wallet {
	return Wallet{
		ID:        id,
		PlayerID:  playerID,
		Currency:  currency,
		balance:   balance,
		Version:   version,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}
}

// Balance retorna o saldo atual da carteira (read-only).
func (w *Wallet) Balance() money.Money {
	return w.balance
}

// Debit debita um valor da carteira, retornando o WalletLedgerEntry gerado.
// Regras aplicadas:
//   - A moeda do valor deve coincidir com a da carteira.
//   - O saldo não pode ficar negativo após o débito.
//   - A versão é incrementada.
//   - UpdatedAt é atualizado para o instante atual.
func (w *Wallet) Debit(amount money.Money, transactionID uuid.UUID) (WalletLedgerEntry, error) {
	if amount.Currency() != w.Currency {
		return WalletLedgerEntry{}, ErrCurrencyMismatch
	}

	balanceBefore := w.balance

	newBalance, err := w.balance.Sub(amount)
	if err != nil {
		return WalletLedgerEntry{}, err
	}

	if newBalance.IsNegative() {
		return WalletLedgerEntry{}, ErrInsufficientFunds
	}

	w.balance = newBalance
	w.Version++
	w.UpdatedAt = time.Now()

	entry := NewDebitEntry(w.ID, transactionID, amount, balanceBefore, w.balance)
	return entry, nil
}

// Credit credita um valor na carteira, retornando o WalletLedgerEntry gerado.
// Regras aplicadas:
//   - A moeda do valor deve coincidir com a da carteira.
//   - A versão é incrementada.
//   - UpdatedAt é atualizado para o instante atual.
func (w *Wallet) Credit(amount money.Money, transactionID uuid.UUID) (WalletLedgerEntry, error) {
	if amount.Currency() != w.Currency {
		return WalletLedgerEntry{}, ErrCurrencyMismatch
	}

	balanceBefore := w.balance

	newBalance, err := w.balance.Add(amount)
	if err != nil {
		return WalletLedgerEntry{}, err
	}

	w.balance = newBalance
	w.Version++
	w.UpdatedAt = time.Now()

	entry := NewCreditEntry(w.ID, transactionID, amount, balanceBefore, w.balance)
	return entry, nil
}
