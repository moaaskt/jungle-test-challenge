package domain

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moaaskt/jungle-test-challenge/pkg/money"
)

func testTime() time.Time {
	return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
}

func TestNewWallet_CreatesWithZeroBalance(t *testing.T) {
	w, err := NewWallet("player-1", "BRL")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if w.Balance().Amount() != 0 {
		t.Errorf("expected zero balance, got %d", w.Balance().Amount())
	}
	if w.Currency != "BRL" {
		t.Errorf("expected currency BRL, got %s", w.Currency)
	}
	if w.Version != 1 {
		t.Errorf("expected version 1, got %d", w.Version)
	}
	if w.PlayerID != "player-1" {
		t.Errorf("expected player-1, got %s", w.PlayerID)
	}
	if w.CreatedAt.IsZero() {
		t.Error("expected CreatedAt to be set")
	}
	if w.UpdatedAt.IsZero() {
		t.Error("expected UpdatedAt to be set")
	}
}

func TestNewWallet_InvalidCurrency(t *testing.T) {
	_, err := NewWallet("player-1", "XX")
	if err == nil {
		t.Fatal("expected error for invalid currency")
	}
}

func TestWallet_CreditIncrementsVersion(t *testing.T) {
	w, _ := NewWallet("player-1", "BRL")
	initialVersion := w.Version

	amount := money.MustNew(1000, "BRL") // 10.00
	txID := uuid.New()

	entry, err := w.Credit(amount, txID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if w.Version != initialVersion+1 {
		t.Errorf("expected version %d, got %d", initialVersion+1, w.Version)
	}
	if w.Balance().Amount() != 1000 {
		t.Errorf("expected balance 1000, got %d", w.Balance().Amount())
	}
	if entry.Type != LedgerEntryTypeCredit {
		t.Errorf("expected CREDIT entry, got %s", entry.Type)
	}
	if entry.BalanceBefore.Amount() != 0 {
		t.Errorf("expected balance_before 0, got %d", entry.BalanceBefore.Amount())
	}
	if entry.BalanceAfter.Amount() != 1000 {
		t.Errorf("expected balance_after 1000, got %d", entry.BalanceAfter.Amount())
	}
	if entry.TransactionID != txID {
		t.Errorf("expected transaction ID %s, got %s", txID, entry.TransactionID)
	}
}

func TestWallet_DebitIncrementsVersion(t *testing.T) {
	w, _ := NewWallet("player-1", "BRL")
	credit := money.MustNew(5000, "BRL") // 50.00
	w.Credit(credit, uuid.New())

	versionBeforeDebit := w.Version
	debitAmount := money.MustNew(2000, "BRL") // 20.00
	txID := uuid.New()

	entry, err := w.Debit(debitAmount, txID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if w.Version != versionBeforeDebit+1 {
		t.Errorf("expected version %d, got %d", versionBeforeDebit+1, w.Version)
	}
	if w.Balance().Amount() != 3000 {
		t.Errorf("expected balance 3000, got %d", w.Balance().Amount())
	}
	if entry.Type != LedgerEntryTypeDebit {
		t.Errorf("expected DEBIT entry, got %s", entry.Type)
	}
	if entry.BalanceBefore.Amount() != 5000 {
		t.Errorf("expected balance_before 5000, got %d", entry.BalanceBefore.Amount())
	}
	if entry.BalanceAfter.Amount() != 3000 {
		t.Errorf("expected balance_after 3000, got %d", entry.BalanceAfter.Amount())
	}
}

func TestWallet_DebitInsufficientFunds(t *testing.T) {
	w, _ := NewWallet("player-1", "BRL")
	credit := money.MustNew(1000, "BRL") // 10.00
	w.Credit(credit, uuid.New())

	debitAmount := money.MustNew(2000, "BRL") // 20.00
	_, err := w.Debit(debitAmount, uuid.New())
	if err != ErrInsufficientFunds {
		t.Errorf("expected ErrInsufficientFunds, got %v", err)
	}

	// Saldo deve permanecer inalterado.
	if w.Balance().Amount() != 1000 {
		t.Errorf("expected balance to remain 1000, got %d", w.Balance().Amount())
	}
}

func TestWallet_DebitExactBalance(t *testing.T) {
	w, _ := NewWallet("player-1", "BRL")
	amount := money.MustNew(5000, "BRL")
	w.Credit(amount, uuid.New())

	_, err := w.Debit(amount, uuid.New())
	if err != nil {
		t.Fatalf("unexpected error debiting exact balance: %v", err)
	}
	if w.Balance().Amount() != 0 {
		t.Errorf("expected zero balance, got %d", w.Balance().Amount())
	}
}

func TestWallet_CurrencyMismatchOnCredit(t *testing.T) {
	w, _ := NewWallet("player-1", "BRL")
	usd := money.MustNew(1000, "USD")

	_, err := w.Credit(usd, uuid.New())
	if err != ErrCurrencyMismatch {
		t.Errorf("expected ErrCurrencyMismatch, got %v", err)
	}
}

func TestWallet_CurrencyMismatchOnDebit(t *testing.T) {
	w, _ := NewWallet("player-1", "BRL")
	credit := money.MustNew(5000, "BRL")
	w.Credit(credit, uuid.New())

	usd := money.MustNew(1000, "USD")
	_, err := w.Debit(usd, uuid.New())
	if err != ErrCurrencyMismatch {
		t.Errorf("expected ErrCurrencyMismatch, got %v", err)
	}
}

func TestWallet_MultipleCreditDebitSequence(t *testing.T) {
	w, _ := NewWallet("player-1", "BRL")

	// Crédito de 100.00
	w.Credit(money.MustNew(10000, "BRL"), uuid.New())
	// Crédito de 50.00
	w.Credit(money.MustNew(5000, "BRL"), uuid.New())
	// Débito de 30.00
	w.Debit(money.MustNew(3000, "BRL"), uuid.New())

	expected := int64(12000) // 120.00
	if w.Balance().Amount() != expected {
		t.Errorf("expected balance %d, got %d", expected, w.Balance().Amount())
	}
	// version: 1 (new) + 3 operations = 4
	if w.Version != 4 {
		t.Errorf("expected version 4, got %d", w.Version)
	}
}

func TestRehydrateWallet_DoesNotValidate(t *testing.T) {
	id := uuid.New()
	balance := money.MustNew(9999, "BRL")
	now := testTime()
	w := RehydrateWallet(id, "player-42", "BRL", balance, 7, now, now)

	if w.ID != id {
		t.Errorf("expected ID %s, got %s", id, w.ID)
	}
	if w.Balance().Amount() != 9999 {
		t.Errorf("expected balance 9999, got %d", w.Balance().Amount())
	}
	if w.Version != 7 {
		t.Errorf("expected version 7, got %d", w.Version)
	}
}

func TestWallet_DebitDoesNotMutateOnError(t *testing.T) {
	w, _ := NewWallet("player-1", "BRL")
	w.Credit(money.MustNew(1000, "BRL"), uuid.New())
	versionBefore := w.Version
	updatedAtBefore := w.UpdatedAt

	// Tenta debitar mais do que o saldo
	_, err := w.Debit(money.MustNew(2000, "BRL"), uuid.New())
	if err == nil {
		t.Fatal("expected error")
	}

	// Versão e saldo devem permanecer inalterados
	if w.Version != versionBefore {
		t.Errorf("version changed on failed debit: expected %d, got %d", versionBefore, w.Version)
	}
	if w.UpdatedAt != updatedAtBefore {
		t.Error("updatedAt changed on failed debit")
	}
}
