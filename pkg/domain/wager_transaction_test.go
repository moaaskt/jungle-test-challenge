package domain

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/moaaskt/jungle-test-challenge/pkg/money"
)

func strPtr(s string) *string { return &s }

// ---------------------------------------------------------------------------
// Criação
// ---------------------------------------------------------------------------

func TestNewWagerTransaction_ValidBet(t *testing.T) {
	walletID := uuid.New()
	amount := money.MustNew(5000, "BRL")

	tx, err := NewWagerTransaction(
		OriginExternal, walletID, "player-1",
		strPtr("provider-abc"), strPtr("ext-tx-001"),
		TransactionTypeBet, amount, "BRL",
		WithIdempotency("idem-key-1", "hash-abc123"),
		WithGameContext("round-42", "game-99"),
		WithExternalTransactionID("ext-tx-001"),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tx.Status != StatusPending {
		t.Errorf("expected PENDING, got %s", tx.Status)
	}
	if tx.Origin != OriginExternal {
		t.Errorf("expected EXTERNAL, got %s", tx.Origin)
	}
	if tx.Type != TransactionTypeBet {
		t.Errorf("expected BET, got %s", tx.Type)
	}
	if *tx.ProviderID != "provider-abc" {
		t.Errorf("expected provider-abc, got %s", *tx.ProviderID)
	}
	if *tx.IdempotencyKey != "idem-key-1" {
		t.Errorf("expected idem-key-1, got %s", *tx.IdempotencyKey)
	}
	if *tx.RoundID != "round-42" {
		t.Errorf("expected round-42, got %s", *tx.RoundID)
	}
}

func TestNewWagerTransaction_LossRequiresZeroAmount(t *testing.T) {
	walletID := uuid.New()
	amount := money.MustNew(100, "BRL") // não-zero

	_, err := NewWagerTransaction(
		OriginExternal, walletID, "player-1",
		strPtr("provider-abc"), strPtr("ext-001"),
		TransactionTypeLoss, amount, "BRL",
	)
	if !errors.Is(err, ErrZeroAmountRequired) {
		t.Errorf("expected ErrZeroAmountRequired, got %v", err)
	}
}

func TestNewWagerTransaction_LossWithZeroAmountSucceeds(t *testing.T) {
	walletID := uuid.New()
	amount := money.MustNew(0, "BRL")

	tx, err := NewWagerTransaction(
		OriginExternal, walletID, "player-1",
		strPtr("provider-abc"), strPtr("ext-001"),
		TransactionTypeLoss, amount, "BRL",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.Type != TransactionTypeLoss {
		t.Errorf("expected LOSS, got %s", tx.Type)
	}
	if !tx.IsLoss() {
		t.Error("IsLoss() should return true")
	}
}

func TestNewWagerTransaction_ExternalRequiresProviderID(t *testing.T) {
	walletID := uuid.New()
	amount := money.MustNew(1000, "BRL")

	_, err := NewWagerTransaction(
		OriginExternal, walletID, "player-1",
		nil, strPtr("ext-001"),
		TransactionTypeBet, amount, "BRL",
	)
	if !errors.Is(err, ErrProviderRequired) {
		t.Errorf("expected ErrProviderRequired, got %v", err)
	}
}

func TestNewWagerTransaction_ExternalRequiresExternalID(t *testing.T) {
	walletID := uuid.New()
	amount := money.MustNew(1000, "BRL")

	_, err := NewWagerTransaction(
		OriginExternal, walletID, "player-1",
		strPtr("provider-abc"), nil,
		TransactionTypeBet, amount, "BRL",
	)
	if !errors.Is(err, ErrExternalIDRequired) {
		t.Errorf("expected ErrExternalIDRequired, got %v", err)
	}
}

func TestNewWagerTransaction_InternalDoesNotRequireProviderID(t *testing.T) {
	walletID := uuid.New()
	amount := money.MustNew(0, "BRL")

	tx, err := NewWagerTransaction(
		OriginInternal, walletID, "player-1",
		nil, nil,
		TransactionTypeOpening, amount, "BRL",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.ProviderID != nil {
		t.Error("expected nil ProviderID for internal transaction")
	}
}

// ---------------------------------------------------------------------------
// Máquina de estados: transições válidas
// ---------------------------------------------------------------------------

func TestWagerTransaction_PendingToProcessed(t *testing.T) {
	tx := mustNewBetTransaction(t)

	if err := tx.Process(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.Status != StatusProcessed {
		t.Errorf("expected PROCESSED, got %s", tx.Status)
	}
}

func TestWagerTransaction_PendingToRejected(t *testing.T) {
	tx := mustNewBetTransaction(t)

	if err := tx.Reject("INSUFFICIENT_FUNDS"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.Status != StatusRejected {
		t.Errorf("expected REJECTED, got %s", tx.Status)
	}
	if *tx.FailureCode != "INSUFFICIENT_FUNDS" {
		t.Errorf("expected failure code INSUFFICIENT_FUNDS, got %s", *tx.FailureCode)
	}
}

func TestWagerTransaction_PendingToFailed(t *testing.T) {
	tx := mustNewBetTransaction(t)

	if err := tx.Fail("DB_ERROR"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.Status != StatusFailed {
		t.Errorf("expected FAILED, got %s", tx.Status)
	}
	if *tx.FailureCode != "DB_ERROR" {
		t.Errorf("expected failure code DB_ERROR, got %s", *tx.FailureCode)
	}
}

func TestWagerTransaction_PendingToPendingReference(t *testing.T) {
	tx := mustNewBetTransaction(t)

	if err := tx.MarkPendingReference(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.Status != StatusPendingReference {
		t.Errorf("expected PENDING_REFERENCE, got %s", tx.Status)
	}
}

func TestWagerTransaction_PendingReferenceToProcessed(t *testing.T) {
	tx := mustNewBetTransaction(t)
	tx.MarkPendingReference()

	refID := uuid.New()
	if err := tx.ResolveAndProcess(refID); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.Status != StatusProcessed {
		t.Errorf("expected PROCESSED, got %s", tx.Status)
	}
	if tx.ReferenceID == nil || *tx.ReferenceID != refID {
		t.Errorf("expected reference ID %s, got %v", refID, tx.ReferenceID)
	}
}

func TestWagerTransaction_PendingReferenceToPending(t *testing.T) {
	tx := mustNewBetTransaction(t)
	tx.MarkPendingReference()

	refID := uuid.New()
	if err := tx.ResolvePendingReference(refID); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.Status != StatusPending {
		t.Errorf("expected PENDING, got %s", tx.Status)
	}
	if tx.ReferenceID == nil || *tx.ReferenceID != refID {
		t.Errorf("expected reference ID %s, got %v", refID, tx.ReferenceID)
	}
}

func TestWagerTransaction_PendingReferenceToRejected(t *testing.T) {
	tx := mustNewBetTransaction(t)
	tx.MarkPendingReference()

	if err := tx.Reject("REFERENCE_NOT_FOUND"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.Status != StatusRejected {
		t.Errorf("expected REJECTED, got %s", tx.Status)
	}
}

func TestWagerTransaction_PendingReferenceToFailed(t *testing.T) {
	tx := mustNewBetTransaction(t)
	tx.MarkPendingReference()

	if err := tx.Fail("TIMEOUT"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.Status != StatusFailed {
		t.Errorf("expected FAILED, got %s", tx.Status)
	}
}

// ---------------------------------------------------------------------------
// Máquina de estados: estados terminais bloqueados
// ---------------------------------------------------------------------------

func TestWagerTransaction_ProcessedIsTerminal(t *testing.T) {
	tx := mustNewBetTransaction(t)
	tx.Process()

	assertTerminalStateBlocked(t, &tx)
}

func TestWagerTransaction_RejectedIsTerminal(t *testing.T) {
	tx := mustNewBetTransaction(t)
	tx.Reject("REASON")

	assertTerminalStateBlocked(t, &tx)
}

func TestWagerTransaction_FailedIsTerminal(t *testing.T) {
	tx := mustNewBetTransaction(t)
	tx.Fail("REASON")

	assertTerminalStateBlocked(t, &tx)
}

// ---------------------------------------------------------------------------
// Máquina de estados: transições inválidas
// ---------------------------------------------------------------------------

func TestWagerTransaction_PendingToMarkPendingReferenceFromNonPending(t *testing.T) {
	tx := mustNewBetTransaction(t)
	tx.MarkPendingReference()

	// Já está em PENDING_REFERENCE, não pode marcar novamente
	if err := tx.MarkPendingReference(); err == nil {
		t.Error("expected error when marking PENDING_REFERENCE from PENDING_REFERENCE")
	}
}

func TestWagerTransaction_ResolveFromNonPendingReference(t *testing.T) {
	tx := mustNewBetTransaction(t) // PENDING

	refID := uuid.New()
	if err := tx.ResolvePendingReference(refID); err == nil {
		t.Error("expected error when resolving from PENDING (not PENDING_REFERENCE)")
	}
}

// ---------------------------------------------------------------------------
// RequiresReference
// ---------------------------------------------------------------------------

func TestWagerTransaction_RequiresReference(t *testing.T) {
	tests := []struct {
		txType   TransactionType
		expected bool
	}{
		{TransactionTypeBet, false},
		{TransactionTypeWin, false},
		{TransactionTypeLoss, false},
		{TransactionTypeOpening, false},
		{TransactionTypeRefund, true},
		{TransactionTypeRollback, true},
	}

	for _, tc := range tests {
		t.Run(string(tc.txType), func(t *testing.T) {
			amount := money.MustNew(0, "BRL")
			if tc.txType != TransactionTypeLoss {
				amount = money.MustNew(1000, "BRL")
			}

			tx, err := NewWagerTransaction(
				OriginExternal, uuid.New(), "player-1",
				strPtr("prov"), strPtr("ext-1"),
				tc.txType, amount, "BRL",
			)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tx.RequiresReference() != tc.expected {
				t.Errorf("RequiresReference() for %s: expected %v, got %v", tc.txType, tc.expected, tx.RequiresReference())
			}
		})
	}
}

// ---------------------------------------------------------------------------
// WithInitialStatus option
// ---------------------------------------------------------------------------

func TestWagerTransaction_WithInitialStatus(t *testing.T) {
	tx, err := NewWagerTransaction(
		OriginExternal, uuid.New(), "player-1",
		strPtr("prov"), strPtr("ext-1"),
		TransactionTypeRefund, money.MustNew(1000, "BRL"), "BRL",
		WithInitialStatus(StatusPendingReference),
		WithReferenceExternalTransactionID("original-ext-tx"),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.Status != StatusPendingReference {
		t.Errorf("expected PENDING_REFERENCE, got %s", tx.Status)
	}
	if *tx.ReferenceExternalTransactionID != "original-ext-tx" {
		t.Errorf("expected original-ext-tx, got %s", *tx.ReferenceExternalTransactionID)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func mustNewBetTransaction(t *testing.T) WagerTransaction {
	t.Helper()
	tx, err := NewWagerTransaction(
		OriginExternal, uuid.New(), "player-1",
		strPtr("provider-abc"), strPtr("ext-001"),
		TransactionTypeBet, money.MustNew(1000, "BRL"), "BRL",
	)
	if err != nil {
		t.Fatalf("failed to create test transaction: %v", err)
	}
	return tx
}

func assertTerminalStateBlocked(t *testing.T, tx *WagerTransaction) {
	t.Helper()

	if err := tx.Process(); err == nil {
		t.Error("expected error on Process() from terminal state")
	} else if !errors.Is(err, ErrTerminalState) {
		t.Errorf("expected ErrTerminalState, got %v", err)
	}

	if err := tx.Reject("X"); err == nil {
		t.Error("expected error on Reject() from terminal state")
	} else if !errors.Is(err, ErrTerminalState) {
		t.Errorf("expected ErrTerminalState, got %v", err)
	}

	if err := tx.Fail("X"); err == nil {
		t.Error("expected error on Fail() from terminal state")
	} else if !errors.Is(err, ErrTerminalState) {
		t.Errorf("expected ErrTerminalState, got %v", err)
	}

	if err := tx.MarkPendingReference(); err == nil {
		t.Error("expected error on MarkPendingReference() from terminal state")
	}
}
