package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/moaaskt/jungle-test-challenge/pkg/money"
)

// ---------------------------------------------------------------------------
// Constantes de FailureCode estáveis e documentados (Seção 7 do desafio)
// ---------------------------------------------------------------------------

const (
	FailureCodeInsufficientFunds         = "INSUFFICIENT_FUNDS"
	FailureCodeInsufficientFundsRollback = "INSUFFICIENT_FUNDS_FOR_ROLLBACK"
	FailureCodeAlreadyRefunded           = "ALREADY_REFUNDED"
	FailureCodeAlreadyRolledBack         = "ALREADY_ROLLED_BACK"
	FailureCodeOriginalTransactionFailed = "ORIGINAL_TRANSACTION_FAILED"
	FailureCodeReferenceNotFound         = "REFERENCE_NOT_FOUND"
	FailureCodeTransactionTimeout        = "TRANSACTION_TIMEOUT"
)

// ---------------------------------------------------------------------------
// Tipos enumerados
// ---------------------------------------------------------------------------

// TransactionType representa os tipos de operação financeira de apostas.
type TransactionType string

const (
	TransactionTypeOpening  TransactionType = "OPENING"
	TransactionTypeBet      TransactionType = "BET"
	TransactionTypeWin      TransactionType = "WIN"
	TransactionTypeLoss     TransactionType = "LOSS"
	TransactionTypeRefund   TransactionType = "REFUND"
	TransactionTypeRollback TransactionType = "ROLLBACK"
)

// TransactionStatus representa o estado na máquina de estados da transação.
type TransactionStatus string

const (
	StatusPending          TransactionStatus = "PENDING"
	StatusPendingReference TransactionStatus = "PENDING_REFERENCE"
	StatusProcessed        TransactionStatus = "PROCESSED"
	StatusRejected         TransactionStatus = "REJECTED"
	StatusFailed           TransactionStatus = "FAILED"
)

// isTerminal retorna true para estados terminais (não permitem transição).
func (s TransactionStatus) isTerminal() bool {
	switch s {
	case StatusProcessed, StatusRejected, StatusFailed:
		return true
	default:
		return false
	}
}

// Origin distingue se a transação foi gerada pelo sistema ou por um provedor externo.
type Origin string

const (
	OriginInternal Origin = "INTERNAL"
	OriginExternal Origin = "EXTERNAL"
)

// ---------------------------------------------------------------------------
// Tabela de transições válidas
// ---------------------------------------------------------------------------

// validTransitions define quais transições de estado são permitidas.
// A chave é o estado de origem; o valor é o conjunto de estados destino válidos.
var validTransitions = map[TransactionStatus]map[TransactionStatus]bool{
	StatusPending: {
		StatusProcessed: true,
		StatusRejected:  true,
		StatusFailed:    true,
	},
	StatusPendingReference: {
		StatusPending:   true,
		StatusProcessed: true,
		StatusRejected:  true,
		StatusFailed:    true,
	},
	// StatusProcessed, StatusRejected e StatusFailed são terminais — não possuem transições.
}

// ---------------------------------------------------------------------------
// WagerTransaction
// ---------------------------------------------------------------------------

// WagerTransaction representa uma operação financeira de aposta.
// Campos mapeados à seção 6.3 do desafio:
//   - IdempotencyKey / PayloadHash: deduplicação durável
//   - RoundID / GameID: contexto do jogo
//   - ExternalTransactionID: ID externo do provedor para a transação
//   - ReferenceExternalTransactionID: ID externo da transação referenciada (REFUND/ROLLBACK)
//   - ReferenceID: referência interna resolvida (UUID da wager_transaction original)
//   - FailureCode: código de erro quando em estado FAILED/REJECTED
//   - Origin: INTERNAL ou EXTERNAL
type WagerTransaction struct {
	ID       uuid.UUID
	Origin   Origin
	WalletID uuid.UUID
	PlayerID string

	// Identificação do provedor (nullable para origin=INTERNAL)
	ProviderID *string
	ExternalID *string

	// Idempotência
	IdempotencyKey *string
	PayloadHash    *string

	// Contexto do jogo
	RoundID *string
	GameID  *string

	// Referência externa (ex: ID da aposta original que um REFUND referencia)
	ExternalTransactionID          *string
	ReferenceExternalTransactionID *string

	// Referência interna resolvida (UUID da wager_transaction original no nosso banco)
	ReferenceID *uuid.UUID

	Type     TransactionType
	Amount   money.Money
	Currency string
	Status   TransactionStatus

	// Código de falha (preenchido quando status == FAILED ou REJECTED)
	FailureCode *string

	// Rastreamento de tentativas do worker de resolução de referências pendentes
	Attempts     int
	NextAttemptAt *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewWagerTransaction cria uma nova WagerTransaction com validações de domínio.
// Transações externas exigem ProviderID e ExternalID não-nulos.
// LOSS exige amount == 0.
func NewWagerTransaction(
	origin Origin,
	walletID uuid.UUID,
	playerID string,
	providerID *string,
	externalID *string,
	txType TransactionType,
	amount money.Money,
	currency string,
	opts ...WagerTransactionOption,
) (WagerTransaction, error) {

	// Validar regra do LOSS: amount deve ser zero.
	if txType == TransactionTypeLoss && !amount.IsZero() {
		return WagerTransaction{}, ErrZeroAmountRequired
	}

	// Transações externas requerem provider_id e external_id.
	if origin == OriginExternal {
		if providerID == nil || *providerID == "" {
			return WagerTransaction{}, ErrProviderRequired
		}
		if externalID == nil || *externalID == "" {
			return WagerTransaction{}, ErrExternalIDRequired
		}
	}

	now := time.Now()
	tx := WagerTransaction{
		ID:         uuid.New(),
		Origin:     origin,
		WalletID:   walletID,
		PlayerID:   playerID,
		ProviderID: providerID,
		ExternalID: externalID,
		Type:       txType,
		Amount:     amount,
		Currency:   currency,
		Status:     StatusPending,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	for _, opt := range opts {
		opt(&tx)
	}

	return tx, nil
}

// WagerTransactionOption permite configurar campos opcionais via functional options.
type WagerTransactionOption func(*WagerTransaction)

// WithIdempotency define a chave e hash de idempotência.
func WithIdempotency(key, hash string) WagerTransactionOption {
	return func(tx *WagerTransaction) {
		tx.IdempotencyKey = &key
		tx.PayloadHash = &hash
	}
}

// WithGameContext define o RoundID e GameID.
func WithGameContext(roundID, gameID string) WagerTransactionOption {
	return func(tx *WagerTransaction) {
		tx.RoundID = &roundID
		tx.GameID = &gameID
	}
}

// WithExternalTransactionID define o ID externo da transação no provedor.
func WithExternalTransactionID(extTxID string) WagerTransactionOption {
	return func(tx *WagerTransaction) {
		tx.ExternalTransactionID = &extTxID
	}
}

// WithReferenceExternalTransactionID define o ID externo da transação referenciada.
func WithReferenceExternalTransactionID(refExtTxID string) WagerTransactionOption {
	return func(tx *WagerTransaction) {
		tx.ReferenceExternalTransactionID = &refExtTxID
	}
}

// WithReferenceID define a referência interna resolvida (UUID da transação original).
func WithReferenceID(refID uuid.UUID) WagerTransactionOption {
	return func(tx *WagerTransaction) {
		tx.ReferenceID = &refID
	}
}

// WithInitialStatus define o status inicial (ex: PENDING_REFERENCE para referências não-resolvidas).
func WithInitialStatus(status TransactionStatus) WagerTransactionOption {
	return func(tx *WagerTransaction) {
		tx.Status = status
	}
}

// RehydrateWagerTransaction reconstrói uma WagerTransaction a partir de dados persistidos.
// Não aplica validações de domínio — assume que os dados vêm de uma fonte confiável.
func RehydrateWagerTransaction(
	id uuid.UUID,
	origin Origin,
	walletID uuid.UUID,
	playerID string,
	providerID *string,
	externalID *string,
	idempotencyKey *string,
	payloadHash *string,
	roundID *string,
	gameID *string,
	externalTransactionID *string,
	referenceExternalTransactionID *string,
	referenceID *uuid.UUID,
	txType TransactionType,
	amount money.Money,
	currency string,
	status TransactionStatus,
	failureCode *string,
	attempts int,
	nextAttemptAt *time.Time,
	createdAt, updatedAt time.Time,
) WagerTransaction {
	return WagerTransaction{
		ID:                             id,
		Origin:                         origin,
		WalletID:                       walletID,
		PlayerID:                       playerID,
		ProviderID:                     providerID,
		ExternalID:                     externalID,
		IdempotencyKey:                 idempotencyKey,
		PayloadHash:                    payloadHash,
		RoundID:                        roundID,
		GameID:                         gameID,
		ExternalTransactionID:          externalTransactionID,
		ReferenceExternalTransactionID: referenceExternalTransactionID,
		ReferenceID:                    referenceID,
		Type:                           txType,
		Amount:                         amount,
		Currency:                       currency,
		Status:                         status,
		FailureCode:                    failureCode,
		Attempts:                       attempts,
		NextAttemptAt:                  nextAttemptAt,
		CreatedAt:                      createdAt,
		UpdatedAt:                      updatedAt,
	}
}

// ---------------------------------------------------------------------------
// Transições de estado
// ---------------------------------------------------------------------------

// transitionTo aplica a transição de estado com verificação de invariantes.
func (tx *WagerTransaction) transitionTo(target TransactionStatus) error {
	if tx.Status.isTerminal() {
		return fmt.Errorf("%w: %s is a terminal state", ErrTerminalState, tx.Status)
	}

	allowed, exists := validTransitions[tx.Status]
	if !exists || !allowed[target] {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidStateTransition, tx.Status, target)
	}

	tx.Status = target
	tx.UpdatedAt = time.Now()
	return nil
}

// Process transiciona a transação para PROCESSED.
func (tx *WagerTransaction) Process() error {
	return tx.transitionTo(StatusProcessed)
}

// Reject transiciona a transação para REJECTED com um código de falha.
func (tx *WagerTransaction) Reject(failureCode string) error {
	if err := tx.transitionTo(StatusRejected); err != nil {
		return err
	}
	tx.FailureCode = &failureCode
	return nil
}

// Fail transiciona a transação para FAILED com um código de falha.
func (tx *WagerTransaction) Fail(failureCode string) error {
	if err := tx.transitionTo(StatusFailed); err != nil {
		return err
	}
	tx.FailureCode = &failureCode
	return nil
}

// MarkPendingReference transiciona a transação para PENDING_REFERENCE.
// Usado quando REFUND/ROLLBACK chega antes da transação original.
func (tx *WagerTransaction) MarkPendingReference() error {
	// PENDING_REFERENCE é válido apenas a partir de PENDING.
	if tx.Status != StatusPending {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidStateTransition, tx.Status, StatusPendingReference)
	}
	tx.Status = StatusPendingReference
	tx.UpdatedAt = time.Now()
	return nil
}

// ResolvePendingReference transiciona de PENDING_REFERENCE para PENDING,
// permitindo que a transação seja processada normalmente após a referência ser resolvida.
func (tx *WagerTransaction) ResolvePendingReference(referenceID uuid.UUID) error {
	return tx.resolveReference(StatusPending, referenceID)
}

// ResolveAndProcess transiciona de PENDING_REFERENCE diretamente para PROCESSED,
// para quando a referência é resolvida e a transação pode ser imediatamente concluída.
func (tx *WagerTransaction) ResolveAndProcess(referenceID uuid.UUID) error {
	return tx.resolveReference(StatusProcessed, referenceID)
}

func (tx *WagerTransaction) resolveReference(target TransactionStatus, referenceID uuid.UUID) error {
	if tx.Status != StatusPendingReference {
		return fmt.Errorf("%w: %s -> %s (must be PENDING_REFERENCE)", ErrInvalidStateTransition, tx.Status, target)
	}

	allowed := validTransitions[StatusPendingReference]
	if !allowed[target] {
		return fmt.Errorf("%w: PENDING_REFERENCE -> %s", ErrInvalidStateTransition, target)
	}

	tx.ReferenceID = &referenceID
	tx.Status = target
	tx.UpdatedAt = time.Now()
	return nil
}

// IsLoss retorna true se a transação é do tipo LOSS.
// LOSS é especial: exige amount == 0, não gera ledger entry, não incrementa versão do wallet.
func (tx *WagerTransaction) IsLoss() bool {
	return tx.Type == TransactionTypeLoss
}

// RequiresReference retorna true se o tipo da transação requer uma referência
// a outra transação (REFUND e ROLLBACK).
func (tx *WagerTransaction) RequiresReference() bool {
	return tx.Type == TransactionTypeRefund || tx.Type == TransactionTypeRollback
}

// ResolutionDirection retorna "CREDIT" ou "DEBIT" para a resolução de uma PENDING_REFERENCE
// com base no tipo da transação original referenciada.
//
// Regras (Seção 7 do desafio):
//   - REFUND de BET → CREDIT (devolve o débito da aposta)
//   - ROLLBACK de BET → CREDIT (desfaz o débito da aposta)
//   - ROLLBACK de WIN → DEBIT (desfaz o crédito do ganho)
//   - ROLLBACK de REFUND → DEBIT (desfaz o crédito do reembolso)
func (tx *WagerTransaction) ResolutionDirection(originalType TransactionType) string {
	switch {
	case tx.Type == TransactionTypeRefund:
		return "CREDIT"
	case tx.Type == TransactionTypeRollback && originalType == TransactionTypeBet:
		return "CREDIT"
	default:
		// ROLLBACK de WIN, REFUND, ou outros → DEBIT (desfaz crédito)
		return "DEBIT"
	}
}
