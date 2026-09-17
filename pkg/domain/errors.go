package domain

import "errors"

var (
	// ErrInsufficientFunds é retornado quando um débito resultaria em saldo negativo.
	ErrInsufficientFunds = errors.New("insufficient funds: debit would result in negative balance")

	// ErrCurrencyMismatch é retornado quando a moeda da operação difere da moeda da carteira.
	ErrCurrencyMismatch = errors.New("currency mismatch: operation currency differs from wallet currency")

	// ErrInvalidStateTransition é retornado quando uma transição de estado inválida é tentada.
	ErrInvalidStateTransition = errors.New("invalid state transition")

	// ErrTerminalState é retornado quando se tenta transicionar a partir de um estado terminal.
	ErrTerminalState = errors.New("cannot transition from terminal state")

	// ErrInvalidVersion é retornado quando a versão do wallet está inconsistente.
	ErrInvalidVersion = errors.New("invalid wallet version")

	// ErrZeroAmountRequired é retornado quando o tipo LOSS exige amount == 0.
	ErrZeroAmountRequired = errors.New("LOSS transactions require amount to be zero")

	// ErrNonZeroAmountRequired é retornado quando o tipo da transação exige amount > 0.
	ErrNonZeroAmountRequired = errors.New("transaction type requires a positive amount")

	// ErrNilWallet é retornado quando uma operação recebe um wallet nulo.
	ErrNilWallet = errors.New("wallet must not be nil")

	// ErrProviderRequired é retornado quando origin é EXTERNAL mas provider_id está ausente.
	ErrProviderRequired = errors.New("external transactions require a provider_id")

	// ErrExternalIDRequired é retornado quando origin é EXTERNAL mas external_id está ausente.
	ErrExternalIDRequired = errors.New("external transactions require an external_id")

	// ErrInsufficientFundsForRollback é retornado quando um ROLLBACK de WIN precisaria
	// debitar mais do que o saldo disponível. Código diferenciado de ErrInsufficientFunds.
	ErrInsufficientFundsForRollback = errors.New("insufficient funds for rollback: debit would result in negative balance")

	// ErrAlreadyRefunded é retornado quando já existe um REFUND processado para a referência.
	ErrAlreadyRefunded = errors.New("transaction has already been refunded")

	// ErrAlreadyRolledBack é retornado quando já existe um ROLLBACK processado para a referência.
	ErrAlreadyRolledBack = errors.New("transaction has already been rolled back")

	// ErrOriginalTransactionFailed é retornado quando a transação referenciada está em REJECTED ou FAILED.
	ErrOriginalTransactionFailed = errors.New("original transaction is rejected or failed")

	// ErrReferenceNotFound é retornado quando o TTL ou máximo de tentativas expiram sem a referência aparecer.
	ErrReferenceNotFound = errors.New("reference transaction not found within TTL")

	// ErrCrossProviderReplay é retornado quando uma requisição tenta reutilizar chave de idempotência de outro provedor.
	ErrCrossProviderReplay = errors.New("forbidden: cannot replay transaction belonging to another provider")
)

