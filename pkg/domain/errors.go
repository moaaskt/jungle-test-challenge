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
)
