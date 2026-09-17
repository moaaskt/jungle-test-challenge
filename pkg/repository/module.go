package repository

import "go.uber.org/fx"

// Module expõe os repositórios para o Fx.
var Module = fx.Provide(
	NewWalletRepository,
	NewWagerTransactionRepository,
	NewLedgerRepository,
	NewIdempotencyRepository,
	NewOutboxRepository,
)
