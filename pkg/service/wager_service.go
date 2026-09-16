package service

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
	"github.com/moaaskt/jungle-test-challenge/pkg/money"
	"github.com/moaaskt/jungle-test-challenge/pkg/repository"
)

type WagerService interface {
	OpenWallet(ctx context.Context, playerID, currency string) (*domain.Wallet, error)
	// ProcessWager será expandido nas próximas fases para suportar idempotência, outbox e concurrency handling.
	// Por enquanto, executa o fluxo básico síncrono.
	ProcessWager(ctx context.Context, req ProcessWagerRequest) (*domain.WagerTransaction, error)
}

type ProcessWagerRequest struct {
	Origin         domain.Origin
	PlayerID       string
	ProviderID     *string
	ExternalID     *string
	IdempotencyKey *string
	PayloadHash    *string
	RoundID        *string
	GameID         *string
	Type           domain.TransactionType
	Amount         int64
	Currency       string
}

type wagerService struct {
	pool       *pgxpool.Pool
	walletRepo repository.WalletRepository
	wagerRepo  repository.WagerTransactionRepository
	ledgerRepo repository.LedgerRepository
}

func NewWagerService(
	pool *pgxpool.Pool,
	walletRepo repository.WalletRepository,
	wagerRepo repository.WagerTransactionRepository,
	ledgerRepo repository.LedgerRepository,
) WagerService {
	return &wagerService{
		pool:       pool,
		walletRepo: walletRepo,
		wagerRepo:  wagerRepo,
		ledgerRepo: ledgerRepo,
	}
}

func (s *wagerService) OpenWallet(ctx context.Context, playerID, currency string) (*domain.Wallet, error) {
	// Inicia transação SQL (Unit of Work)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Verifica se já existe
	existing, err := s.walletRepo.GetByPlayerAndCurrency(ctx, tx, playerID, currency)
	if err != nil && err != repository.ErrNotFound {
		return nil, fmt.Errorf("failed to check existing wallet: %w", err)
	}
	if existing != nil {
		return nil, repository.ErrWalletAlreadyExists
	}

	w, err := domain.NewWallet(playerID, currency)
	if err != nil {
		return nil, fmt.Errorf("domain validation failed: %w", err)
	}

	if err := s.walletRepo.Insert(ctx, tx, &w); err != nil {
		return nil, fmt.Errorf("failed to insert wallet: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return &w, nil
}

func (s *wagerService) ProcessWager(ctx context.Context, req ProcessWagerRequest) (*domain.WagerTransaction, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// 1. Obter a carteira (nesta fase, assumimos que a carteira já existe)
	w, err := s.walletRepo.GetByPlayerAndCurrency(ctx, tx, req.PlayerID, req.Currency)
	if err != nil {
		if err == repository.ErrNotFound {
			return nil, fmt.Errorf("wallet not found for player %s and currency %s", req.PlayerID, req.Currency)
		}
		return nil, fmt.Errorf("failed to get wallet: %w", err)
	}

	// 2. Criar a transação do domínio
	amount, err := money.New(req.Amount, req.Currency)
	if err != nil {
		return nil, fmt.Errorf("invalid amount: %w", err)
	}

	wagerTx, err := domain.NewWagerTransaction(
		req.Origin,
		w.ID,
		req.PlayerID,
		req.ProviderID,
		req.ExternalID,
		req.Type,
		amount,
		req.Currency,
		domain.WithIdempotency(safeDeref(req.IdempotencyKey), safeDeref(req.PayloadHash)),
		domain.WithGameContext(safeDeref(req.RoundID), safeDeref(req.GameID)),
	)
	if err != nil {
		return nil, fmt.Errorf("domain validation failed: %w", err)
	}

	// 3. Aplicar a operação na carteira (Debitar ou Creditar dependendo do tipo)
	var entry domain.WalletLedgerEntry
	var opErr error

	switch req.Type {
	case domain.TransactionTypeBet:
		entry, opErr = w.Debit(amount, wagerTx.ID)
	case domain.TransactionTypeWin:
		entry, opErr = w.Credit(amount, wagerTx.ID)
	case domain.TransactionTypeLoss:
		// LOSS não altera saldo nem gera ledger entry (regra de domínio)
	default:
		// Para REFUND/ROLLBACK, lógica será implementada na fase 8. 
		// Por ora, vamos retornar um erro de não implementado.
		return nil, fmt.Errorf("transaction type %s not fully supported in this phase", req.Type)
	}

	if opErr != nil {
		wagerTx.Reject(opErr.Error())
		// Salva apenas a transação rejeitada
		if err := s.wagerRepo.Insert(ctx, tx, &wagerTx); err != nil {
			return nil, fmt.Errorf("failed to insert rejected transaction: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed to commit: %w", err)
		}
		return &wagerTx, opErr // Retorna o erro de domínio para o handler
	}

	wagerTx.Process()

	// 4. Salvar tudo
	if err := s.walletRepo.UpdateBalance(ctx, tx, w); err != nil {
		return nil, fmt.Errorf("failed to update wallet balance: %w", err)
	}

	if err := s.wagerRepo.Insert(ctx, tx, &wagerTx); err != nil {
		return nil, fmt.Errorf("failed to insert transaction: %w", err)
	}

	// Se for LOSS, entry.ID será nil (zero UUID)
	if !wagerTx.IsLoss() {
		if err := s.ledgerRepo.Insert(ctx, tx, &entry); err != nil {
			return nil, fmt.Errorf("failed to insert ledger entry: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return &wagerTx, nil
}

func safeDeref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
