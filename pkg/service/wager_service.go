package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
	"github.com/moaaskt/jungle-test-challenge/pkg/money"
	"github.com/moaaskt/jungle-test-challenge/pkg/repository"
)

type WagerService interface {
	OpenWallet(ctx context.Context, playerID, currency string, initialBalance int64) (*domain.Wallet, error)
	// ProcessWager será expandido nas próximas fases para suportar idempotência, outbox e concurrency handling.
	// Por enquanto, executa o fluxo básico síncrono.
	ProcessWager(ctx context.Context, req ProcessWagerRequest) (ProcessWagerResult, error)
}

type ProcessWagerResult struct {
	TransactionID    uuid.UUID
	Status           domain.TransactionStatus
	Balance          money.Money
	IdempotentReplay bool
	RawResponse      []byte // Populated if it's an idempotent replay
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
	pool            *pgxpool.Pool
	walletRepo      repository.WalletRepository
	wagerRepo       repository.WagerTransactionRepository
	ledgerRepo      repository.LedgerRepository
	idempotencyRepo repository.IdempotencyRepository
}

func NewWagerService(
	pool *pgxpool.Pool,
	walletRepo repository.WalletRepository,
	wagerRepo repository.WagerTransactionRepository,
	ledgerRepo repository.LedgerRepository,
	idempotencyRepo repository.IdempotencyRepository,
) WagerService {
	return &wagerService{
		pool:            pool,
		walletRepo:      walletRepo,
		wagerRepo:       wagerRepo,
		ledgerRepo:      ledgerRepo,
		idempotencyRepo: idempotencyRepo,
	}
}

func (s *wagerService) OpenWallet(ctx context.Context, playerID, currency string, initialBalance int64) (*domain.Wallet, error) {
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
		return nil, fmt.Errorf("failed to create wallet: %w", err)
	}

	var entry *domain.WalletLedgerEntry
	var wagerTx *domain.WagerTransaction

	if initialBalance > 0 {
		amt, err := money.New(initialBalance, currency)
		if err != nil {
			return nil, fmt.Errorf("invalid initial balance: %w", err)
		}

		e, err := w.Credit(amt, uuid.Nil)
		if err != nil {
			return nil, fmt.Errorf("failed to credit initial balance: %w", err)
		}

		// Força a versão de volta para 1, conforme regra de abertura
		w.Version = 1

		entry = &e

		txW, err := domain.NewWagerTransaction(
			domain.OriginInternal,
			w.ID,
			playerID,
			nil,
			nil,
			domain.TransactionTypeOpening,
			amt,
			currency,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create opening transaction: %w", err)
		}

		txW.Process()

		// Atualizar o ID da transação no ledger entry
		entry.TransactionID = txW.ID
		wagerTx = &txW
	}

	if err := s.walletRepo.Insert(ctx, tx, &w); err != nil {
		return nil, fmt.Errorf("failed to insert wallet: %w", err)
	}

	if wagerTx != nil {
		if err := s.wagerRepo.Insert(ctx, tx, wagerTx); err != nil {
			return nil, fmt.Errorf("failed to insert opening transaction: %w", err)
		}
		if err := s.ledgerRepo.Insert(ctx, tx, entry); err != nil {
			return nil, fmt.Errorf("failed to insert opening ledger entry: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return &w, nil
}

func (s *wagerService) ProcessWager(ctx context.Context, req ProcessWagerRequest) (ProcessWagerResult, error) {
	// Loop de retry para concorrência na mesma chave de idempotência ou mesmo provider/external_id
	for attempts := 1; attempts <= 5; attempts++ {
		result, err := s.processWagerInternal(ctx, req)
		if err == repository.ErrIdempotencyKeyConflict {
			// Concorrente ganhou no INSERT (ou em idempotency_records ou em wager_transactions).
			// Uma breve pausa de 10ms permite que a transação vencedora finalize o commit se ainda estiver gravando.
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return result, err
	}
	return ProcessWagerResult{}, fmt.Errorf("failed after retries due to idempotency conflicts")
}

func (s *wagerService) processWagerInternal(ctx context.Context, req ProcessWagerRequest) (ProcessWagerResult, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ProcessWagerResult{}, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	idemKey := safeDeref(req.IdempotencyKey)
	payloadHash := safeDeref(req.PayloadHash)

	// 1. Checa idempotência
	if idemKey != "" {
		record, err := s.idempotencyRepo.GetByKey(ctx, tx, idemKey)
		if err != nil {
			return ProcessWagerResult{}, fmt.Errorf("failed to check idempotency key: %w", err)
		}
		if record != nil {
			if record.PayloadHash != payloadHash {
				// Conflito: chave igual, conteúdo diferente
				return ProcessWagerResult{}, fmt.Errorf("idempotency key conflict: hash mismatch") // O handler deve mapear para 409
			}
			// Replay
			return ProcessWagerResult{
				IdempotentReplay: true,
				RawResponse:      record.ResponseBody,
			}, nil
		}
	}

	// 2. Obter a carteira com lock pessimista por linha (SELECT ... FOR UPDATE)
	w, err := s.walletRepo.GetByPlayerAndCurrencyForUpdate(ctx, tx, req.PlayerID, req.Currency)
	if err != nil {
		if err == repository.ErrNotFound {
			return ProcessWagerResult{}, fmt.Errorf("wallet not found for player %s and currency %s", req.PlayerID, req.Currency)
		}
		return ProcessWagerResult{}, fmt.Errorf("failed to get wallet: %w", err)
	}

	// 3. Criar a transação do domínio
	amount, err := money.New(req.Amount, req.Currency)
	if err != nil {
		return ProcessWagerResult{}, fmt.Errorf("invalid amount: %w", err)
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
		domain.WithIdempotency(idemKey, payloadHash),
		domain.WithGameContext(safeDeref(req.RoundID), safeDeref(req.GameID)),
	)
	if err != nil {
		return ProcessWagerResult{}, fmt.Errorf("domain validation failed: %w", err)
	}

	// 4. Aplicar a operação na carteira
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
		return ProcessWagerResult{}, fmt.Errorf("transaction type %s not fully supported in this phase", req.Type)
	}

	if opErr != nil {
		wagerTx.Reject(opErr.Error())
		if err := s.wagerRepo.Insert(ctx, tx, &wagerTx); err != nil {
			if err == repository.ErrProviderExternalConflict {
				if idemKey != "" {
					tx.Rollback(ctx)
					return ProcessWagerResult{}, repository.ErrIdempotencyKeyConflict
				}
				return ProcessWagerResult{}, fmt.Errorf("idempotency key conflict: provider external id already exists with different key")
			}
			return ProcessWagerResult{}, fmt.Errorf("failed to insert rejected transaction: %w", err)
		}

		// Rejeição é decisão de negócio válida: gravar idempotência para replay
		responseMap := map[string]any{
			"transactionId": wagerTx.ID.String(),
			"status":        string(wagerTx.Status),
			"balance": map[string]string{
				"amount":   w.Balance().FormattedAmount(),
				"currency": w.Balance().Currency(),
			},
			"idempotentReplay": false,
		}
		responseBody, _ := json.Marshal(responseMap)

		if idemKey != "" {
			record := &domain.IdempotencyRecord{
				Key:            idemKey,
				PayloadHash:    payloadHash,
				ResponseStatus: 200, // Rejeição de negócio retorna 200 OK com status REJECTED
				ResponseBody:   responseBody,
			}
			if err := s.idempotencyRepo.Insert(ctx, tx, record); err != nil {
				if err == repository.ErrIdempotencyKeyConflict {
					tx.Rollback(ctx)
					return ProcessWagerResult{}, err
				}
				return ProcessWagerResult{}, fmt.Errorf("failed to insert idempotency record for rejected tx: %w", err)
			}
		}

		if err := tx.Commit(ctx); err != nil {
			return ProcessWagerResult{}, fmt.Errorf("failed to commit: %w", err)
		}

		// Retorna resultado REJECTED como resposta válida
		return ProcessWagerResult{
			TransactionID: wagerTx.ID,
			Status:        wagerTx.Status,
			Balance:       w.Balance(),
		}, nil
	}

	wagerTx.Process()

	// 5. Salvar agregados
	if err := s.walletRepo.UpdateBalance(ctx, tx, w); err != nil {
		return ProcessWagerResult{}, fmt.Errorf("failed to update wallet balance: %w", err)
	}

	if err := s.wagerRepo.Insert(ctx, tx, &wagerTx); err != nil {
		if err == repository.ErrProviderExternalConflict {
			if idemKey != "" {
				tx.Rollback(ctx)
				return ProcessWagerResult{}, repository.ErrIdempotencyKeyConflict
			}
			return ProcessWagerResult{}, fmt.Errorf("idempotency key conflict: provider external id already exists with different key") // The handler will map this to 409
		}
		return ProcessWagerResult{}, fmt.Errorf("failed to insert transaction: %w", err)
	}

	if !wagerTx.IsLoss() {
		if err := s.ledgerRepo.Insert(ctx, tx, &entry); err != nil {
			return ProcessWagerResult{}, fmt.Errorf("failed to insert ledger entry: %w", err)
		}
	}

	// 6. Gravar Idempotency Record (dentro da mesma transação SQL)
	// Para gravar a resposta, o handler monta um JSON.
	// Para não acoplar com HTTP, o Service gera a resposta esperada no JSON.
	responseMap := map[string]any{
		"transactionId": wagerTx.ID.String(),
		"status":        string(wagerTx.Status),
		"balance": map[string]string{
			"amount":   w.Balance().FormattedAmount(),
			"currency": w.Balance().Currency(),
		},
		"idempotentReplay": false, // O original é false
	}

	responseBody, _ := json.Marshal(responseMap)

	if idemKey != "" {
		record := &domain.IdempotencyRecord{
			Key:            idemKey,
			PayloadHash:    payloadHash,
			ResponseStatus: 200, // Processed
			ResponseBody:   responseBody,
		}
		if err := s.idempotencyRepo.Insert(ctx, tx, record); err != nil {
			if err == repository.ErrIdempotencyKeyConflict {
				// Rollback imediato
				tx.Rollback(ctx)
				return ProcessWagerResult{}, err // O loop de fora pegará esse erro e tentará denovo
			}
			return ProcessWagerResult{}, fmt.Errorf("failed to insert idempotency record: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return ProcessWagerResult{}, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return ProcessWagerResult{
		TransactionID: wagerTx.ID,
		Status:        wagerTx.Status,
		Balance:       w.Balance(),
	}, nil
}

func safeDeref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
