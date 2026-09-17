package service

import (
	"context"
	"encoding/json"
	"errors"
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
	ProcessWager(ctx context.Context, req ProcessWagerRequest) (ProcessWagerResult, error)
	ProcessWagerWithInbox(ctx context.Context, req ProcessWagerRequest, inbox domain.InboxRecord) (ProcessWagerResult, error)
	GetWallet(ctx context.Context, walletID uuid.UUID) (*domain.Wallet, error)
	GetTransaction(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error)
	GetTransactionByExternal(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error)
	GetLedger(ctx context.Context, walletID uuid.UUID, currency string, cursor *repository.LedgerCursor, limit int) ([]*domain.WalletLedgerEntry, *repository.LedgerCursor, error)
}

type ProcessWagerResult struct {
	TransactionID    uuid.UUID
	Status           domain.TransactionStatus
	Balance          money.Money
	IdempotentReplay bool
	RawResponse      []byte // Populated if it's an idempotent replay
}

type ProcessWagerRequest struct {
	Origin                         domain.Origin
	PlayerID                       string
	ProviderID                     *string
	ExternalID                     *string
	IdempotencyKey                 *string
	PayloadHash                    *string
	RoundID                        *string
	GameID                         *string
	Type                           domain.TransactionType
	Amount                         int64
	Currency                       string
	ReferenceExternalTransactionID *string
}

type wagerService struct {
	pool            *pgxpool.Pool
	walletRepo      repository.WalletRepository
	wagerRepo       repository.WagerTransactionRepository
	ledgerRepo      repository.LedgerRepository
	idempotencyRepo repository.IdempotencyRepository
	outboxRepo      repository.OutboxRepository
	inboxRepo       repository.InboxRepository
}

func NewWagerService(
	pool *pgxpool.Pool,
	walletRepo repository.WalletRepository,
	wagerRepo repository.WagerTransactionRepository,
	ledgerRepo repository.LedgerRepository,
	idempotencyRepo repository.IdempotencyRepository,
	outboxRepo repository.OutboxRepository,
	inboxRepo repository.InboxRepository,
) WagerService {
	return &wagerService{
		pool:            pool,
		walletRepo:      walletRepo,
		wagerRepo:       wagerRepo,
		ledgerRepo:      ledgerRepo,
		idempotencyRepo: idempotencyRepo,
		outboxRepo:      outboxRepo,
		inboxRepo:       inboxRepo,
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

		// Gravação atômica na outbox (Seção 9 da spec)
		evProcessed, err := domain.NewWagerTransactionProcessedOutboxEvent(wagerTx, wagerTx.ID.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create opening processed outbox event: %w", err)
		}
		if err := s.outboxRepo.Insert(ctx, tx, evProcessed); err != nil {
			return nil, fmt.Errorf("failed to insert opening processed outbox event: %w", err)
		}

		zeroMoney, _ := money.New(0, currency)
		evBalance, err := domain.NewWalletBalanceChangedOutboxEvent(
			w.ID,
			wagerTx.ID,
			"CREDIT",
			entry.Amount,
			zeroMoney,
			entry.BalanceAfter,
			1, // versão na abertura é 1
			wagerTx.ID.String(),
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create opening balance outbox event: %w", err)
		}
		if err := s.outboxRepo.Insert(ctx, tx, evBalance); err != nil {
			return nil, fmt.Errorf("failed to insert opening balance outbox event: %w", err)
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
		if existingTx, err := s.wagerRepo.GetByIdempotencyKey(ctx, tx, idemKey); err == nil && existingTx != nil {
			if existingTx.ProviderID != nil && req.ProviderID != nil && *existingTx.ProviderID != *req.ProviderID {
				return ProcessWagerResult{}, domain.ErrCrossProviderReplay
			}
		}

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
	balanceBefore := w.Balance()

	correlationID := idemKey
	if correlationID == "" {
		if req.ExternalID != nil && *req.ExternalID != "" {
			correlationID = *req.ExternalID
		} else {
			correlationID = wagerTx.ID.String()
		}
	}
	var causationID *string = nil

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

		// Gravar evento de outbox para rejeição de negócio
		evRejected, err := domain.NewWagerTransactionRejectedOutboxEvent(&wagerTx, correlationID, causationID)
		if err != nil {
			return ProcessWagerResult{}, fmt.Errorf("failed to create rejected outbox event: %w", err)
		}
		if err := s.outboxRepo.Insert(ctx, tx, evRejected); err != nil {
			return ProcessWagerResult{}, fmt.Errorf("failed to insert rejected outbox event: %w", err)
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
	if !wagerTx.IsLoss() {
		if err := s.walletRepo.UpdateBalance(ctx, tx, w); err != nil {
			return ProcessWagerResult{}, fmt.Errorf("failed to update wallet balance: %w", err)
		}
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

	// 6. Gravar eventos de Outbox (WagerTransactionProcessed e WalletBalanceChanged)
	evProcessed, err := domain.NewWagerTransactionProcessedOutboxEvent(&wagerTx, correlationID, causationID)
	if err != nil {
		return ProcessWagerResult{}, fmt.Errorf("failed to create processed outbox event: %w", err)
	}
	if err := s.outboxRepo.Insert(ctx, tx, evProcessed); err != nil {
		return ProcessWagerResult{}, fmt.Errorf("failed to insert processed outbox event: %w", err)
	}

	if !wagerTx.IsLoss() {
		dir := "DEBIT"
		if req.Type == domain.TransactionTypeWin {
			dir = "CREDIT"
		}
		evBalance, err := domain.NewWalletBalanceChangedOutboxEvent(
			w.ID,
			wagerTx.ID,
			dir,
			amount,
			balanceBefore,
			w.Balance(),
			w.Version,
			correlationID,
			causationID,
		)
		if err != nil {
			return ProcessWagerResult{}, fmt.Errorf("failed to create balance changed outbox event: %w", err)
		}
		if err := s.outboxRepo.Insert(ctx, tx, evBalance); err != nil {
			return ProcessWagerResult{}, fmt.Errorf("failed to insert balance changed outbox event: %w", err)
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

func (s *wagerService) ProcessWagerWithInbox(ctx context.Context, req ProcessWagerRequest, inbox domain.InboxRecord) (ProcessWagerResult, error) {
	for attempts := 1; attempts <= 5; attempts++ {
		result, err := s.processWagerWithInboxInternal(ctx, req, inbox)
		if errors.Is(err, repository.ErrIdempotencyKeyConflict) {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return result, err
	}
	return ProcessWagerResult{}, fmt.Errorf("failed after retries due to idempotency conflicts")
}

func (s *wagerService) processWagerWithInboxInternal(ctx context.Context, req ProcessWagerRequest, inbox domain.InboxRecord) (ProcessWagerResult, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ProcessWagerResult{}, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// 1. Checagem de Inbox (deduplicação a nível de mensageria SQS / consumer)
	existingInbox, err := s.inboxRepo.Get(ctx, tx, inbox.ConsumerName, inbox.MessageID)
	if err != nil {
		return ProcessWagerResult{}, fmt.Errorf("failed to check inbox: %w", err)
	}
	if existingInbox != nil {
		// Reentrega pós-commit detectada! O consumidor deve expurgar a mensagem do SQS.
		return ProcessWagerResult{
			IdempotentReplay: true,
		}, nil
	}

	// Inserir registro na Inbox dentro da mesma transação atômica
	if err := s.inboxRepo.Insert(ctx, tx, &inbox); err != nil {
		if errors.Is(err, repository.ErrInboxDuplicateMessage) {
			return ProcessWagerResult{
				IdempotentReplay: true,
			}, nil
		}
		return ProcessWagerResult{}, fmt.Errorf("failed to insert inbox record: %w", err)
	}

	idemKey := safeDeref(req.IdempotencyKey)
	payloadHash := safeDeref(req.PayloadHash)

	// 2. Checagem de Idempotência da regra de negócio (se fornecida)
	if idemKey != "" {
		if existingTx, err := s.wagerRepo.GetByIdempotencyKey(ctx, tx, idemKey); err == nil && existingTx != nil {
			if existingTx.ProviderID != nil && req.ProviderID != nil && *existingTx.ProviderID != *req.ProviderID {
				return ProcessWagerResult{}, domain.ErrCrossProviderReplay
			}
		}

		record, err := s.idempotencyRepo.GetByKey(ctx, tx, idemKey)
		if err != nil {
			return ProcessWagerResult{}, fmt.Errorf("failed to check idempotency key: %w", err)
		}
		if record != nil {
			if record.PayloadHash != payloadHash {
				return ProcessWagerResult{}, fmt.Errorf("idempotency key conflict: hash mismatch")
			}
			// Replay a nível de negócio: comita a inserção da inbox para evitar re-verificações futuras
			if err := tx.Commit(ctx); err != nil {
				return ProcessWagerResult{}, fmt.Errorf("failed to commit inbox for idempotent replay: %w", err)
			}
			return ProcessWagerResult{
				IdempotentReplay: true,
				RawResponse:      record.ResponseBody,
			}, nil
		}
	}

	// 3. Obter a carteira com lock pessimista por linha (SELECT ... FOR UPDATE)
	w, err := s.walletRepo.GetByPlayerAndCurrencyForUpdate(ctx, tx, req.PlayerID, req.Currency)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return ProcessWagerResult{}, fmt.Errorf("wallet not found for player %s and currency %s", req.PlayerID, req.Currency)
		}
		return ProcessWagerResult{}, fmt.Errorf("failed to get wallet: %w", err)
	}

	correlationID := idemKey
	if correlationID == "" {
		correlationID = inbox.MessageID
	}
	var causationID *string = nil

	// 4. Tratar mensagens fora de ordem: REFUND / ROLLBACK sem transação original existente
	if req.Type == domain.TransactionTypeRefund || req.Type == domain.TransactionTypeRollback {
		var refTx *domain.WagerTransaction
		if req.ReferenceExternalTransactionID != nil && *req.ReferenceExternalTransactionID != "" && req.ProviderID != nil {
			refTx, err = s.wagerRepo.GetByProviderAndExternalID(ctx, tx, *req.ProviderID, *req.ReferenceExternalTransactionID)
			if err != nil && !errors.Is(err, repository.ErrNotFound) {
				return ProcessWagerResult{}, fmt.Errorf("failed to check reference transaction: %w", err)
			}
		}

		if refTx == nil {
			// A aposta original ainda não chegou: persistir como PENDING_REFERENCE
			opts := []domain.WagerTransactionOption{
				domain.WithInitialStatus(domain.StatusPendingReference),
				domain.WithIdempotency(idemKey, payloadHash),
				domain.WithGameContext(safeDeref(req.RoundID), safeDeref(req.GameID)),
			}
			if req.ReferenceExternalTransactionID != nil {
				opts = append(opts, domain.WithReferenceExternalTransactionID(*req.ReferenceExternalTransactionID))
			}
			if req.ExternalID != nil {
				opts = append(opts, domain.WithExternalTransactionID(*req.ExternalID))
			}

			amount, _ := money.New(req.Amount, req.Currency)
			pendingTx, err := domain.NewWagerTransaction(
				req.Origin,
				w.ID,
				req.PlayerID,
				req.ProviderID,
				req.ExternalID,
				req.Type,
				amount,
				req.Currency,
				opts...,
			)
			if err != nil {
				return ProcessWagerResult{}, fmt.Errorf("failed to create pending reference wager tx: %w", err)
			}

			if err := s.wagerRepo.Insert(ctx, tx, &pendingTx); err != nil {
				if errors.Is(err, repository.ErrProviderExternalConflict) {
					return ProcessWagerResult{}, repository.ErrIdempotencyKeyConflict
				}
				return ProcessWagerResult{}, fmt.Errorf("failed to insert pending reference tx: %w", err)
			}

			// Gravar evento de Outbox WagerTransactionPendingReference
			evPending, err := domain.NewWagerTransactionPendingReferenceOutboxEvent(
				&pendingTx,
				correlationID,
				causationID,
			)
			if err != nil {
				return ProcessWagerResult{}, fmt.Errorf("failed to create pending reference outbox event: %w", err)
			}
			if err := s.outboxRepo.Insert(ctx, tx, evPending); err != nil {
				return ProcessWagerResult{}, fmt.Errorf("failed to insert pending reference outbox event: %w", err)
			}

			// Commit atômico (inclui a Inbox!)
			if err := tx.Commit(ctx); err != nil {
				return ProcessWagerResult{}, fmt.Errorf("failed to commit pending reference transaction: %w", err)
			}

			return ProcessWagerResult{
				TransactionID: pendingTx.ID,
				Status:        domain.StatusPendingReference,
				Balance:       w.Balance(),
			}, nil
		}
	}

	// 5. Criar a transação do domínio normal
	amount, err := money.New(req.Amount, req.Currency)
	if err != nil {
		return ProcessWagerResult{}, fmt.Errorf("invalid amount: %w", err)
	}

	opts := []domain.WagerTransactionOption{
		domain.WithIdempotency(idemKey, payloadHash),
		domain.WithGameContext(safeDeref(req.RoundID), safeDeref(req.GameID)),
	}
	if req.ReferenceExternalTransactionID != nil {
		opts = append(opts, domain.WithReferenceExternalTransactionID(*req.ReferenceExternalTransactionID))
	}
	if req.ExternalID != nil {
		opts = append(opts, domain.WithExternalTransactionID(*req.ExternalID))
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
		opts...,
	)
	if err != nil {
		return ProcessWagerResult{}, fmt.Errorf("domain validation failed: %w", err)
	}

	// 6. Aplicar operação na carteira
	var entry domain.WalletLedgerEntry
	var opErr error
	balanceBefore := w.Balance()

	switch req.Type {
	case domain.TransactionTypeBet:
		entry, opErr = w.Debit(amount, wagerTx.ID)
	case domain.TransactionTypeWin, domain.TransactionTypeRefund, domain.TransactionTypeRollback:
		entry, opErr = w.Credit(amount, wagerTx.ID)
	case domain.TransactionTypeLoss:
		// LOSS não altera saldo
	default:
		return ProcessWagerResult{}, fmt.Errorf("transaction type %s not supported", req.Type)
	}

	if opErr != nil {
		// Rejeição por regra de negócio (ex: saldo insuficiente)
		wagerTx.Reject(opErr.Error())
		if err := s.wagerRepo.Insert(ctx, tx, &wagerTx); err != nil {
			if errors.Is(err, repository.ErrProviderExternalConflict) {
				return ProcessWagerResult{}, repository.ErrIdempotencyKeyConflict
			}
			return ProcessWagerResult{}, fmt.Errorf("failed to insert rejected transaction: %w", err)
		}

		evRejected, err := domain.NewWagerTransactionRejectedOutboxEvent(&wagerTx, correlationID, causationID)
		if err != nil {
			return ProcessWagerResult{}, fmt.Errorf("failed to create rejected outbox event: %w", err)
		}
		if err := s.outboxRepo.Insert(ctx, tx, evRejected); err != nil {
			return ProcessWagerResult{}, fmt.Errorf("failed to insert rejected outbox event: %w", err)
		}

		if idemKey != "" {
			responseMap := map[string]any{
				"transactionId":    wagerTx.ID.String(),
				"status":           string(wagerTx.Status),
				"balance":          map[string]string{"amount": w.Balance().FormattedAmount(), "currency": w.Balance().Currency()},
				"idempotentReplay": false,
			}
			responseBody, _ := json.Marshal(responseMap)
			record := &domain.IdempotencyRecord{
				Key:            idemKey,
				PayloadHash:    payloadHash,
				ResponseStatus: 200,
				ResponseBody:   responseBody,
			}
			if err := s.idempotencyRepo.Insert(ctx, tx, record); err != nil {
				if errors.Is(err, repository.ErrIdempotencyKeyConflict) {
					return ProcessWagerResult{}, err
				}
				return ProcessWagerResult{}, fmt.Errorf("failed to insert idempotency record for rejected tx: %w", err)
			}
		}

		if err := tx.Commit(ctx); err != nil {
			return ProcessWagerResult{}, fmt.Errorf("failed to commit rejected tx: %w", err)
		}

		return ProcessWagerResult{
			TransactionID: wagerTx.ID,
			Status:        wagerTx.Status,
			Balance:       w.Balance(),
		}, nil
	}

	wagerTx.Process()

	// 7. Salvar agregados
	if !wagerTx.IsLoss() {
		if err := s.walletRepo.UpdateBalance(ctx, tx, w); err != nil {
			return ProcessWagerResult{}, fmt.Errorf("failed to update wallet balance: %w", err)
		}
	}

	if err := s.wagerRepo.Insert(ctx, tx, &wagerTx); err != nil {
		if errors.Is(err, repository.ErrProviderExternalConflict) {
			return ProcessWagerResult{}, repository.ErrIdempotencyKeyConflict
		}
		return ProcessWagerResult{}, fmt.Errorf("failed to insert transaction: %w", err)
	}

	if !wagerTx.IsLoss() {
		if err := s.ledgerRepo.Insert(ctx, tx, &entry); err != nil {
			return ProcessWagerResult{}, fmt.Errorf("failed to insert ledger entry: %w", err)
		}
	}

	// 8. Gravar eventos de Outbox
	evProcessed, err := domain.NewWagerTransactionProcessedOutboxEvent(&wagerTx, correlationID, causationID)
	if err != nil {
		return ProcessWagerResult{}, fmt.Errorf("failed to create processed outbox event: %w", err)
	}
	if err := s.outboxRepo.Insert(ctx, tx, evProcessed); err != nil {
		return ProcessWagerResult{}, fmt.Errorf("failed to insert processed outbox event: %w", err)
	}

	if !wagerTx.IsLoss() {
		dir := "DEBIT"
		if req.Type == domain.TransactionTypeWin || req.Type == domain.TransactionTypeRefund || req.Type == domain.TransactionTypeRollback {
			dir = "CREDIT"
		}
		evBalance, err := domain.NewWalletBalanceChangedOutboxEvent(
			w.ID,
			wagerTx.ID,
			dir,
			amount,
			balanceBefore,
			w.Balance(),
			w.Version,
			correlationID,
			causationID,
		)
		if err != nil {
			return ProcessWagerResult{}, fmt.Errorf("failed to create balance changed outbox event: %w", err)
		}
		if err := s.outboxRepo.Insert(ctx, tx, evBalance); err != nil {
			return ProcessWagerResult{}, fmt.Errorf("failed to insert balance changed outbox event: %w", err)
		}
	}

	// -----------------------------------------------------------------------
	// Resolução inline de referências pendentes (Seção 6.5 — Momento A)
	// Se esta transação recém processada é referência de algum REFUND/ROLLBACK
	// que chegou antes (PENDING_REFERENCE), resolvemos atomicamente aqui.
	// -----------------------------------------------------------------------
	if req.ExternalID != nil && req.ProviderID != nil {
		if err := s.resolvePendingRefs(ctx, tx, w, &wagerTx, correlationID, causationID); err != nil {
			return ProcessWagerResult{}, fmt.Errorf("failed to resolve pending refs inline: %w", err)
		}
	}

	// 9. Gravar Idempotência de negócio
	if idemKey != "" {
		responseMap := map[string]any{
			"transactionId":    wagerTx.ID.String(),
			"status":           string(wagerTx.Status),
			"balance":          map[string]string{"amount": w.Balance().FormattedAmount(), "currency": w.Balance().Currency()},
			"idempotentReplay": false,
		}
		responseBody, _ := json.Marshal(responseMap)
		record := &domain.IdempotencyRecord{
			Key:            idemKey,
			PayloadHash:    payloadHash,
			ResponseStatus: 200,
			ResponseBody:   responseBody,
		}
		if err := s.idempotencyRepo.Insert(ctx, tx, record); err != nil {
			if errors.Is(err, repository.ErrIdempotencyKeyConflict) {
				return ProcessWagerResult{}, err
			}
			return ProcessWagerResult{}, fmt.Errorf("failed to insert idempotency record: %w", err)
		}
	}

	// Commit atômico final (inclui domínio, ledger, outbox e inbox!)
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

// ---------------------------------------------------------------------------
// Resolução de Referências Pendentes (Seção 6.5 / 7 do desafio)
// ---------------------------------------------------------------------------

// resolvePendingRefs busca e resolve todas as transações PENDING_REFERENCE que
// referenciam a transação original recém-processada. Usado no Momento A (inline).
func (s *wagerService) resolvePendingRefs(
	ctx context.Context, tx pgx.Tx,
	w *domain.Wallet, originalTx *domain.WagerTransaction,
	correlationID string, causationID *string,
) error {
	providerID := safeDeref(originalTx.ProviderID)
	externalID := safeDeref(originalTx.ExternalID)
	if providerID == "" || externalID == "" {
		return nil
	}

	pendingRefs, err := s.wagerRepo.GetPendingByExternalReference(ctx, tx, providerID, externalID)
	if err != nil {
		return fmt.Errorf("failed to query pending references: %w", err)
	}

	for _, pendingTx := range pendingRefs {
		if err := s.resolveSinglePendingRef(ctx, tx, w, pendingTx, originalTx, correlationID, causationID); err != nil {
			return fmt.Errorf("failed to resolve pending reference %s: %w", pendingTx.ID, err)
		}
	}
	return nil
}

// resolveSinglePendingRef resolve uma única transação PENDING_REFERENCE.
// Aplica todas as regras da Seção 7 do desafio:
//   1. Guarda contra dupla reversão (ALREADY_REFUNDED / ALREADY_ROLLED_BACK)
//   2. Referência original em REJECTED/FAILED → rejeitar com ORIGINAL_TRANSACTION_FAILED
//   3. Direção correta: ROLLBACK de WIN é DEBIT (verifica saldo)
//   4. Saldo insuficiente para rollback → INSUFFICIENT_FUNDS_FOR_ROLLBACK
func (s *wagerService) resolveSinglePendingRef(
	ctx context.Context, tx pgx.Tx,
	w *domain.Wallet,
	pendingTx *domain.WagerTransaction,
	originalTx *domain.WagerTransaction,
	correlationID string, causationID *string,
) error {
	// 1. Se originalTx é nil, é TTL expirado — rejeitar com REFERENCE_NOT_FOUND
	if originalTx == nil {
		pendingTx.Reject(domain.FailureCodeReferenceNotFound)
		if err := s.wagerRepo.Update(ctx, tx, pendingTx); err != nil {
			return fmt.Errorf("failed to update expired pending ref: %w", err)
		}
		evRejected, err := domain.NewWagerTransactionRejectedOutboxEvent(pendingTx, correlationID, causationID)
		if err != nil {
			return fmt.Errorf("failed to create rejected outbox event for expired ref: %w", err)
		}
		return s.outboxRepo.Insert(ctx, tx, evRejected)
	}

	// 2. Referência original em REJECTED ou FAILED → rejeitar
	if originalTx.Status == domain.StatusRejected || originalTx.Status == domain.StatusFailed {
		pendingTx.Reject(domain.FailureCodeOriginalTransactionFailed)
		if err := s.wagerRepo.Update(ctx, tx, pendingTx); err != nil {
			return fmt.Errorf("failed to update pending ref with failed original: %w", err)
		}
		evRejected, err := domain.NewWagerTransactionRejectedOutboxEvent(pendingTx, correlationID, causationID)
		if err != nil {
			return fmt.Errorf("failed to create rejected outbox event: %w", err)
		}
		return s.outboxRepo.Insert(ctx, tx, evRejected)
	}

	// 3. Guarda contra dupla reversão
	alreadyReversed, err := s.wagerRepo.HasProcessedReversal(ctx, tx, originalTx.ID, pendingTx.Type)
	if err != nil {
		return fmt.Errorf("failed to check processed reversal: %w", err)
	}
	if alreadyReversed {
		failureCode := domain.FailureCodeAlreadyRefunded
		if pendingTx.Type == domain.TransactionTypeRollback {
			failureCode = domain.FailureCodeAlreadyRolledBack
		}
		pendingTx.Reject(failureCode)
		if err := s.wagerRepo.Update(ctx, tx, pendingTx); err != nil {
			return fmt.Errorf("failed to update duplicate reversal: %w", err)
		}
		evRejected, err := domain.NewWagerTransactionRejectedOutboxEvent(pendingTx, correlationID, causationID)
		if err != nil {
			return fmt.Errorf("failed to create rejected outbox event for duplicate reversal: %w", err)
		}
		return s.outboxRepo.Insert(ctx, tx, evRejected)
	}

	// 4. Determinar direção financeira do movimento
	direction := pendingTx.ResolutionDirection(originalTx.Type)
	balanceBefore := w.Balance()

	var entry domain.WalletLedgerEntry
	var opErr error

	switch direction {
	case "CREDIT":
		entry, opErr = w.Credit(pendingTx.Amount, pendingTx.ID)
	case "DEBIT":
		entry, opErr = w.Debit(pendingTx.Amount, pendingTx.ID)
		if errors.Is(opErr, domain.ErrInsufficientFunds) {
			// Código diferenciado para rollback sem saldo (Seção 7)
			pendingTx.Reject(domain.FailureCodeInsufficientFundsRollback)
			if err := s.wagerRepo.Update(ctx, tx, pendingTx); err != nil {
				return fmt.Errorf("failed to update rollback with insufficient funds: %w", err)
			}
			evRejected, err := domain.NewWagerTransactionRejectedOutboxEvent(pendingTx, correlationID, causationID)
			if err != nil {
				return fmt.Errorf("failed to create rejected outbox event for insufficient rollback: %w", err)
			}
			return s.outboxRepo.Insert(ctx, tx, evRejected)
		}
	}
	if opErr != nil {
		return fmt.Errorf("failed to apply %s for pending ref: %w", direction, opErr)
	}

	// 5. Resolução bem-sucedida
	if err := pendingTx.ResolveAndProcess(originalTx.ID); err != nil {
		return fmt.Errorf("failed to resolve and process pending ref: %w", err)
	}

	if err := s.walletRepo.UpdateBalance(ctx, tx, w); err != nil {
		return fmt.Errorf("failed to update wallet balance after resolution: %w", err)
	}
	if err := s.wagerRepo.Update(ctx, tx, pendingTx); err != nil {
		return fmt.Errorf("failed to update resolved pending ref: %w", err)
	}
	if err := s.ledgerRepo.Insert(ctx, tx, &entry); err != nil {
		return fmt.Errorf("failed to insert ledger entry for resolved ref: %w", err)
	}

	// Emitir eventos de Outbox
	evProcessed, err := domain.NewWagerTransactionProcessedOutboxEvent(pendingTx, correlationID, causationID)
	if err != nil {
		return fmt.Errorf("failed to create processed outbox event for resolved ref: %w", err)
	}
	if err := s.outboxRepo.Insert(ctx, tx, evProcessed); err != nil {
		return fmt.Errorf("failed to insert processed outbox event for resolved ref: %w", err)
	}

	evBalance, err := domain.NewWalletBalanceChangedOutboxEvent(
		w.ID,
		pendingTx.ID,
		direction,
		pendingTx.Amount,
		balanceBefore,
		w.Balance(),
		w.Version,
		correlationID,
		causationID,
	)
	if err != nil {
		return fmt.Errorf("failed to create balance changed outbox event for resolved ref: %w", err)
	}
	if err := s.outboxRepo.Insert(ctx, tx, evBalance); err != nil {
		return fmt.Errorf("failed to insert balance changed outbox event for resolved ref: %w", err)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Métodos de Consulta (Seção 9 do desafio)
// ---------------------------------------------------------------------------

func (s *wagerService) GetWallet(ctx context.Context, walletID uuid.UUID) (*domain.Wallet, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("failed to begin read-only transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	w, err := s.walletRepo.GetByID(ctx, tx, walletID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit read-only transaction: %w", err)
	}
	return w, nil
}

func (s *wagerService) GetTransaction(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("failed to begin read-only transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	wt, err := s.wagerRepo.GetByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit read-only transaction: %w", err)
	}
	return wt, nil
}

func (s *wagerService) GetTransactionByExternal(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("failed to begin read-only transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	wt, err := s.wagerRepo.GetByProviderAndExternalID(ctx, tx, providerID, externalID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit read-only transaction: %w", err)
	}
	return wt, nil
}

func (s *wagerService) GetLedger(ctx context.Context, walletID uuid.UUID, currency string, cursor *repository.LedgerCursor, limit int) ([]*domain.WalletLedgerEntry, *repository.LedgerCursor, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to begin read-only transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	entries, nextCursor, err := s.ledgerRepo.GetByWalletID(ctx, tx, walletID, currency, cursor, limit)
	if err != nil {
		return nil, nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, nil, fmt.Errorf("failed to commit read-only transaction: %w", err)
	}
	return entries, nextCursor, nil
}

