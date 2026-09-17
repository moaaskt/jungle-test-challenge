package test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
	"github.com/moaaskt/jungle-test-challenge/pkg/messaging"
	"github.com/moaaskt/jungle-test-challenge/pkg/repository"
	"github.com/moaaskt/jungle-test-challenge/pkg/service"
)

// TestPendingRef_InlineResolution_MomentA valida o fluxo onde o REFUND chega ANTES do BET.
// 1. REFUND chega via SQS -> gravado como PENDING_REFERENCE (sem BET ainda).
// 2. BET chega via SQS -> processa o BET (DEBIT) e no mesmo commit resolve o REFUND (CREDIT).
func TestPendingRef_InlineResolution_MomentA(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	cfg := getTestSQSConfig()
	sqsClient, err := messaging.NewSQSClient(cfg)
	if err != nil {
		t.Fatalf("failed to create SQS client: %v", err)
	}

	provisioner := messaging.NewQueueProvisioner(sqsClient, cfg)
	ctx := context.Background()
	queues, err := provisioner.EnsureQueues(ctx)
	if err != nil {
		t.Fatalf("failed to provision queues: %v", err)
	}

	walletRepo := repository.NewWalletRepository()
	wagerRepo := repository.NewWagerTransactionRepository()
	ledgerRepo := repository.NewLedgerRepository()
	idemRepo := repository.NewIdempotencyRepository()
	outboxRepo := repository.NewOutboxRepository(pool)
	inboxRepo := repository.NewInboxRepository()

	svc := service.NewWagerService(pool, walletRepo, wagerRepo, ledgerRepo, idemRepo, outboxRepo, inboxRepo)

	consumerCfg := messaging.DefaultSQSConsumerConfig(queues.WagerRequestsQueueURL)
	consumerCfg.WaitTimeSeconds = 1
	consumer := messaging.NewSQSConsumer(sqsClient, svc, consumerCfg, nil)
	consumer.Start()
	defer consumer.Stop(ctx)

	// 1. Criar carteira com 100.00 BRL
	playerID := fmt.Sprintf("player-inline-%s", uuid.New().String()[:8])
	wallet, err := svc.OpenWallet(ctx, playerID, "BRL", 10000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	providerID := "provider-inline-test"
	betExtID := fmt.Sprintf("bet-ext-%s", uuid.New().String())
	refundExtID := fmt.Sprintf("refund-ext-%s", uuid.New().String())

	// 2. Enviar REFUND antes do BET
	refundEnvelope := messaging.WagerMessageEnvelope{
		MessageID:  fmt.Sprintf("msg-refund-%s", uuid.New().String()),
		Type:       "WagerTransactionRequested",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Data: messaging.WagerMessageData{
			ProviderID:                     providerID,
			ExternalTransactionID:          refundExtID,
			IdempotencyKey:                 fmt.Sprintf("refund-idem-%s", refundExtID),
			PlayerID:                       playerID,
			WalletID:                       wallet.ID.String(),
			RoundID:                        "round-1",
			GameID:                         "game-1",
			Kind:                           "REFUND",
			ReferenceExternalTransactionID: betExtID,
			Money: struct {
				Amount   string `json:"amount"`
				Currency string `json:"currency"`
			}{
				Amount:   "25.00",
				Currency: "BRL",
			},
		},
	}
	refundBytes, _ := json.Marshal(refundEnvelope)

	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(queues.WagerRequestsQueueURL),
		MessageBody:            aws.String(string(refundBytes)),
		MessageGroupId:         aws.String(playerID),
		MessageDeduplicationId: aws.String(refundEnvelope.MessageID),
	})
	if err != nil {
		t.Fatalf("failed to send REFUND message: %v", err)
	}

	// Aguardar REFUND ser persistido como PENDING_REFERENCE
	var refundStatus domain.TransactionStatus
	for i := 0; i < 30; i++ {
		time.Sleep(200 * time.Millisecond)
		refundStatus, err = getWagerStatus(ctx, pool, providerID, refundExtID)
		if err == nil && refundStatus == domain.StatusPendingReference {
			break
		}
	}
	if refundStatus != domain.StatusPendingReference {
		t.Fatalf("expected REFUND status PENDING_REFERENCE, got: %s (err: %v)", refundStatus, err)
	}

	// Saldo ainda deve ser 100.00
	bal, err := getWalletBalance(ctx, pool, playerID, "BRL")
	if err != nil || bal != 10000 {
		t.Fatalf("expected balance 10000 after pending refund, got %d (err: %v)", bal, err)
	}

	// 3. Agora enviar o BET original
	betEnvelope := messaging.WagerMessageEnvelope{
		MessageID:  fmt.Sprintf("msg-bet-%s", uuid.New().String()),
		Type:       "WagerTransactionRequested",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Data: messaging.WagerMessageData{
			ProviderID:            providerID,
			ExternalTransactionID: betExtID,
			IdempotencyKey:        fmt.Sprintf("bet-idem-%s", betExtID),
			PlayerID:              playerID,
			WalletID:              wallet.ID.String(),
			RoundID:               "round-1",
			GameID:                "game-1",
			Kind:                  "BET",
			Money: struct {
				Amount   string `json:"amount"`
				Currency string `json:"currency"`
			}{
				Amount:   "25.00",
				Currency: "BRL",
			},
		},
	}
	betBytes, _ := json.Marshal(betEnvelope)

	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(queues.WagerRequestsQueueURL),
		MessageBody:            aws.String(string(betBytes)),
		MessageGroupId:         aws.String(playerID),
		MessageDeduplicationId: aws.String(betEnvelope.MessageID),
	})
	if err != nil {
		t.Fatalf("failed to send BET message: %v", err)
	}

	// Aguardar BET ser processado e REFUND ser resolvido inline para PROCESSED
	for i := 0; i < 30; i++ {
		time.Sleep(200 * time.Millisecond)
		bStatus, _ := getWagerStatus(ctx, pool, providerID, betExtID)
		rStatus, _ := getWagerStatus(ctx, pool, providerID, refundExtID)
		if bStatus == domain.StatusProcessed && rStatus == domain.StatusProcessed {
			break
		}
	}

	bStatus, _ := getWagerStatus(ctx, pool, providerID, betExtID)
	rStatus, _ := getWagerStatus(ctx, pool, providerID, refundExtID)

	if bStatus != domain.StatusProcessed {
		t.Fatalf("expected BET PROCESSED, got %s", bStatus)
	}
	if rStatus != domain.StatusProcessed {
		t.Fatalf("expected REFUND resolved to PROCESSED, got %s", rStatus)
	}

	// Saldo deve ser 100.00 (100 - 25 + 25)
	bal, err = getWalletBalance(ctx, pool, playerID, "BRL")
	if err != nil || bal != 10000 {
		t.Fatalf("expected balance 10000, got %d (err: %v)", bal, err)
	}

	// Ledger deve conter 3 entradas (OPENING, BET, REFUND)
	entries, _, err := svc.GetLedger(ctx, wallet.ID, "BRL", nil, 10)
	if err != nil {
		t.Fatalf("failed to get ledger entries: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 ledger entries (OPENING, BET, REFUND), got %d", len(entries))
	}
}

// TestPendingRef_PeriodicWorker_MomentB valida a resolução pelo worker periódico quando
// a referência pendente e o BET são processados de forma assíncrona.
func TestPendingRef_PeriodicWorker_MomentB(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	walletRepo := repository.NewWalletRepository()
	wagerRepo := repository.NewWagerTransactionRepository()
	ledgerRepo := repository.NewLedgerRepository()
	idemRepo := repository.NewIdempotencyRepository()
	outboxRepo := repository.NewOutboxRepository(pool)
	inboxRepo := repository.NewInboxRepository()

	svc := service.NewWagerService(pool, walletRepo, wagerRepo, ledgerRepo, idemRepo, outboxRepo, inboxRepo)

	playerID := fmt.Sprintf("player-worker-%s", uuid.New().String()[:8])
	wallet, err := svc.OpenWallet(ctx, playerID, "BRL", 5000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	providerID := "provider-worker-test"
	betExtID := fmt.Sprintf("bet-w-%s", uuid.New().String())
	refundExtID := fmt.Sprintf("refund-w-%s", uuid.New().String())

	// 1. Inserir BET como PROCESSED
	betID := uuid.New()
	_, err = pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			id, origin, external_id, provider_id, idempotency_key,
			wallet_id, player_id, round_id, game_id, type, amount, currency, status, created_at, updated_at
		) VALUES (
			$1, 'EXTERNAL', $2, $3, $4,
			$5, $6, 'round-1', 'game-1', 'BET', 2000, 'BRL', 'PROCESSED', now(), now()
		)
	`, betID, betExtID, providerID, betExtID, wallet.ID, playerID)
	if err != nil {
		t.Fatalf("failed to insert BET: %v", err)
	}

	// 2. Inserir REFUND como PENDING_REFERENCE apontando para betExtID
	refundID := uuid.New()
	_, err = pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			id, origin, external_id, provider_id, idempotency_key,
			wallet_id, player_id, round_id, game_id, type, amount, currency, status,
			external_reference_id, attempts, next_attempt_at, created_at, updated_at
		) VALUES (
			$1, 'EXTERNAL', $2, $3, $4,
			$5, $6, 'round-1', 'game-1', 'REFUND', 2000, 'BRL', 'PENDING_REFERENCE',
			$7, 0, now() - interval '1 second', now(), now()
		)
	`, refundID, refundExtID, providerID, refundExtID, wallet.ID, playerID, betExtID)
	if err != nil {
		t.Fatalf("failed to insert REFUND: %v", err)
	}

	// 3. Executar o worker
	resolver := service.NewPendingRefResolver(pool, wagerRepo, walletRepo, ledgerRepo, outboxRepo, service.DefaultPendingRefResolverConfig(), nil)
	if err := resolver.ResolveOnce(ctx); err != nil {
		t.Fatalf("failed to run resolver.ResolveOnce: %v", err)
	}

	// 4. Validar transição para PROCESSED
	status, err := getWagerStatus(ctx, pool, providerID, refundExtID)
	if err != nil || status != domain.StatusProcessed {
		t.Fatalf("expected REFUND PROCESSED, got %s (err: %v)", status, err)
	}

	// Saldo deve ser 70.00 (50.00 + 20.00 do refund)
	bal, err := getWalletBalance(ctx, pool, playerID, "BRL")
	if err != nil || bal != 7000 {
		t.Fatalf("expected balance 7000, got %d (err: %v)", bal, err)
	}
}

// TestPendingRef_Idempotency valida que invocar o resolver múltiplas vezes não duplica saldo.
func TestPendingRef_Idempotency(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	walletRepo := repository.NewWalletRepository()
	wagerRepo := repository.NewWagerTransactionRepository()
	ledgerRepo := repository.NewLedgerRepository()
	idemRepo := repository.NewIdempotencyRepository()
	outboxRepo := repository.NewOutboxRepository(pool)
	inboxRepo := repository.NewInboxRepository()

	svc := service.NewWagerService(pool, walletRepo, wagerRepo, ledgerRepo, idemRepo, outboxRepo, inboxRepo)

	playerID := fmt.Sprintf("player-idem-%s", uuid.New().String()[:8])
	wallet, err := svc.OpenWallet(ctx, playerID, "BRL", 5000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	providerID := "provider-idem-test"
	betExtID := fmt.Sprintf("bet-idem-%s", uuid.New().String())
	refundExtID := fmt.Sprintf("ref-idem-%s", uuid.New().String())

	betID := uuid.New()
	_, _ = pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			id, origin, external_id, provider_id, idempotency_key,
			wallet_id, player_id, round_id, game_id, type, amount, currency, status, created_at, updated_at
		) VALUES (
			$1, 'EXTERNAL', $2, $3, $4,
			$5, $6, 'round-1', 'game-1', 'BET', 1000, 'BRL', 'PROCESSED', now(), now()
		)
	`, betID, betExtID, providerID, betExtID, wallet.ID, playerID)

	refundID := uuid.New()
	_, _ = pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			id, origin, external_id, provider_id, idempotency_key,
			wallet_id, player_id, round_id, game_id, type, amount, currency, status,
			external_reference_id, attempts, next_attempt_at, created_at, updated_at
		) VALUES (
			$1, 'EXTERNAL', $2, $3, $4,
			$5, $6, 'round-1', 'game-1', 'REFUND', 1000, 'BRL', 'PENDING_REFERENCE',
			$7, 0, now() - interval '1 second', now(), now()
		)
	`, refundID, refundExtID, providerID, refundExtID, wallet.ID, playerID, betExtID)

	resolver := service.NewPendingRefResolver(pool, wagerRepo, walletRepo, ledgerRepo, outboxRepo, service.DefaultPendingRefResolverConfig(), nil)

	// Rodada 1: Resolve
	if err := resolver.ResolveOnce(ctx); err != nil {
		t.Fatalf("first resolveOnce failed: %v", err)
	}

	bal1, _ := getWalletBalance(ctx, pool, playerID, "BRL")

	// Rodada 2: Não deve duplicar nem dar erro
	if err := resolver.ResolveOnce(ctx); err != nil {
		t.Fatalf("second resolveOnce failed: %v", err)
	}

	bal2, _ := getWalletBalance(ctx, pool, playerID, "BRL")
	if bal1 != bal2 {
		t.Fatalf("balance mutated on duplicate round: bal1=%d, bal2=%d", bal1, bal2)
	}
}

// TestPendingRef_SkipLocked_NoDeadlock valida que dois workers concorrentes varrendo itens não geram deadlock.
func TestPendingRef_SkipLocked_NoDeadlock(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	walletRepo := repository.NewWalletRepository()
	wagerRepo := repository.NewWagerTransactionRepository()
	ledgerRepo := repository.NewLedgerRepository()
	idemRepo := repository.NewIdempotencyRepository()
	outboxRepo := repository.NewOutboxRepository(pool)
	inboxRepo := repository.NewInboxRepository()

	svc := service.NewWagerService(pool, walletRepo, wagerRepo, ledgerRepo, idemRepo, outboxRepo, inboxRepo)

	playerID := fmt.Sprintf("player-conc-%s", uuid.New().String()[:8])
	wallet, err := svc.OpenWallet(ctx, playerID, "BRL", 50000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	providerID := "provider-conc-test"
	for i := 0; i < 5; i++ {
		bID := uuid.New()
		bExt := fmt.Sprintf("bet-c-%d-%s", i, uuid.New().String())
		rID := uuid.New()
		rExt := fmt.Sprintf("ref-c-%d-%s", i, uuid.New().String())

		_, _ = pool.Exec(ctx, `
			INSERT INTO wager_transactions (
				id, origin, external_id, provider_id, idempotency_key,
				wallet_id, player_id, round_id, game_id, type, amount, currency, status, created_at, updated_at
			) VALUES ($1, 'EXTERNAL', $2, $3, $4, $5, $6, 'round-1', 'game-1', 'BET', 100, 'BRL', 'PROCESSED', now(), now())
		`, bID, bExt, providerID, bExt, wallet.ID, playerID)

		_, _ = pool.Exec(ctx, `
			INSERT INTO wager_transactions (
				id, origin, external_id, provider_id, idempotency_key,
				wallet_id, player_id, round_id, game_id, type, amount, currency, status,
				external_reference_id, attempts, next_attempt_at, created_at, updated_at
			) VALUES ($1, 'EXTERNAL', $2, $3, $4, $5, $6, 'round-1', 'game-1', 'REFUND', 100, 'BRL', 'PENDING_REFERENCE', $7, 0, now() - interval '1 second', now(), now())
		`, rID, rExt, providerID, rExt, wallet.ID, playerID, bExt)
	}

	resolver1 := service.NewPendingRefResolver(pool, wagerRepo, walletRepo, ledgerRepo, outboxRepo, service.DefaultPendingRefResolverConfig(), nil)
	resolver2 := service.NewPendingRefResolver(pool, wagerRepo, walletRepo, ledgerRepo, outboxRepo, service.DefaultPendingRefResolverConfig(), nil)

	var wg sync.WaitGroup
	errs := make(chan error, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := resolver1.ResolveOnce(ctx); err != nil {
			errs <- err
		}
	}()
	go func() {
		defer wg.Done()
		if err := resolver2.ResolveOnce(ctx); err != nil {
			errs <- err
		}
	}()
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent resolution failed with error: %v", err)
	}
}

// TestPendingRef_TTLExpired_Rejection valida que pendência com mais de 60 minutos é rejeitada com REFERENCE_NOT_FOUND.
func TestPendingRef_TTLExpired_Rejection(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	walletRepo := repository.NewWalletRepository()
	wagerRepo := repository.NewWagerTransactionRepository()
	ledgerRepo := repository.NewLedgerRepository()
	outboxRepo := repository.NewOutboxRepository(pool)
	inboxRepo := repository.NewInboxRepository()
	idemRepo := repository.NewIdempotencyRepository()

	svc := service.NewWagerService(pool, walletRepo, wagerRepo, ledgerRepo, idemRepo, outboxRepo, inboxRepo)

	playerID := fmt.Sprintf("player-ttl-%s", uuid.New().String()[:8])
	w, err := svc.OpenWallet(ctx, playerID, "BRL", 1000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	providerID := "provider-ttl-test"
	refExtID := fmt.Sprintf("ref-ttl-%s", uuid.New().String())
	nonExistentBet := "bet-never-arrived"

	txID := uuid.New()
	// Força created_at para 65 minutos atrás
	_, err = pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			id, origin, external_id, provider_id, idempotency_key,
			wallet_id, player_id, round_id, game_id, type, amount, currency, status,
			external_reference_id, attempts, next_attempt_at, created_at, updated_at
		) VALUES (
			$1, 'EXTERNAL', $2, $3, $4,
			$5, $6, 'round-1', 'game-1', 'REFUND', 1000, 'BRL', 'PENDING_REFERENCE',
			$7, 0, now() - interval '1 second', now() - interval '65 minutes', now() - interval '65 minutes'
		)
	`, txID, refExtID, providerID, refExtID, w.ID, playerID, nonExistentBet)
	if err != nil {
		t.Fatalf("failed to insert expired ref: %v", err)
	}

	resolver := service.NewPendingRefResolver(pool, wagerRepo, walletRepo, ledgerRepo, outboxRepo, service.DefaultPendingRefResolverConfig(), nil)
	if err := resolver.ResolveOnce(ctx); err != nil {
		t.Fatalf("resolveOnce failed: %v", err)
	}

	// Validar que status é REJECTED e error_code é REFERENCE_NOT_FOUND
	var status, errCode string
	err = pool.QueryRow(ctx, "SELECT status, error_code FROM wager_transactions WHERE id = $1", txID).Scan(&status, &errCode)
	if err != nil {
		t.Fatalf("failed to query status: %v", err)
	}

	if status != string(domain.StatusRejected) {
		t.Fatalf("expected status REJECTED, got %s", status)
	}
	if errCode != domain.FailureCodeReferenceNotFound {
		t.Fatalf("expected error_code %s, got %s", domain.FailureCodeReferenceNotFound, errCode)
	}
}

// TestPendingRef_OriginalFailed_Rejection valida que se o BET original estiver REJECTED,
// a pendência é rejeitada com ORIGINAL_TRANSACTION_FAILED.
func TestPendingRef_OriginalFailed_Rejection(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	walletRepo := repository.NewWalletRepository()
	wagerRepo := repository.NewWagerTransactionRepository()
	ledgerRepo := repository.NewLedgerRepository()
	outboxRepo := repository.NewOutboxRepository(pool)
	inboxRepo := repository.NewInboxRepository()
	idemRepo := repository.NewIdempotencyRepository()

	svc := service.NewWagerService(pool, walletRepo, wagerRepo, ledgerRepo, idemRepo, outboxRepo, inboxRepo)

	playerID := fmt.Sprintf("player-orig-fail-%s", uuid.New().String()[:8])
	w, err := svc.OpenWallet(ctx, playerID, "BRL", 5000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	providerID := "provider-orig-fail-test"
	betExtID := fmt.Sprintf("bet-fail-%s", uuid.New().String())
	refExtID := fmt.Sprintf("ref-orig-fail-%s", uuid.New().String())

	// 1. BET em REJECTED
	betID := uuid.New()
	_, _ = pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			id, origin, external_id, provider_id, idempotency_key,
			wallet_id, player_id, round_id, game_id, type, amount, currency, status, error_code, created_at, updated_at
		) VALUES (
			$1, 'EXTERNAL', $2, $3, $4,
			$5, $6, 'round-1', 'game-1', 'BET', 5000, 'BRL', 'REJECTED', 'INSUFFICIENT_FUNDS', now(), now()
		)
	`, betID, betExtID, providerID, betExtID, w.ID, playerID)

	// 2. REFUND em PENDING_REFERENCE apontando para o BET
	refID := uuid.New()
	_, _ = pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			id, origin, external_id, provider_id, idempotency_key,
			wallet_id, player_id, round_id, game_id, type, amount, currency, status,
			external_reference_id, attempts, next_attempt_at, created_at, updated_at
		) VALUES (
			$1, 'EXTERNAL', $2, $3, $4,
			$5, $6, 'round-1', 'game-1', 'REFUND', 5000, 'BRL', 'PENDING_REFERENCE',
			$7, 0, now() - interval '1 second', now(), now()
		)
	`, refID, refExtID, providerID, refExtID, w.ID, playerID, betExtID)

	resolver := service.NewPendingRefResolver(pool, wagerRepo, walletRepo, ledgerRepo, outboxRepo, service.DefaultPendingRefResolverConfig(), nil)
	if err := resolver.ResolveOnce(ctx); err != nil {
		t.Fatalf("resolveOnce failed: %v", err)
	}

	var status, errCode string
	_ = pool.QueryRow(ctx, "SELECT status, error_code FROM wager_transactions WHERE id = $1", refID).Scan(&status, &errCode)

	if status != string(domain.StatusRejected) {
		t.Fatalf("expected status REJECTED, got %s", status)
	}
	if errCode != domain.FailureCodeOriginalTransactionFailed {
		t.Fatalf("expected error_code %s, got %s", domain.FailureCodeOriginalTransactionFailed, errCode)
	}
}

// TestPendingRef_RollbackWin_DebitInsufficientFunds valida que ROLLBACK de WIN é débito e,
// se o saldo for insuficiente, é rejeitado com INSUFFICIENT_FUNDS_FOR_ROLLBACK.
func TestPendingRef_RollbackWin_DebitInsufficientFunds(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	walletRepo := repository.NewWalletRepository()
	wagerRepo := repository.NewWagerTransactionRepository()
	ledgerRepo := repository.NewLedgerRepository()
	outboxRepo := repository.NewOutboxRepository(pool)
	inboxRepo := repository.NewInboxRepository()
	idemRepo := repository.NewIdempotencyRepository()

	svc := service.NewWagerService(pool, walletRepo, wagerRepo, ledgerRepo, idemRepo, outboxRepo, inboxRepo)

	playerID := fmt.Sprintf("player-roll-fail-%s", uuid.New().String()[:8])
	w, err := svc.OpenWallet(ctx, playerID, "BRL", 0) // saldo inicial 0
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	providerID := "provider-roll-test"
	winExtID := fmt.Sprintf("win-ext-%s", uuid.New().String())
	rollbackExtID := fmt.Sprintf("roll-ext-%s", uuid.New().String())

	// 1. WIN processado de 100.00 (mas carteira está com 0 de saldo)
	winID := uuid.New()
	_, _ = pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			id, origin, external_id, provider_id, idempotency_key,
			wallet_id, player_id, round_id, game_id, type, amount, currency, status, created_at, updated_at
		) VALUES (
			$1, 'EXTERNAL', $2, $3, $4,
			$5, $6, 'round-1', 'game-1', 'WIN', 10000, 'BRL', 'PROCESSED', now(), now()
		)
	`, winID, winExtID, providerID, winExtID, w.ID, playerID)

	// 2. ROLLBACK de 100.00 em PENDING_REFERENCE apontando para WIN
	rollID := uuid.New()
	_, _ = pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			id, origin, external_id, provider_id, idempotency_key,
			wallet_id, player_id, round_id, game_id, type, amount, currency, status,
			external_reference_id, attempts, next_attempt_at, created_at, updated_at
		) VALUES (
			$1, 'EXTERNAL', $2, $3, $4,
			$5, $6, 'round-1', 'game-1', 'ROLLBACK', 10000, 'BRL', 'PENDING_REFERENCE',
			$7, 0, now() - interval '1 second', now(), now()
		)
	`, rollID, rollbackExtID, providerID, rollbackExtID, w.ID, playerID, winExtID)

	resolver := service.NewPendingRefResolver(pool, wagerRepo, walletRepo, ledgerRepo, outboxRepo, service.DefaultPendingRefResolverConfig(), nil)
	if err := resolver.ResolveOnce(ctx); err != nil {
		t.Fatalf("resolveOnce failed: %v", err)
	}

	var status, errCode string
	_ = pool.QueryRow(ctx, "SELECT status, error_code FROM wager_transactions WHERE id = $1", rollID).Scan(&status, &errCode)

	if status != string(domain.StatusRejected) {
		t.Fatalf("expected status REJECTED, got %s", status)
	}
	if errCode != domain.FailureCodeInsufficientFundsRollback {
		t.Fatalf("expected error_code %s, got %s", domain.FailureCodeInsufficientFundsRollback, errCode)
	}
}

// TestPendingRef_DoubleReversal_AlreadyRefunded valida que tentar reembolsar um BET já reembolsado
// resulta em rejeição com ALREADY_REFUNDED.
func TestPendingRef_DoubleReversal_AlreadyRefunded(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	walletRepo := repository.NewWalletRepository()
	wagerRepo := repository.NewWagerTransactionRepository()
	ledgerRepo := repository.NewLedgerRepository()
	outboxRepo := repository.NewOutboxRepository(pool)
	inboxRepo := repository.NewInboxRepository()
	idemRepo := repository.NewIdempotencyRepository()

	svc := service.NewWagerService(pool, walletRepo, wagerRepo, ledgerRepo, idemRepo, outboxRepo, inboxRepo)

	playerID := fmt.Sprintf("player-dup-ref-%s", uuid.New().String()[:8])
	w, err := svc.OpenWallet(ctx, playerID, "BRL", 5000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	providerID := "provider-dup-test"
	betExtID := fmt.Sprintf("bet-dup-%s", uuid.New().String())
	refund1ExtID := fmt.Sprintf("ref-1-%s", uuid.New().String())
	refund2ExtID := fmt.Sprintf("ref-2-%s", uuid.New().String())

	// 1. BET PROCESSED
	betID := uuid.New()
	_, _ = pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			id, origin, external_id, provider_id, idempotency_key,
			wallet_id, player_id, round_id, game_id, type, amount, currency, status, created_at, updated_at
		) VALUES ($1, 'EXTERNAL', $2, $3, $4, $5, $6, 'round-1', 'game-1', 'BET', 1000, 'BRL', 'PROCESSED', now(), now())
	`, betID, betExtID, providerID, betExtID, w.ID, playerID)

	// 2. Primeiro REFUND PROCESSED apontando para betID
	ref1ID := uuid.New()
	_, _ = pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			id, origin, external_id, provider_id, idempotency_key,
			wallet_id, player_id, round_id, game_id, type, amount, currency, status,
			reference_id, external_reference_id, created_at, updated_at
		) VALUES ($1, 'EXTERNAL', $2, $3, $4, $5, $6, 'round-1', 'game-1', 'REFUND', 1000, 'BRL', 'PROCESSED', $7, $8, now(), now())
	`, ref1ID, refund1ExtID, providerID, refund1ExtID, w.ID, playerID, betID, betExtID)

	// 3. Segundo REFUND PENDING_REFERENCE apontando para o mesmo betExtID
	ref2ID := uuid.New()
	_, _ = pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			id, origin, external_id, provider_id, idempotency_key,
			wallet_id, player_id, round_id, game_id, type, amount, currency, status,
			external_reference_id, attempts, next_attempt_at, created_at, updated_at
		) VALUES ($1, 'EXTERNAL', $2, $3, $4, $5, $6, 'round-1', 'game-1', 'REFUND', 1000, 'BRL', 'PENDING_REFERENCE', $7, 0, now() - interval '1 second', now(), now())
	`, ref2ID, refund2ExtID, providerID, refund2ExtID, w.ID, playerID, betExtID)

	resolver := service.NewPendingRefResolver(pool, wagerRepo, walletRepo, ledgerRepo, outboxRepo, service.DefaultPendingRefResolverConfig(), nil)
	if err := resolver.ResolveOnce(ctx); err != nil {
		t.Fatalf("resolveOnce failed: %v", err)
	}

	var status, errCode string
	_ = pool.QueryRow(ctx, "SELECT status, error_code FROM wager_transactions WHERE id = $1", ref2ID).Scan(&status, &errCode)

	if status != string(domain.StatusRejected) {
		t.Fatalf("expected status REJECTED, got %s", status)
	}
	if errCode != domain.FailureCodeAlreadyRefunded {
		t.Fatalf("expected error_code %s, got %s", domain.FailureCodeAlreadyRefunded, errCode)
	}
}
