package test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
	"github.com/moaaskt/jungle-test-challenge/pkg/repository"
	"github.com/moaaskt/jungle-test-challenge/pkg/service"
)

func getTestPool(t *testing.T) *pgxpool.Pool {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://jungle:password@localhost:5432/jungle_test?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed to connect to test database: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("failed to ping test database: %v", err)
	}
	return pool
}

// TestOutbox_TransactionalAtomicity valida que rollbacks não persistem eventos e commits persistem todos os eventos
func TestOutbox_TransactionalAtomicity(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	walletRepo := repository.NewWalletRepository()
	wagerRepo := repository.NewWagerTransactionRepository()
	ledgerRepo := repository.NewLedgerRepository()
	idemRepo := repository.NewIdempotencyRepository()
	outboxRepo := repository.NewOutboxRepository(pool)

	svc := service.NewWagerService(pool, walletRepo, wagerRepo, ledgerRepo, idemRepo, outboxRepo)

	// 1. Validar que rollback descarta evento da outbox
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx: %v", err)
	}

	testEvID := uuid.New()
	dummyEvent := &domain.OutboxEvent{
		ID:            testEvID,
		AggregateType: "Test",
		AggregateID:   testEvID.String(),
		EventType:     "TestRollbackEvent",
		Payload:       json.RawMessage(`{"test":true}`),
		Status:        domain.OutboxStatusPending,
		CreatedAt:     time.Now().UTC(),
		NextAttemptAt: time.Now().UTC(),
	}

	if err := outboxRepo.Insert(ctx, tx, dummyEvent); err != nil {
		t.Fatalf("failed to insert dummy outbox event: %v", err)
	}

	// Executar Rollback explícito
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	// Verificar que o evento NÃO existe no banco
	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE id = $1", testEvID).Scan(&count)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 events after rollback, found %d", count)
	}

	// 2. Validar OpenWallet com saldo positivo: gera WagerTransactionProcessed e WalletBalanceChanged
	playerID := "player-atom-" + uuid.NewString()
	wPositive, err := svc.OpenWallet(ctx, playerID, "BRL", 5000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	rows, err := pool.Query(ctx, "SELECT event_type, payload FROM outbox_events WHERE aggregate_id = $1 OR payload->'data'->>'walletId' = $1", wPositive.ID.String())
	if err != nil {
		t.Fatalf("failed to query outbox events: %v", err)
	}
	defer rows.Close()

	eventsFound := make(map[string]bool)
	for rows.Next() {
		var evType string
		var payloadBytes []byte
		if err := rows.Scan(&evType, &payloadBytes); err != nil {
			t.Fatalf("failed to scan outbox row: %v", err)
		}
		eventsFound[evType] = true
	}

	if !eventsFound[domain.EventTypeWagerTransactionProcessed] {
		t.Errorf("expected WagerTransactionProcessed outbox event for positive open wallet")
	}
	if !eventsFound[domain.EventTypeWalletBalanceChanged] {
		t.Errorf("expected WalletBalanceChanged outbox event for positive open wallet")
	}

	// 3. Validar OpenWallet com saldo zero: NÃO cria eventos financeiros na outbox (Seção 9 da spec)
	playerZeroID := "player-zero-" + uuid.NewString()
	wZero, err := svc.OpenWallet(ctx, playerZeroID, "BRL", 0)
	if err != nil {
		t.Fatalf("failed to open zero wallet: %v", err)
	}

	var zeroEventsCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1 OR payload->'data'->>'walletId' = $1", wZero.ID.String()).Scan(&zeroEventsCount)
	if err != nil {
		t.Fatalf("failed to query zero wallet events: %v", err)
	}
	if zeroEventsCount != 0 {
		t.Errorf("expected 0 outbox events for zero balance opening, found %d", zeroEventsCount)
	}

	// 4. Validar ProcessWager BET: gera WagerTransactionProcessed e WalletBalanceChanged
	provID := "prov-1"
	extBetID := "ext-bet-" + uuid.NewString()
	idemKeyBet := "idem-bet-" + uuid.NewString()
	betRes, err := svc.ProcessWager(ctx, service.ProcessWagerRequest{
		Origin:         domain.OriginExternal,
		PlayerID:       playerID,
		ProviderID:     &provID,
		ExternalID:     &extBetID,
		IdempotencyKey: &idemKeyBet,
		Type:           domain.TransactionTypeBet,
		Amount:         1000,
		Currency:       "BRL",
	})
	if err != nil {
		t.Fatalf("failed to process bet: %v", err)
	}

	var betProcessedCount, betBalanceCount int
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE event_type = $1 AND aggregate_id = $2",
		domain.EventTypeWagerTransactionProcessed, betRes.TransactionID.String()).Scan(&betProcessedCount)
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE event_type = $1 AND payload->'data'->>'transactionId' = $2",
		domain.EventTypeWalletBalanceChanged, betRes.TransactionID.String()).Scan(&betBalanceCount)

	if betProcessedCount != 1 {
		t.Errorf("expected 1 WagerTransactionProcessed event for bet, got %d", betProcessedCount)
	}
	if betBalanceCount != 1 {
		t.Errorf("expected 1 WalletBalanceChanged event for bet, got %d", betBalanceCount)
	}

	// 5. Validar ProcessWager LOSS: gera WagerTransactionProcessed mas NÃO gera WalletBalanceChanged
	extLossID := "ext-loss-" + uuid.NewString()
	idemKeyLoss := "idem-loss-" + uuid.NewString()
	lossRes, err := svc.ProcessWager(ctx, service.ProcessWagerRequest{
		Origin:         domain.OriginExternal,
		PlayerID:       playerID,
		ProviderID:     &provID,
		ExternalID:     &extLossID,
		IdempotencyKey: &idemKeyLoss,
		Type:           domain.TransactionTypeLoss,
		Amount:         0,
		Currency:       "BRL",
	})
	if err != nil {
		t.Fatalf("failed to process loss: %v", err)
	}

	var lossProcessedCount, lossBalanceCount int
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE event_type = $1 AND aggregate_id = $2",
		domain.EventTypeWagerTransactionProcessed, lossRes.TransactionID.String()).Scan(&lossProcessedCount)
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE event_type = $1 AND payload->'data'->>'transactionId' = $2",
		domain.EventTypeWalletBalanceChanged, lossRes.TransactionID.String()).Scan(&lossBalanceCount)

	if lossProcessedCount != 1 {
		t.Errorf("expected 1 WagerTransactionProcessed event for loss, got %d", lossProcessedCount)
	}
	if lossBalanceCount != 0 {
		t.Errorf("expected 0 WalletBalanceChanged event for loss, got %d", lossBalanceCount)
	}

	// 6. Validar ProcessWager REJECTED (saldo insuficiente): gera WagerTransactionRejected
	extRejID := "ext-rej-" + uuid.NewString()
	idemKeyRej := "idem-rej-" + uuid.NewString()
	rejRes, err := svc.ProcessWager(ctx, service.ProcessWagerRequest{
		Origin:         domain.OriginExternal,
		PlayerID:       playerID,
		ProviderID:     &provID,
		ExternalID:     &extRejID,
		IdempotencyKey: &idemKeyRej,
		Type:           domain.TransactionTypeBet,
		Amount:         99999999, // saldo insuficiente
		Currency:       "BRL",
	})
	if err != nil {
		t.Fatalf("rejected wager returned unexpected error: %v", err)
	}
	if rejRes.Status != domain.StatusRejected {
		t.Fatalf("expected status REJECTED, got %s", rejRes.Status)
	}

	var rejEventCount int
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE event_type = $1 AND aggregate_id = $2",
		domain.EventTypeWagerTransactionRejected, rejRes.TransactionID.String()).Scan(&rejEventCount)
	if rejEventCount != 1 {
		t.Errorf("expected 1 WagerTransactionRejected event for rejected wager, got %d", rejEventCount)
	}
}

type recordingPublisher struct {
	mu        sync.Mutex
	published map[uuid.UUID]int
}

func (p *recordingPublisher) Publish(ctx context.Context, ev *domain.OutboxEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.published[ev.ID]++
	return nil
}

// TestOutbox_MultiPublisher_SkipLocked_NoDuplicates valida que múltiplos publishers concorrentes
// disputam a outbox usando SKIP LOCKED sem duplicar publicações nem gerar conflitos.
func TestOutbox_MultiPublisher_SkipLocked_NoDuplicates(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	outboxRepo := repository.NewOutboxRepository(pool)

	numEvents := 60
	eventIDs := make([]uuid.UUID, numEvents)

	// Inserir massa de eventos pendentes
	for i := 0; i < numEvents; i++ {
		evID := uuid.New()
		eventIDs[i] = evID
		ev := &domain.OutboxEvent{
			ID:            evID,
			AggregateType: "TestContention",
			AggregateID:   evID.String(),
			EventType:     "ContentionEvent",
			Payload:       json.RawMessage(fmt.Sprintf(`{"index":%d}`, i)),
			Status:        domain.OutboxStatusPending,
			CreatedAt:     time.Now().UTC(),
			NextAttemptAt: time.Now().UTC(),
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("failed to begin tx: %v", err)
		}
		if err := outboxRepo.Insert(ctx, tx, ev); err != nil {
			t.Fatalf("failed to insert contention event: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("failed to commit contention event: %v", err)
		}
	}

	recPub := &recordingPublisher{
		published: make(map[uuid.UUID]int),
	}

	// Iniciar 4 publishers em paralelo disputando os mesmos registros
	numPublishers := 4
	var wg sync.WaitGroup
	wg.Add(numPublishers)

	cfg := service.OutboxRelayerConfig{
		BatchSize:     10,
		PollInterval:  20 * time.Millisecond,
		LeaseDuration: 10 * time.Second,
		MaxRetries:    3,
		BaseBackoff:   100 * time.Millisecond,
	}

	for i := 0; i < numPublishers; i++ {
		go func(workerID int) {
			defer wg.Done()
			relayer := service.NewOutboxRelayer(outboxRepo, recPub, &cfg)

			// Processar em loop até drenar os eventos inseridos
			timeout := time.After(4 * time.Second)
			for {
				select {
				case <-timeout:
					return
				default:
					n, err := relayer.ProcessBatch(ctx)
					if err != nil {
						t.Errorf("worker %d batch error: %v", workerID, err)
						return
					}
					if n == 0 {
						time.Sleep(30 * time.Millisecond)
					}
				}
			}
		}(i)
	}

	wg.Wait()

	// Validar que cada um dos 60 eventos foi publicado exatamente 1 vez
	recPub.mu.Lock()
	defer recPub.mu.Unlock()

	for _, id := range eventIDs {
		times, exists := recPub.published[id]
		if !exists {
			t.Errorf("event %s was never published", id)
		} else if times != 1 {
			t.Errorf("event %s was published %d times (expected exactly 1)", id, times)
		}

		// Validar status no banco
		var status string
		err := pool.QueryRow(ctx, "SELECT status FROM outbox_events WHERE id = $1", id).Scan(&status)
		if err != nil {
			t.Fatalf("failed to query status: %v", err)
		}
		if status != "PUBLISHED" {
			t.Errorf("expected event %s to be PUBLISHED, got %s", id, status)
		}
	}
}

// TestOutbox_AbandonedLease_Recovery valida que um evento abandonado por falha de worker
// tem seu lease expirado e é retomado por outro worker preservando o mesmo eventId.
func TestOutbox_AbandonedLease_Recovery(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	outboxRepo := repository.NewOutboxRepository(pool)

	abandonedID := uuid.New()
	pastLease := time.Now().UTC().Add(-10 * time.Minute) // lease expirado no passado

	// Inserir evento com lease expirado e tentativa anterior
	query := `
		INSERT INTO outbox_events (
			id, aggregate_type, aggregate_id, event_type, payload, status, retry_count, next_attempt_at, locked_until, created_at
		) VALUES (
			$1, 'TestAggregate', $2, 'AbandonedEvent', '{"abandoned":true}', 'PENDING', 1, NOW() - INTERVAL '5 minutes', $3, NOW() - INTERVAL '10 minutes'
		)
	`
	_, err := pool.Exec(ctx, query, abandonedID, abandonedID.String(), pastLease)
	if err != nil {
		t.Fatalf("failed to insert abandoned event: %v", err)
	}

	recPub := &recordingPublisher{
		published: make(map[uuid.UUID]int),
	}

	cfg := service.OutboxRelayerConfig{
		BatchSize:     10,
		LeaseDuration: 30 * time.Second,
		MaxRetries:    5,
		BaseBackoff:   100 * time.Millisecond,
	}
	relayer := service.NewOutboxRelayer(outboxRepo, recPub, &cfg)

	// Novo worker executa processamento do lote
	n, err := relayer.ProcessBatch(ctx)
	if err != nil {
		t.Fatalf("failed to process batch: %v", err)
	}
	if n == 0 {
		t.Fatalf("expected at least 1 claimed abandoned event, got 0")
	}

	recPub.mu.Lock()
	count := recPub.published[abandonedID]
	recPub.mu.Unlock()

	if count != 1 {
		t.Errorf("expected abandoned event %s to be published exactly 1 time, got %d", abandonedID, count)
	}

	var status string
	var lockedUntil *time.Time
	err = pool.QueryRow(ctx, "SELECT status, locked_until FROM outbox_events WHERE id = $1", abandonedID).Scan(&status, &lockedUntil)
	if err != nil {
		t.Fatalf("failed to query event: %v", err)
	}
	if status != "PUBLISHED" {
		t.Errorf("expected status PUBLISHED, got %s", status)
	}
	if lockedUntil != nil {
		t.Errorf("expected locked_until to be cleared (NULL), got %v", lockedUntil)
	}
}

type flakyPublisher struct {
	failCount int32
	maxFails  int32
}

func (p *flakyPublisher) Publish(ctx context.Context, ev *domain.OutboxEvent) error {
	cur := atomic.AddInt32(&p.failCount, 1)
	if cur <= p.maxFails {
		return errors.New("simulated network transient error")
	}
	return nil
}

// TestOutbox_ExponentialBackoff_And_Failure valida retries com backoff e marcação FAILED após esgotar limites
func TestOutbox_ExponentialBackoff_And_Failure(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	outboxRepo := repository.NewOutboxRepository(pool)

	flakyID := uuid.New()
	ev := &domain.OutboxEvent{
		ID:            flakyID,
		AggregateType: "Flaky",
		AggregateID:   flakyID.String(),
		EventType:     "FlakyEvent",
		Payload:       json.RawMessage(`{"flaky":true}`),
		Status:        domain.OutboxStatusPending,
		CreatedAt:     time.Now().UTC(),
		NextAttemptAt: time.Now().UTC(),
	}

	tx, _ := pool.Begin(ctx)
	_ = outboxRepo.Insert(ctx, tx, ev)
	_ = tx.Commit(ctx)

	// Publisher que falha nas 2 primeiras tentativas e sucede na 3ª
	publisher := &flakyPublisher{maxFails: 2}
	cfg := service.OutboxRelayerConfig{
		BatchSize:     10,
		LeaseDuration: 5 * time.Second,
		MaxRetries:    5,
		BaseBackoff:   50 * time.Millisecond,
	}
	relayer := service.NewOutboxRelayer(outboxRepo, publisher, &cfg)

	// Tentativa 1: Falha
	_, err := relayer.ProcessBatch(ctx)
	if err != nil {
		t.Fatalf("batch error: %v", err)
	}

	var status1 string
	var retryCount1 int
	var nextAttempt1 time.Time
	_ = pool.QueryRow(ctx, "SELECT status, retry_count, next_attempt_at FROM outbox_events WHERE id = $1", flakyID).
		Scan(&status1, &retryCount1, &nextAttempt1)

	if status1 != "PENDING" {
		t.Errorf("expected status PENDING after 1st failure, got %s", status1)
	}
	if retryCount1 != 1 {
		t.Errorf("expected retry_count 1, got %d", retryCount1)
	}
	if nextAttempt1.Before(time.Now().UTC()) {
		t.Errorf("expected next_attempt_at in the future, got %v", nextAttempt1)
	}

	// Forçar next_attempt_at para agora para testar a tentativa 2
	_, _ = pool.Exec(ctx, "UPDATE outbox_events SET next_attempt_at = NOW() WHERE id = $1", flakyID)

	// Tentativa 2: Falha
	_, _ = relayer.ProcessBatch(ctx)
	var retryCount2 int
	_ = pool.QueryRow(ctx, "SELECT retry_count FROM outbox_events WHERE id = $1", flakyID).Scan(&retryCount2)
	if retryCount2 != 2 {
		t.Errorf("expected retry_count 2, got %d", retryCount2)
	}

	// Forçar next_attempt_at para agora para a tentativa 3 (sucesso)
	_, _ = pool.Exec(ctx, "UPDATE outbox_events SET next_attempt_at = NOW() WHERE id = $1", flakyID)

	// Tentativa 3: Sucesso
	_, _ = relayer.ProcessBatch(ctx)
	var finalStatus string
	_ = pool.QueryRow(ctx, "SELECT status FROM outbox_events WHERE id = $1", flakyID).Scan(&finalStatus)
	if finalStatus != "PUBLISHED" {
		t.Errorf("expected status PUBLISHED on 3rd attempt, got %s", finalStatus)
	}

	// 2. Testar falha permanente atingindo MaxRetries
	failPermID := uuid.New()
	evPerm := &domain.OutboxEvent{
		ID:            failPermID,
		AggregateType: "PermFail",
		AggregateID:   failPermID.String(),
		EventType:     "PermFailEvent",
		Payload:       json.RawMessage(`{"perm":true}`),
		Status:        domain.OutboxStatusPending,
		CreatedAt:     time.Now().UTC(),
		NextAttemptAt: time.Now().UTC(),
	}

	txPerm, _ := pool.Begin(ctx)
	_ = outboxRepo.Insert(ctx, txPerm, evPerm)
	_ = txPerm.Commit(ctx)

	permPublisher := &flakyPublisher{maxFails: 9999}
	cfgPerm := service.OutboxRelayerConfig{
		BatchSize:     10,
		LeaseDuration: 5 * time.Second,
		MaxRetries:    2, // Apenas 2 tentativas permitidas
		BaseBackoff:   10 * time.Millisecond,
	}
	relayerPerm := service.NewOutboxRelayer(outboxRepo, permPublisher, &cfgPerm)

	// Tentativa 1
	_, _ = relayerPerm.ProcessBatch(ctx)
	_, _ = pool.Exec(ctx, "UPDATE outbox_events SET next_attempt_at = NOW() WHERE id = $1", failPermID)

	// Tentativa 2 (atinge limite de 2)
	_, _ = relayerPerm.ProcessBatch(ctx)

	var permStatus string
	_ = pool.QueryRow(ctx, "SELECT status FROM outbox_events WHERE id = $1", failPermID).Scan(&permStatus)
	if permStatus != "FAILED" {
		t.Errorf("expected status FAILED after reaching MaxRetries, got %s", permStatus)
	}
}

// TestOutbox_LagMetric valida cálculo correto de atraso e backlog pendente
func TestOutbox_LagMetric(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	outboxRepo := repository.NewOutboxRepository(pool)

	// Inserir um evento pendente criado 10 segundos atrás
	evID := uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, status, created_at, next_attempt_at)
		VALUES ($1, 'LagTest', $2, 'LagEvent', '{}', 'PENDING', NOW() - INTERVAL '10 seconds', NOW())
	`, evID, evID.String())
	if err != nil {
		t.Fatalf("failed to insert lag test event: %v", err)
	}

	count, age, err := outboxRepo.GetLag(ctx)
	if err != nil {
		t.Fatalf("failed to get lag: %v", err)
	}

	if count < 1 {
		t.Errorf("expected pending count >= 1, got %d", count)
	}
	if age < 8*time.Second {
		t.Errorf("expected oldest age >= 8s, got %v", age)
	}

	// Limpar o evento
	_, _ = pool.Exec(ctx, "DELETE FROM outbox_events WHERE id = $1", evID)
}
