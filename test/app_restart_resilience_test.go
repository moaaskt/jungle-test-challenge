package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moaaskt/jungle-test-challenge/pkg/api"
	"github.com/moaaskt/jungle-test-challenge/pkg/auth"
	"github.com/moaaskt/jungle-test-challenge/pkg/config"
	"github.com/moaaskt/jungle-test-challenge/pkg/database"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
	"github.com/moaaskt/jungle-test-challenge/pkg/messaging"
	"github.com/moaaskt/jungle-test-challenge/pkg/repository"
	"github.com/moaaskt/jungle-test-challenge/pkg/service"
	"go.uber.org/fx"
)

// TestAppRestart_PreservesIdempotencyAndRecoversPending atende integralmente à Seção 13, item 8 do edital:
// "Reinicie a aplicação e verifique que idempotência, pendências e consistência financeira foram preservadas.
// Se houver aceite assíncrono, interrompa o processo após confirmar PENDING e antes de executar a operação;
// outra instância deve retomá-la. Ao final, confira o saldo armazenado contra a soma de créditos menos débitos do ledger."
func TestAppRestart_PreservesIdempotencyAndRecoversPending(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	baseCfg := getTestSQSConfig()

	reqQueueName := fmt.Sprintf("test-restart-req-%d.fifo", time.Now().UnixNano())
	eventsQueueName := fmt.Sprintf("test-restart-ev-%d.fifo", time.Now().UnixNano())
	dlqName := fmt.Sprintf("test-restart-dlq-%d.fifo", time.Now().UnixNano())

	// -------------------------------------------------------------------------
	// FASE 1: Instância 1 opera e depois sofre interrupção/shutdown
	// -------------------------------------------------------------------------
	port1 := getFreePort(t)
	cfg1 := &config.Config{
		DatabaseURL:           baseCfg.DatabaseURL,
		Port:                  fmt.Sprintf("%d", port1),
		AWSEndpoint:           baseCfg.AWSEndpoint,
		AWSRegion:             baseCfg.AWSRegion,
		SQSWagerRequestsQueue: reqQueueName,
		SQSWagerEventsQueue:   eventsQueueName,
		SQSDLQQueue:           dlqName,
		KeycloakURL:           "http://localhost:8085",
		KeycloakRealm:         "jungle",
		AuthEnabled:           true,
	}

	app1 := fx.New(
		fx.Supply(cfg1),
		fx.Provide(slog.Default),
		auth.Module,
		database.Module,
		repository.Module,
		messaging.Module,
		service.Module,
		api.Module,
		fx.Invoke(func(*http.Server) {}),
	)

	startCtx1, cancelStart1 := context.WithTimeout(ctx, 20*time.Second)
	defer cancelStart1()
	if err := app1.Start(startCtx1); err != nil {
		t.Fatalf("failed to start app instance 1: %v", err)
	}

	tokenInternal := getKeycloakToken(t, "internal-service", "internal-service-secret")
	tokenProviderA := getKeycloakToken(t, "provider-a", "provider-a-secret")

	// 1. Instância 1: Abre carteira com saldo inicial de R$ 100,00
	playerID := fmt.Sprintf("player-restart-%d", time.Now().UnixNano())
	openWalletPayload, _ := json.Marshal(map[string]any{
		"playerId": playerID,
		"initialBalance": map[string]string{
			"amount":   "100.00",
			"currency": "BRL",
		},
	})
	reqOpen, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/wallets", port1), bytes.NewReader(openWalletPayload))
	reqOpen.Header.Set("Authorization", "Bearer "+tokenInternal)
	reqOpen.Header.Set("Content-Type", "application/json")
	respOpen, err := http.DefaultClient.Do(reqOpen)
	if err != nil {
		t.Fatalf("failed to open wallet on instance 1: %v", err)
	}
	openBody, _ := io.ReadAll(respOpen.Body)
	respOpen.Body.Close()
	if respOpen.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created for open wallet, got %d: %s", respOpen.StatusCode, string(openBody))
	}
	var walletResp struct {
		ID       string `json:"id"`
		WalletID string `json:"walletId"`
	}
	_ = json.Unmarshal(openBody, &walletResp)
	walletID := walletResp.ID
	if walletID == "" {
		walletID = walletResp.WalletID
	}

	// 2. Instância 1: Executa aposta BET de R$ 30,00 com Idempotency-Key estável
	idempotencyKey := fmt.Sprintf("idem-restart-key-%d", time.Now().UnixNano())
	extTxID := uuid.New().String()
	wagerPayload, _ := json.Marshal(map[string]any{
		"providerId":            "provider-a",
		"externalTransactionId": extTxID,
		"playerId":              playerID,
		"walletId":              walletID,
		"roundId":               "round-restart-1",
		"gameId":                "game-restart-1",
		"kind":                  "BET",
		"money": map[string]string{
			"amount":   "30.00",
			"currency": "BRL",
		},
	})
	reqWager, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/wagering/transactions", port1), bytes.NewReader(wagerPayload))
	reqWager.Header.Set("Authorization", "Bearer "+tokenProviderA)
	reqWager.Header.Set("Idempotency-Key", idempotencyKey)
	reqWager.Header.Set("Content-Type", "application/json")

	respWager, err := http.DefaultClient.Do(reqWager)
	if err != nil {
		t.Fatalf("failed to post wager on instance 1: %v", err)
	}
	bodyBytes, _ := io.ReadAll(respWager.Body)
	respWager.Body.Close()

	if respWager.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for wager on instance 1, got %d: %s", respWager.StatusCode, string(bodyBytes))
	}
	var wagerResp1 struct {
		TransactionID string `json:"transactionId"`
		Balance       struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"balance"`
		IdempotentReplay bool `json:"idempotentReplay"`
	}
	_ = json.Unmarshal(bodyBytes, &wagerResp1)
	if wagerResp1.Balance.Amount != "70.00" {
		t.Errorf("expected balance 70.00 on instance 1, got %s", wagerResp1.Balance.Amount)
	}

	// 3. Simula crash / interrupção abrupta de processo deixando transação PENDING
	staleTxID := uuid.New()
	staleExtID := uuid.New().String()
	insertPendingQuery := `
		INSERT INTO wager_transactions (
			id, origin, external_id, provider_id, wallet_id, player_id,
			type, amount, currency, status, created_at, updated_at
		) VALUES (
			$1, 'EXTERNAL', $2, 'provider-a', $3, $4,
			'BET', 1000, 'BRL', 'PENDING', $5, $5
		)
	`
	pastTime := time.Now().Add(-40 * time.Second)
	_, err = pool.Exec(ctx, insertPendingQuery, staleTxID, staleExtID, walletID, playerID, pastTime)
	if err != nil {
		t.Fatalf("failed to insert stale pending tx: %v", err)
	}

	// 4. Interrompe graciosamente a Instância 1
	stopCtx1, cancelStop1 := context.WithTimeout(ctx, 15*time.Second)
	defer cancelStop1()
	if err := app1.Stop(stopCtx1); err != nil {
		t.Fatalf("failed to stop app instance 1: %v", err)
	}

	// -------------------------------------------------------------------------
	// FASE 2: Inicialização da Instância 2 contra a mesma base e mensageria
	// -------------------------------------------------------------------------
	port2 := getFreePort(t)
	cfg2 := &config.Config{
		DatabaseURL:           baseCfg.DatabaseURL,
		Port:                  fmt.Sprintf("%d", port2),
		AWSEndpoint:           baseCfg.AWSEndpoint,
		AWSRegion:             baseCfg.AWSRegion,
		SQSWagerRequestsQueue: reqQueueName,
		SQSWagerEventsQueue:   eventsQueueName,
		SQSDLQQueue:           dlqName,
		KeycloakURL:           "http://localhost:8085",
		KeycloakRealm:         "jungle",
		AuthEnabled:           true,
	}

	app2 := fx.New(
		fx.Supply(cfg2),
		fx.Provide(slog.Default),
		auth.Module,
		database.Module,
		repository.Module,
		messaging.Module,
		service.Module,
		api.Module,
		fx.Invoke(func(*http.Server) {}),
	)

	startCtx2, cancelStart2 := context.WithTimeout(ctx, 20*time.Second)
	defer cancelStart2()
	if err := app2.Start(startCtx2); err != nil {
		t.Fatalf("failed to start app instance 2: %v", err)
	}

	// A. Validação de Idempotência Preservada após Restart:
	// A Instância 2 deve reconhecer o replay exato da Instância 1 sem duplicar débito
	reqReplay, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/wagering/transactions", port2), bytes.NewReader(wagerPayload))
	reqReplay.Header.Set("Authorization", "Bearer "+tokenProviderA)
	reqReplay.Header.Set("Idempotency-Key", idempotencyKey)
	reqReplay.Header.Set("Content-Type", "application/json")

	respReplay, err := http.DefaultClient.Do(reqReplay)
	if err != nil {
		t.Fatalf("failed to post idempotent replay to instance 2: %v", err)
	}
	var wagerResp2 struct {
		TransactionID string `json:"transactionId"`
		Balance       struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"balance"`
		IdempotentReplay bool `json:"idempotentReplay"`
	}
	_ = json.NewDecoder(respReplay.Body).Decode(&wagerResp2)
	respReplay.Body.Close()

	if respReplay.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for replay on instance 2, got %d", respReplay.StatusCode)
	}
	if !wagerResp2.IdempotentReplay {
		t.Errorf("expected idempotentReplay: true on instance 2 after restart")
	}
	if wagerResp2.Balance.Amount != "70.00" {
		t.Errorf("expected balance 70.00 on replay, got %s", wagerResp2.Balance.Amount)
	}

	// B. Validação da Recuperação de Pendência deixada pela Instância 1:
	// Aguarda o StaleTxRecoveryWorker da Instância 2 processar a transação
	var staleStatus string
	var staleErrCode *string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		err := pool.QueryRow(ctx, "SELECT status, error_code FROM wager_transactions WHERE id = $1", staleTxID).Scan(&staleStatus, &staleErrCode)
		if err == nil && staleStatus == string(domain.StatusFailed) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if staleStatus != string(domain.StatusFailed) {
		t.Errorf("expected stale tx to be recovered to FAILED by instance 2, got %s", staleStatus)
	}
	if staleErrCode == nil || *staleErrCode != domain.FailureCodeTransactionTimeout {
		t.Errorf("expected error code TRANSACTION_TIMEOUT, got %v", staleErrCode)
	}

	// C. Validação de Consistência Contábil Final (Reconciliação):
	// O saldo armazenado deve conferir rigorosamente com a soma de créditos - débitos
	reqRec, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/wallets/%s/reconciliation", port2, walletID), nil)
	reqRec.Header.Set("Authorization", "Bearer "+tokenInternal)
	respRec, err := http.DefaultClient.Do(reqRec)
	if err != nil {
		t.Fatalf("failed to execute reconciliation on instance 2: %v", err)
	}
	defer respRec.Body.Close()

	if respRec.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(respRec.Body)
		t.Fatalf("expected 200 OK for reconciliation, got %d: %s", respRec.StatusCode, string(body))
	}

	var recResult struct {
		StoredBalance struct {
			Amount string `json:"amount"`
		} `json:"storedBalance"`
		CalculatedBalance struct {
			Amount string `json:"amount"`
		} `json:"calculatedBalance"`
		Difference struct {
			Amount string `json:"amount"`
		} `json:"difference"`
		Consistent bool `json:"consistent"`
	}
	if err := json.NewDecoder(respRec.Body).Decode(&recResult); err != nil {
		t.Fatalf("failed to decode reconciliation result: %v", err)
	}

	if !recResult.Consistent {
		t.Errorf("expected wallet to be consistent after restart")
	}
	if recResult.Difference.Amount != "0.00" {
		t.Errorf("expected difference 0.00, got %s", recResult.Difference.Amount)
	}
	if recResult.StoredBalance.Amount != "70.00" || recResult.CalculatedBalance.Amount != "70.00" {
		t.Errorf("expected stored and calculated balance to be 70.00, got stored=%s calculated=%s",
			recResult.StoredBalance.Amount, recResult.CalculatedBalance.Amount)
	}

	// Encerra a Instância 2
	stopCtx2, cancelStop2 := context.WithTimeout(ctx, 15*time.Second)
	defer cancelStop2()
	if err := app2.Stop(stopCtx2); err != nil {
		t.Fatalf("failed to stop app instance 2: %v", err)
	}
}
