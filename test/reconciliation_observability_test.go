package test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moaaskt/jungle-test-challenge/pkg/api"
	"github.com/moaaskt/jungle-test-challenge/pkg/auth"
	"github.com/moaaskt/jungle-test-challenge/pkg/config"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
	"github.com/moaaskt/jungle-test-challenge/pkg/messaging"
	"github.com/moaaskt/jungle-test-challenge/pkg/repository"
	"github.com/moaaskt/jungle-test-challenge/pkg/service"
)

// setupReconciliationTestServer monta um ambiente de teste HTTP completo
// com Keycloak real, Postgres real e SQS/LocalStack real.
func setupReconciliationTestServer(t *testing.T) (*httptest.Server, *pgxpool.Pool, service.WagerService, func()) {
	t.Helper()
	pool := getTestPool(t)

	cfgAuth := &config.Config{
		Port:          "8080",
		KeycloakURL:   "http://localhost:8085",
		KeycloakRealm: "jungle",
		AuthEnabled:   true,
	}

	validator, err := auth.NewJWKSTokenValidator(cfgAuth)
	if err != nil {
		t.Fatalf("failed to create JWKS validator: %v", err)
	}

	walletRepo := repository.NewWalletRepository()
	wagerRepo := repository.NewWagerTransactionRepository()
	ledgerRepo := repository.NewLedgerRepository()
	idemRepo := repository.NewIdempotencyRepository()
	outboxRepo := repository.NewOutboxRepository(pool)
	inboxRepo := repository.NewInboxRepository()

	svc := service.NewWagerService(pool, walletRepo, wagerRepo, ledgerRepo, idemRepo, outboxRepo, inboxRepo)

	sqsCfg := getTestSQSConfig()
	sqsClient, err := messaging.NewSQSClient(sqsCfg)
	if err != nil {
		t.Fatalf("failed to create SQS client: %v", err)
	}

	handlers := api.NewHandlers(svc).WithHealthDependencies(pool, sqsClient)
	mux := api.NewMux(handlers)

	rootHandler := auth.AuthMiddleware(validator, true)(mux)
	ts := httptest.NewServer(rootHandler)

	cleanup := func() {
		ts.Close()
		pool.Close()
	}

	return ts, pool, svc, cleanup
}

// 1. TestReconciliation_ConsistentWallet: Reconciliação em carteira íntegra
// Garante cálculo exato sem floats, consistent == true, difference == "0.00" e saldo inalterado.
func TestReconciliation_ConsistentWallet(t *testing.T) {
	ts, pool, svc, cleanup := setupReconciliationTestServer(t)
	defer cleanup()

	ctx := context.Background()
	playerID := fmt.Sprintf("player-rec-ok-%d", time.Now().UnixNano())

	// Abre carteira com saldo R$ 100,00
	wallet, err := svc.OpenWallet(ctx, playerID, "BRL", 10000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	// Executa BET de R$ 20,00
	providerID := "provider-a"
	betExtID := uuid.New().String()
	betReq := service.ProcessWagerRequest{
		Origin:     domain.OriginExternal,
		PlayerID:   playerID,
		ProviderID: &providerID,
		ExternalID: &betExtID,
		Type:       domain.TransactionTypeBet,
		Amount:     2000,
		Currency:   "BRL",
	}
	if _, err := svc.ProcessWager(ctx, betReq); err != nil {
		t.Fatalf("failed to process BET: %v", err)
	}

	// Executa WIN de R$ 50,00
	winExtID := uuid.New().String()
	winReq := service.ProcessWagerRequest{
		Origin:     domain.OriginExternal,
		PlayerID:   playerID,
		ProviderID: &providerID,
		ExternalID: &winExtID,
		Type:       domain.TransactionTypeWin,
		Amount:     5000,
		Currency:   "BRL",
	}
	if _, err := svc.ProcessWager(ctx, winReq); err != nil {
		t.Fatalf("failed to process WIN: %v", err)
	}

	// Dispara reconciliação via POST /wallets/{walletId}/reconciliation com internal-service token
	token := getKeycloakToken(t, "internal-service", "internal-service-secret")
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/wallets/%s/reconciliation", ts.URL, wallet.ID), nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to execute request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 OK, got %d: %s", resp.StatusCode, string(body))
	}

	type MoneyDTO struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	}
	var res struct {
		WalletID          string   `json:"walletId"`
		StoredBalance     MoneyDTO `json:"storedBalance"`
		CalculatedBalance MoneyDTO `json:"calculatedBalance"`
		Difference        MoneyDTO `json:"difference"`
		Consistent        bool     `json:"consistent"`
		CheckedEntries    int      `json:"checkedEntries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if res.WalletID != wallet.ID.String() {
		t.Errorf("expected walletId %s, got %s", wallet.ID, res.WalletID)
	}
	if res.StoredBalance.Amount != "130.00" {
		t.Errorf("expected storedBalance 130.00, got %s", res.StoredBalance.Amount)
	}
	if res.CalculatedBalance.Amount != "130.00" {
		t.Errorf("expected calculatedBalance 130.00, got %s", res.CalculatedBalance.Amount)
	}
	if res.Difference.Amount != "0.00" {
		t.Errorf("expected difference 0.00, got %s", res.Difference.Amount)
	}
	if !res.Consistent {
		t.Errorf("expected consistent true, got %v", res.Consistent)
	}
	if res.CheckedEntries != 3 { // abertura + bet + win
		t.Errorf("expected checkedEntries 3, got %d", res.CheckedEntries)
	}

	// Invariante Mandatório (Seção 9): Saldo da carteira no banco NUNCA deve ser alterado
	var currentBalance int64
	err = pool.QueryRow(ctx, "SELECT balance FROM wallets WHERE id = $1", wallet.ID).Scan(&currentBalance)
	if err != nil {
		t.Fatalf("failed to query wallet balance: %v", err)
	}
	if currentBalance != 13000 {
		t.Errorf("expected wallet balance in DB to remain 13000, got %d", currentBalance)
	}
}

// 2. TestReconciliation_InconsistentWallet: Reconciliação detecta divergência
// Invariante Mandatório: Reporta consistent == false e cálculo da divergência sem alterar o saldo da carteira!
func TestReconciliation_InconsistentWallet(t *testing.T) {
	ts, pool, svc, cleanup := setupReconciliationTestServer(t)
	defer cleanup()

	ctx := context.Background()
	playerID := fmt.Sprintf("player-rec-diverge-%d", time.Now().UnixNano())

	// Abre carteira com R$ 100,00
	wallet, err := svc.OpenWallet(ctx, playerID, "BRL", 10000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	// Executa aposta de R$ 20,00 (saldo real vai para R$ 80,00)
	providerID := "provider-a"
	betExtID := uuid.New().String()
	betReq := service.ProcessWagerRequest{
		Origin:     domain.OriginExternal,
		PlayerID:   playerID,
		ProviderID: &providerID,
		ExternalID: &betExtID,
		Type:       domain.TransactionTypeBet,
		Amount:     2000,
		Currency:   "BRL",
	}
	if _, err := svc.ProcessWager(ctx, betReq); err != nil {
		t.Fatalf("failed to process BET: %v", err)
	}

	// Induz divergência artificial no banco: altera apenas a tabela wallets (+ R$ 5,00 = 500 centavos)
	// Saldo armazenado na carteira: 85,00. Saldo calculado pelo ledger: 80,00. Diferença esperada: 5,00.
	_, err = pool.Exec(ctx, "UPDATE wallets SET balance = balance + 500 WHERE id = $1", wallet.ID)
	if err != nil {
		t.Fatalf("failed to induce divergence: %v", err)
	}

	token := getKeycloakToken(t, "internal-service", "internal-service-secret")
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/wallets/%s/reconciliation", ts.URL, wallet.ID), nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to execute request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 OK, got %d: %s", resp.StatusCode, string(body))
	}

	type MoneyDTO struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	}
	var res struct {
		WalletID          string   `json:"walletId"`
		StoredBalance     MoneyDTO `json:"storedBalance"`
		CalculatedBalance MoneyDTO `json:"calculatedBalance"`
		Difference        MoneyDTO `json:"difference"`
		Consistent        bool     `json:"consistent"`
		CheckedEntries    int      `json:"checkedEntries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if res.Consistent {
		t.Errorf("expected consistent false due to divergence, got true")
	}
	if res.StoredBalance.Amount != "85.00" {
		t.Errorf("expected storedBalance 85.00, got %s", res.StoredBalance.Amount)
	}
	if res.CalculatedBalance.Amount != "80.00" {
		t.Errorf("expected calculatedBalance 80.00, got %s", res.CalculatedBalance.Amount)
	}
	if res.Difference.Amount != "5.00" {
		t.Errorf("expected difference 5.00, got %s", res.Difference.Amount)
	}
	if res.CheckedEntries != 2 { // abertura + bet
		t.Errorf("expected checkedEntries 2, got %d", res.CheckedEntries)
	}

	// Invariante Mandatório (Seção 9): Mesmo com divergência, o saldo da carteira NÃO deve ser alterado
	var currentBalance int64
	err = pool.QueryRow(ctx, "SELECT balance FROM wallets WHERE id = $1", wallet.ID).Scan(&currentBalance)
	if err != nil {
		t.Fatalf("failed to query wallet balance: %v", err)
	}
	if currentBalance != 8500 {
		t.Errorf("expected wallet balance in DB to remain 8500 (unmodified), got %d", currentBalance)
	}
}

// 3. TestReconciliation_WalletNotFound: 404 para carteira inexistente com RFC 7807
func TestReconciliation_WalletNotFound(t *testing.T) {
	ts, _, _, cleanup := setupReconciliationTestServer(t)
	defer cleanup()

	token := getKeycloakToken(t, "internal-service", "internal-service-secret")
	randomWalletID := uuid.New().String()
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/wallets/%s/reconciliation", ts.URL, randomWalletID), nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to execute request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 404 Not Found, got %d: %s", resp.StatusCode, string(body))
	}

	var errResp struct {
		Code   string `json:"code"`
		Detail string `json:"detail"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if errResp.Code != "WALLET_NOT_FOUND" {
		t.Errorf("expected code WALLET_NOT_FOUND, got %s", errResp.Code)
	}
}

// 4. TestReconciliation_AuthorizationIsolation: Apenas internal-service pode reconciliar
// Provedores externos (provider-a, provider-b) devem receber 403 Forbidden.
func TestReconciliation_AuthorizationIsolation(t *testing.T) {
	ts, _, svc, cleanup := setupReconciliationTestServer(t)
	defer cleanup()

	ctx := context.Background()
	playerID := fmt.Sprintf("player-rec-auth-%d", time.Now().UnixNano())
	wallet, err := svc.OpenWallet(ctx, playerID, "BRL", 5000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	url := fmt.Sprintf("%s/wallets/%s/reconciliation", ts.URL, wallet.ID)

	// Cenário A: Sem Token -> 401 Unauthorized
	respNoAuth, err := http.Post(url, "application/json", nil)
	if err != nil {
		t.Fatalf("failed to send request: %v", err)
	}
	respNoAuth.Body.Close()
	if respNoAuth.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 for unauthenticated request, got %d", respNoAuth.StatusCode)
	}

	// Cenário B: Token de provider-a -> 403 Forbidden
	tokenProviderA := getKeycloakToken(t, "provider-a", "provider-a-secret")
	reqA, _ := http.NewRequest(http.MethodPost, url, nil)
	reqA.Header.Set("Authorization", "Bearer "+tokenProviderA)
	respA, err := http.DefaultClient.Do(reqA)
	if err != nil {
		t.Fatalf("failed to send request: %v", err)
	}
	respA.Body.Close()
	if respA.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden for provider-a, got %d", respA.StatusCode)
	}

	// Cenário C: Token de provider-b -> 403 Forbidden
	tokenProviderB := getKeycloakToken(t, "provider-b", "provider-b-secret")
	reqB, _ := http.NewRequest(http.MethodPost, url, nil)
	reqB.Header.Set("Authorization", "Bearer "+tokenProviderB)
	respB, err := http.DefaultClient.Do(reqB)
	if err != nil {
		t.Fatalf("failed to send request: %v", err)
	}
	respB.Body.Close()
	if respB.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden for provider-b, got %d", respB.StatusCode)
	}

	// Cenário D: Token de internal-service -> 200 OK
	tokenInternal := getKeycloakToken(t, "internal-service", "internal-service-secret")
	reqInternal, _ := http.NewRequest(http.MethodPost, url, nil)
	reqInternal.Header.Set("Authorization", "Bearer "+tokenInternal)
	respInternal, err := http.DefaultClient.Do(reqInternal)
	if err != nil {
		t.Fatalf("failed to send request: %v", err)
	}
	respInternal.Body.Close()
	if respInternal.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for internal-service, got %d", respInternal.StatusCode)
	}
}

// 5. TestObservability_PrometheusMetricsEndpoint: Valida /metrics sem auth e catálogo Prometheus
func TestObservability_PrometheusMetricsEndpoint(t *testing.T) {
	ts, _, _, cleanup := setupReconciliationTestServer(t)
	defer cleanup()

	// /metrics é bypass público
	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatalf("failed to request /metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for /metrics, got %d", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read /metrics body: %v", err)
	}
	body := string(bodyBytes)

	// Validação do catálogo de métricas exigidas na Seção 12
	expectedMetrics := []string{
		"wager_transactions_total",
		"wager_duplicates_total",
		"sqs_retries_total",
		"sqs_dlq_total",
		"concurrency_conflicts_total",
		"outbox_lag_seconds",
		"wager_processing_duration_seconds",
		"reconciliation_divergences_total",
	}

	for _, metricName := range expectedMetrics {
		if !strings.Contains(body, metricName) {
			t.Errorf("expected /metrics output to contain metric '%s'", metricName)
		}
	}
}

// 6. TestHealth_Readiness_DeepCheck: Valida /health/live e /health/ready com teste ativo de PostgreSQL e SQS
func TestHealth_Readiness_DeepCheck(t *testing.T) {
	ts, _, _, cleanup := setupReconciliationTestServer(t)
	defer cleanup()

	// 1. Liveness check
	respLive, err := http.Get(ts.URL + "/health/live")
	if err != nil {
		t.Fatalf("failed to call /health/live: %v", err)
	}
	defer respLive.Body.Close()

	if respLive.StatusCode != http.StatusOK {
		t.Errorf("expected 200 for /health/live, got %d", respLive.StatusCode)
	}

	var liveBody map[string]string
	if err := json.NewDecoder(respLive.Body).Decode(&liveBody); err != nil {
		t.Fatalf("failed to decode live response: %v", err)
	}
	if liveBody["status"] != "UP" && liveBody["status"] != "ALIVE" {
		t.Errorf("expected status UP or ALIVE, got %s", liveBody["status"])
	}

	// 2. Readiness check profundo (Postgres + SQS)
	respReady, err := http.Get(ts.URL + "/health/ready")
	if err != nil {
		t.Fatalf("failed to call /health/ready: %v", err)
	}
	defer respReady.Body.Close()

	if respReady.StatusCode != http.StatusOK {
		t.Errorf("expected 200 for /health/ready, got %d", respReady.StatusCode)
	}

	var readyBody map[string]any
	if err := json.NewDecoder(respReady.Body).Decode(&readyBody); err != nil {
		t.Fatalf("failed to decode ready response: %v", err)
	}
	if readyBody["status"] != "READY" {
		t.Errorf("expected status READY, got %v", readyBody["status"])
	}
	if readyBody["database"] != "UP" {
		t.Errorf("expected database UP, got %v", readyBody["database"])
	}
	if readyBody["sqs"] != "UP" {
		t.Errorf("expected sqs UP, got %v", readyBody["sqs"])
	}
}

// 7. TestStaleTxRecovery_InterruptedProcess: Valida recuperação de transação presa em PENDING
// Cenário 1: Crash antes de tocar no ledger -> Fail com FailureCodeTransactionTimeout
// Cenário 2: Crash após tocar no ledger -> Process
func TestStaleTxRecovery_InterruptedProcess(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	wagerRepo := repository.NewWagerTransactionRepository()
	walletRepo := repository.NewWalletRepository()
	ledgerRepo := repository.NewLedgerRepository()
	idemRepo := repository.NewIdempotencyRepository()
	outboxRepo := repository.NewOutboxRepository(pool)
	inboxRepo := repository.NewInboxRepository()

	svc := service.NewWagerService(pool, walletRepo, wagerRepo, ledgerRepo, idemRepo, outboxRepo, inboxRepo)

	playerID := fmt.Sprintf("player-stale-%d", time.Now().UnixNano())
	wallet, err := svc.OpenWallet(ctx, playerID, "BRL", 20000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	workerCfg := service.StaleTxRecoveryConfig{
		Interval:       1 * time.Second,
		StaleThreshold: 10 * time.Millisecond, // curto para o teste
		BatchSize:      10,
	}
	worker := service.NewStaleTxRecoveryWorker(pool, wagerRepo, workerCfg, nil)

	// Sub-teste A: Transação sem ledger (crash antes de alterar a carteira)
	// Deve ser transicionada para FAILED com TRANSACTION_TIMEOUT
	staleTxID1 := uuid.New()
	providerID := "provider-a"
	extID1 := uuid.New().String()
	insertQuery := `
		INSERT INTO wager_transactions (
			id, origin, external_id, provider_id, wallet_id, player_id,
			type, amount, currency, status, created_at, updated_at
		) VALUES (
			$1, 'EXTERNAL', $2, $3, $4, $5,
			'BET', 1000, 'BRL', 'PENDING', $6, $6
		)
	`
	pastTime := time.Now().Add(-5 * time.Second)
	_, err = pool.Exec(ctx, insertQuery, staleTxID1, extID1, providerID, wallet.ID, playerID, pastTime)
	if err != nil {
		t.Fatalf("failed to insert stale pending tx 1: %v", err)
	}

	// Sub-teste B: Transação COM ledger (crash após gravar ledger mas antes de marcar PROCESSED)
	// Deve ser recuperada para PROCESSED
	staleTxID2 := uuid.New()
	extID2 := uuid.New().String()
	_, err = pool.Exec(ctx, insertQuery, staleTxID2, extID2, providerID, wallet.ID, playerID, pastTime)
	if err != nil {
		t.Fatalf("failed to insert stale pending tx 2: %v", err)
	}

	insertLedgerQuery := `
		INSERT INTO wallet_ledger_entries (
			id, wallet_id, transaction_id, type, amount, balance_before, balance_after, created_at
		) VALUES (
			$1, $2, $3, 'DEBIT', 1000, 20000, 19000, $4
		)
	`
	_, err = pool.Exec(ctx, insertLedgerQuery, uuid.New(), wallet.ID, staleTxID2, pastTime)
	if err != nil {
		t.Fatalf("failed to insert mock ledger for stale tx 2: %v", err)
	}

	// Executa uma rodada do worker
	recovered, err := worker.RecoverOnce(ctx)
	if err != nil {
		t.Fatalf("failed to execute RecoverOnce: %v", err)
	}
	if recovered < 2 {
		t.Errorf("expected at least 2 recovered transactions, got %d", recovered)
	}

	// Valida tx 1 (sem ledger) -> FAILED com TRANSACTION_TIMEOUT
	var status1 string
	var errCode1 *string
	err = pool.QueryRow(ctx, "SELECT status, error_code FROM wager_transactions WHERE id = $1", staleTxID1).Scan(&status1, &errCode1)
	if err != nil {
		t.Fatalf("failed to query stale tx 1: %v", err)
	}
	if status1 != string(domain.StatusFailed) {
		t.Errorf("expected status FAILED for tx 1, got %s", status1)
	}
	if errCode1 == nil || *errCode1 != domain.FailureCodeTransactionTimeout {
		t.Errorf("expected error code TRANSACTION_TIMEOUT, got %v", errCode1)
	}

	// Valida tx 2 (com ledger) -> PROCESSED
	var status2 string
	err = pool.QueryRow(ctx, "SELECT status FROM wager_transactions WHERE id = $1", staleTxID2).Scan(&status2)
	if err != nil {
		t.Fatalf("failed to query stale tx 2: %v", err)
	}
	if status2 != string(domain.StatusProcessed) {
		t.Errorf("expected status PROCESSED for tx 2, got %s", status2)
	}
}
