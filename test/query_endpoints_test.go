package test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/moaaskt/jungle-test-challenge/pkg/api"
	"github.com/moaaskt/jungle-test-challenge/pkg/auth"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
	"github.com/moaaskt/jungle-test-challenge/pkg/repository"
	"github.com/moaaskt/jungle-test-challenge/pkg/service"
)

func setupTestServer(t *testing.T) (*httptest.Server, service.WagerService, func()) {
	pool := getTestPool(t)

	walletRepo := repository.NewWalletRepository()
	wagerRepo := repository.NewWagerTransactionRepository()
	ledgerRepo := repository.NewLedgerRepository()
	idemRepo := repository.NewIdempotencyRepository()
	outboxRepo := repository.NewOutboxRepository(pool)
	inboxRepo := repository.NewInboxRepository()

	svc := service.NewWagerService(pool, walletRepo, wagerRepo, ledgerRepo, idemRepo, outboxRepo, inboxRepo)
	handlers := api.NewHandlers(svc)
	mux := api.NewMux(handlers)

	ts := httptest.NewServer(auth.AuthMiddleware(nil, false)(mux))
	cleanup := func() {
		ts.Close()
		pool.Close()
	}

	return ts, svc, cleanup
}

// TestQuery_GetWallet valida o endpoint GET /wallets/{walletId} (Seção 9)
func TestQuery_GetWallet(t *testing.T) {
	ts, svc, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()
	playerID := fmt.Sprintf("player-query-%s", uuid.New().String()[:8])
	wallet, err := svc.OpenWallet(ctx, playerID, "BRL", 25000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	resp, err := http.Get(fmt.Sprintf("%s/wallets/%s", ts.URL, wallet.ID.String()))
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var res struct {
		WalletID string `json:"walletId"`
		PlayerID string `json:"playerId"`
		Currency string `json:"currency"`
		Balance  struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"balance"`
		Version int `json:"version"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if res.WalletID != wallet.ID.String() {
		t.Errorf("expected walletId %s, got %s", wallet.ID, res.WalletID)
	}
	if res.Balance.Amount != "250.00" {
		t.Errorf("expected balance 250.00, got %s", res.Balance.Amount)
	}
	if res.Version < 1 {
		t.Errorf("expected version >= 1, got %d", res.Version)
	}
}

// TestQuery_GetLedger_CursorPagination valida paginação keyset com cursor opaco em GET /wallets/{walletId}/ledger
func TestQuery_GetLedger_CursorPagination(t *testing.T) {
	ts, svc, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()
	playerID := fmt.Sprintf("player-ledger-%s", uuid.New().String()[:8])
	wallet, err := svc.OpenWallet(ctx, playerID, "BRL", 50000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	providerID := "provider-ledger-test"

	// Gerar 4 transações para totalizar 5 entradas no ledger (1 do OPENING + 4 de BETs)
	for i := 0; i < 4; i++ {
		extID := fmt.Sprintf("tx-led-%d-%s", i, uuid.New().String())
		_, err := svc.ProcessWager(ctx, service.ProcessWagerRequest{
			Origin:         domain.OriginExternal,
			PlayerID:       playerID,
			ProviderID:     &providerID,
			ExternalID:     &extID,
			IdempotencyKey: &extID,
			Type:           domain.TransactionTypeBet,
			Amount:         1000,
			Currency:       "BRL",
		})
		if err != nil {
			t.Fatalf("failed to process bet: %v", err)
		}
	}

	type LedgerResponse struct {
		WalletID string `json:"walletId"`
		Entries  []struct {
			ID            string `json:"id"`
			TransactionID string `json:"transactionId"`
			Type          string `json:"type"`
			Amount        struct {
				Amount   string `json:"amount"`
				Currency string `json:"currency"`
			} `json:"amount"`
		} `json:"entries"`
		NextCursor *string `json:"nextCursor"`
		Limit      int     `json:"limit"`
	}

	// Página 1: limit 2
	resp1, err := http.Get(fmt.Sprintf("%s/wallets/%s/ledger?limit=2", ts.URL, wallet.ID.String()))
	if err != nil {
		t.Fatalf("request page 1 failed: %v", err)
	}
	defer resp1.Body.Close()

	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("page 1 status: %d", resp1.StatusCode)
	}

	var page1 LedgerResponse
	if err := json.NewDecoder(resp1.Body).Decode(&page1); err != nil {
		t.Fatalf("failed to decode page 1: %v", err)
	}

	if len(page1.Entries) != 2 {
		t.Fatalf("expected 2 entries in page 1, got %d", len(page1.Entries))
	}
	if page1.NextCursor == nil || *page1.NextCursor == "" {
		t.Fatalf("expected non-empty nextCursor in page 1")
	}

	// Página 2: limit 2 com cursor da página 1
	resp2, err := http.Get(fmt.Sprintf("%s/wallets/%s/ledger?limit=2&cursor=%s", ts.URL, wallet.ID.String(), *page1.NextCursor))
	if err != nil {
		t.Fatalf("request page 2 failed: %v", err)
	}
	defer resp2.Body.Close()

	var page2 LedgerResponse
	if err := json.NewDecoder(resp2.Body).Decode(&page2); err != nil {
		t.Fatalf("failed to decode page 2: %v", err)
	}

	if len(page2.Entries) != 2 {
		t.Fatalf("expected 2 entries in page 2, got %d", len(page2.Entries))
	}
	if page2.NextCursor == nil || *page2.NextCursor == "" {
		t.Fatalf("expected non-empty nextCursor in page 2")
	}

	// Validar que os itens de page 2 são distintos de page 1
	if page2.Entries[0].ID == page1.Entries[0].ID || page2.Entries[0].ID == page1.Entries[1].ID {
		t.Fatalf("overlapping entry across pages: %s", page2.Entries[0].ID)
	}

	// Página 3: restante com cursor da página 2
	resp3, err := http.Get(fmt.Sprintf("%s/wallets/%s/ledger?limit=2&cursor=%s", ts.URL, wallet.ID.String(), *page2.NextCursor))
	if err != nil {
		t.Fatalf("request page 3 failed: %v", err)
	}
	defer resp3.Body.Close()

	var page3 LedgerResponse
	if err := json.NewDecoder(resp3.Body).Decode(&page3); err != nil {
		t.Fatalf("failed to decode page 3: %v", err)
	}

	if len(page3.Entries) != 1 {
		t.Fatalf("expected 1 entry in page 3, got %d", len(page3.Entries))
	}
	// Última página -> nextCursor deve ser nulo
	if page3.NextCursor != nil {
		t.Fatalf("expected nil nextCursor on last page, got: %s", *page3.NextCursor)
	}
}

// TestQuery_GetTransaction valida GET /wagering/transactions/{transactionId}
func TestQuery_GetTransaction(t *testing.T) {
	ts, svc, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()
	playerID := fmt.Sprintf("player-tx-%s", uuid.New().String()[:8])
	_, err := svc.OpenWallet(ctx, playerID, "BRL", 50000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	providerID := "provider-get-tx"
	extID := fmt.Sprintf("ext-get-%s", uuid.New().String())

	res, err := svc.ProcessWager(ctx, service.ProcessWagerRequest{
		Origin:         domain.OriginExternal,
		PlayerID:       playerID,
		ProviderID:     &providerID,
		ExternalID:     &extID,
		IdempotencyKey: &extID,
		Type:           domain.TransactionTypeBet,
		Amount:         1550,
		Currency:       "BRL",
	})
	if err != nil {
		t.Fatalf("failed to process wager: %v", err)
	}

	resp, err := http.Get(fmt.Sprintf("%s/wagering/transactions/%s", ts.URL, res.TransactionID.String()))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var txRes struct {
		TransactionID         string `json:"transactionId"`
		Origin                string `json:"origin"`
		ExternalTransactionID string `json:"externalTransactionId"`
		ProviderID            string `json:"providerId"`
		WalletID              string `json:"walletId"`
		PlayerID              string `json:"playerId"`
		Type                  string `json:"type"`
		Amount                struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"amount"`
		Status string `json:"status"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&txRes); err != nil {
		t.Fatalf("failed to decode tx response: %v", err)
	}

	if txRes.TransactionID != res.TransactionID.String() {
		t.Errorf("expected txId %s, got %s", res.TransactionID, txRes.TransactionID)
	}
	if txRes.ExternalTransactionID != extID {
		t.Errorf("expected externalId %s, got %s", extID, txRes.ExternalTransactionID)
	}
	if txRes.Status != "PROCESSED" {
		t.Errorf("expected PROCESSED, got %s", txRes.Status)
	}
	if txRes.Amount.Amount != "15.50" {
		t.Errorf("expected amount 15.50, got %s", txRes.Amount.Amount)
	}
}

// TestQuery_GetTransactionByExternal valida rota hierárquica GET /providers/{providerId}/wagering/transactions/{externalTransactionId} (Seção 9)
func TestQuery_GetTransactionByExternal(t *testing.T) {
	ts, svc, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()
	playerID := fmt.Sprintf("player-hier-%s", uuid.New().String()[:8])
	_, err := svc.OpenWallet(ctx, playerID, "BRL", 50000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	providerID := "provider-hier-test"
	extID := fmt.Sprintf("ext-hier-%s", uuid.New().String())

	res, err := svc.ProcessWager(ctx, service.ProcessWagerRequest{
		Origin:         domain.OriginExternal,
		PlayerID:       playerID,
		ProviderID:     &providerID,
		ExternalID:     &extID,
		IdempotencyKey: &extID,
		Type:           domain.TransactionTypeBet,
		Amount:         3000,
		Currency:       "BRL",
	})
	if err != nil {
		t.Fatalf("failed to process wager: %v", err)
	}

	url := fmt.Sprintf("%s/providers/%s/wagering/transactions/%s", ts.URL, providerID, extID)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var txRes struct {
		TransactionID         string `json:"transactionId"`
		ProviderID            string `json:"providerId"`
		ExternalTransactionID string `json:"externalTransactionId"`
		Status                string `json:"status"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&txRes); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if txRes.TransactionID != res.TransactionID.String() {
		t.Errorf("expected %s, got %s", res.TransactionID, txRes.TransactionID)
	}
	if txRes.ProviderID != providerID {
		t.Errorf("expected providerId %s, got %s", providerID, txRes.ProviderID)
	}
	if txRes.ExternalTransactionID != extID {
		t.Errorf("expected externalTransactionId %s, got %s", extID, txRes.ExternalTransactionID)
	}
}

// TestQuery_NotFoundEndpoints valida que 404 é retornado quando os IDs não existem
func TestQuery_NotFoundEndpoints(t *testing.T) {
	ts, _, cleanup := setupTestServer(t)
	defer cleanup()

	randomUUID := uuid.New().String()

	endpoints := []string{
		fmt.Sprintf("%s/wallets/%s", ts.URL, randomUUID),
		fmt.Sprintf("%s/wagering/transactions/%s", ts.URL, randomUUID),
		fmt.Sprintf("%s/providers/provider-x/wagering/transactions/ext-non-existent", ts.URL),
	}

	for _, url := range endpoints {
		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("request to %s failed: %v", url, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("expected 404 for %s, got %d", url, resp.StatusCode)
		}
	}
}

// TestQuery_InvalidCursor valida que cursor inválido ou malformado retorna 400
func TestQuery_InvalidCursor(t *testing.T) {
	ts, svc, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()
	playerID := fmt.Sprintf("player-cur-%s", uuid.New().String()[:8])
	wallet, err := svc.OpenWallet(ctx, playerID, "BRL", 1000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	// 1. Base64 inválido
	resp1, err := http.Get(fmt.Sprintf("%s/wallets/%s/ledger?cursor=not-valid-base64!", ts.URL, wallet.ID.String()))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp1.Body.Close()

	if resp1.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid base64 cursor, got %d", resp1.StatusCode)
	}

	// 2. Base64 válido mas com formato incorreto (sem o delimiter '#')
	invalidFormatCursor := "aGVsbG8gd29ybGQ=" // "hello world" em base64
	resp2, err := http.Get(fmt.Sprintf("%s/wallets/%s/ledger?cursor=%s", ts.URL, wallet.ID.String(), invalidFormatCursor))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for bad cursor format, got %d", resp2.StatusCode)
	}
}
