package test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/moaaskt/jungle-test-challenge/pkg/api"
	"github.com/moaaskt/jungle-test-challenge/pkg/auth"
	"github.com/moaaskt/jungle-test-challenge/pkg/config"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
	"github.com/moaaskt/jungle-test-challenge/pkg/repository"
	"github.com/moaaskt/jungle-test-challenge/pkg/service"
)

// getKeycloakToken obtém um token JWT real via client_credentials contra o container real do Keycloak.
func getKeycloakToken(t *testing.T, clientID, clientSecret string) string {
	t.Helper()
	keycloakURL := os.Getenv("KEYCLOAK_URL")
	if keycloakURL == "" {
		keycloakURL = "http://localhost:8085"
	}
	realm := os.Getenv("KEYCLOAK_REALM")
	if realm == "" {
		realm = "jungle"
	}

	tokenURL := fmt.Sprintf("%s/realms/%s/protocol/openid-connect/token", keycloakURL, realm)
	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_id", clientID)
	data.Set("client_secret", clientSecret)

	req, err := http.NewRequest(http.MethodPost, tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		t.Fatalf("failed to create token request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to request token from Keycloak: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("failed to obtain token (status %d): %s", resp.StatusCode, string(body))
	}

	var res struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatalf("failed to decode token response: %v", err)
	}
	return res.AccessToken
}

func setupAuthTestServer(t *testing.T) (*httptest.Server, service.WagerService, func()) {
	t.Helper()
	pool := getTestPool(t)

	keycloakURL := os.Getenv("KEYCLOAK_URL")
	if keycloakURL == "" {
		keycloakURL = "http://localhost:8085"
	}
	realm := os.Getenv("KEYCLOAK_REALM")
	if realm == "" {
		realm = "jungle"
	}

	cfg := &config.Config{
		Port:          "8080",
		KeycloakURL:   keycloakURL,
		KeycloakRealm: realm,
		AuthEnabled:   true,
	}

	validator, err := auth.NewJWKSTokenValidator(cfg)
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
	handlers := api.NewHandlers(svc)
	mux := api.NewMux(handlers)

	rootHandler := auth.AuthMiddleware(validator, true)(mux)
	ts := httptest.NewServer(rootHandler)

	cleanup := func() {
		ts.Close()
		pool.Close()
	}

	return ts, svc, cleanup
}

// 1. TestAuth_MissingToken: Requisições sem token retornam 401 Unauthorized
func TestAuth_MissingToken(t *testing.T) {
	ts, _, cleanup := setupAuthTestServer(t)
	defer cleanup()

	// POST /wallets
	respWallets, err := http.Post(ts.URL+"/wallets", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer respWallets.Body.Close()
	if respWallets.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 for /wallets without token, got %d", respWallets.StatusCode)
	}

	// POST /wagering/transactions
	respWager, err := http.Post(ts.URL+"/wagering/transactions", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer respWager.Body.Close()
	if respWager.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 for /wagering/transactions without token, got %d", respWager.StatusCode)
	}
}

// 2. TestAuth_InvalidToken: Token malformado ou header inválido retorna 401 Unauthorized
func TestAuth_InvalidToken(t *testing.T) {
	ts, _, cleanup := setupAuthTestServer(t)
	defer cleanup()

	// Header Bearer com string corrompida
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/wallets", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer not-a-valid-jwt-token")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 for invalid JWT, got %d", resp.StatusCode)
	}

	// Header que não começa com Bearer (ex: Basic)
	reqBasic, _ := http.NewRequest(http.MethodPost, ts.URL+"/wallets", strings.NewReader(`{}`))
	reqBasic.Header.Set("Authorization", "Basic dXNlcjpwYXNzd29yZA==")
	reqBasic.Header.Set("Content-Type", "application/json")

	respBasic, err := http.DefaultClient.Do(reqBasic)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer respBasic.Body.Close()
	if respBasic.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 for non-Bearer auth header, got %d", respBasic.StatusCode)
	}
}

// 3. TestAuth_ExpiredToken: Token com expiração vencida retorna 401 Unauthorized
func TestAuth_ExpiredToken(t *testing.T) {
	ts, _, cleanup := setupAuthTestServer(t)
	defer cleanup()

	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate rsa key: %v", err)
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss": "http://localhost:8085/realms/jungle",
		"sub": "some-subject",
		"exp": time.Now().Add(-1 * time.Hour).Unix(),
		"azp": "provider-a",
	})
	token.Header["kid"] = "some-key-id"

	tokenString, err := token.SignedString(privKey)
	if err != nil {
		t.Fatalf("failed to sign expired token: %v", err)
	}

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/wagering/transactions", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer "+tokenString)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 for expired token, got %d", resp.StatusCode)
	}
}

// 4. TestAuth_InternalService_Wallets: internal-service cria carteira e consulta ledger com sucesso (201 / 200)
func TestAuth_InternalService_Wallets(t *testing.T) {
	ts, _, cleanup := setupAuthTestServer(t)
	defer cleanup()

	token := getKeycloakToken(t, "internal-service", "internal-service-secret")

	// Criar Carteira (POST /wallets)
	playerID := fmt.Sprintf("player-internal-%s", uuid.New().String()[:8])
	payload := map[string]any{
		"playerId": playerID,
		"initialBalance": map[string]string{
			"amount":   "100.00",
			"currency": "BRL",
		},
	}
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/wallets", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 201 Created for internal-service, got %d: %s", resp.StatusCode, string(respBody))
	}

	var walletResp struct {
		ID string `json:"id"`
	}
	json.NewDecoder(resp.Body).Decode(&walletResp)

	// Consultar Carteira (GET /wallets/{walletId})
	reqGet, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/wallets/%s", ts.URL, walletResp.ID), nil)
	reqGet.Header.Set("Authorization", "Bearer "+token)

	respGet, err := http.DefaultClient.Do(reqGet)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer respGet.Body.Close()
	if respGet.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for GET /wallets/{id}, got %d", respGet.StatusCode)
	}

	// Consultar Ledger (GET /wallets/{walletId}/ledger)
	reqLedger, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/wallets/%s/ledger", ts.URL, walletResp.ID), nil)
	reqLedger.Header.Set("Authorization", "Bearer "+token)

	respLedger, err := http.DefaultClient.Do(reqLedger)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer respLedger.Body.Close()
	if respLedger.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for GET /wallets/{id}/ledger, got %d", respLedger.StatusCode)
	}
}

// 5. TestAuth_Provider_WalletForbidden: provider-a tentando acessar rotas de carteira recebe 403 Forbidden
func TestAuth_Provider_WalletForbidden(t *testing.T) {
	ts, svc, cleanup := setupAuthTestServer(t)
	defer cleanup()

	token := getKeycloakToken(t, "provider-a", "provider-a-secret")

	// Criar previamente uma carteira para teste de consulta
	ctx := context.Background()
	playerID := fmt.Sprintf("player-prov-block-%s", uuid.New().String()[:8])
	w, err := svc.OpenWallet(ctx, playerID, "BRL", 5000)
	if err != nil {
		t.Fatalf("failed to open test wallet: %v", err)
	}

	// Tentativa de POST /wallets
	reqPost, _ := http.NewRequest(http.MethodPost, ts.URL+"/wallets", strings.NewReader(`{"playerId":"test"}`))
	reqPost.Header.Set("Authorization", "Bearer "+token)
	reqPost.Header.Set("Content-Type", "application/json")

	respPost, err := http.DefaultClient.Do(reqPost)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer respPost.Body.Close()
	if respPost.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden for provider on POST /wallets, got %d", respPost.StatusCode)
	}

	// Tentativa de GET /wallets/{walletId}
	reqGet, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/wallets/%s", ts.URL, w.ID), nil)
	reqGet.Header.Set("Authorization", "Bearer "+token)

	respGet, err := http.DefaultClient.Do(reqGet)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer respGet.Body.Close()
	if respGet.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden for provider on GET /wallets/{id}, got %d", respGet.StatusCode)
	}

	// Tentativa de GET /wallets/{walletId}/ledger
	reqLedger, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/wallets/%s/ledger", ts.URL, w.ID), nil)
	reqLedger.Header.Set("Authorization", "Bearer "+token)

	respLedger, err := http.DefaultClient.Do(reqLedger)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer respLedger.Body.Close()
	if respLedger.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden for provider on GET /wallets/{id}/ledger, got %d", respLedger.StatusCode)
	}
}

// 6. TestAuth_Provider_OwnWagering: provider-a submetendo aposta para provider-a obtém 200 OK
func TestAuth_Provider_OwnWagering(t *testing.T) {
	ts, svc, cleanup := setupAuthTestServer(t)
	defer cleanup()

	ctx := context.Background()
	playerID := fmt.Sprintf("player-wager-auth-%s", uuid.New().String()[:8])
	w, err := svc.OpenWallet(ctx, playerID, "BRL", 50000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	token := getKeycloakToken(t, "provider-a", "provider-a-secret")

	extID := fmt.Sprintf("ext-auth-own-%s", uuid.New().String()[:8])
	idemKey := fmt.Sprintf("idem-auth-own-%s", uuid.New().String()[:8])

	payload := map[string]any{
		"providerId":            "provider-a",
		"externalTransactionId": extID,
		"playerId":              playerID,
		"walletId":              w.ID.String(),
		"roundId":               "round-1",
		"gameId":                "game-1",
		"kind":                  "BET",
		"money": map[string]string{
			"amount":   "50.00",
			"currency": "BRL",
		},
	}
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/wagering/transactions", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Idempotency-Key", idemKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 OK for own wagering, got %d: %s", resp.StatusCode, string(respBody))
	}
}

// 7. TestAuth_Provider_CrossProviderWageringForbidden: provider-a tentando submeter aposta como provider-b recebe 403 Forbidden
func TestAuth_Provider_CrossProviderWageringForbidden(t *testing.T) {
	ts, svc, cleanup := setupAuthTestServer(t)
	defer cleanup()

	ctx := context.Background()
	playerID := fmt.Sprintf("player-cross-wager-%s", uuid.New().String()[:8])
	w, err := svc.OpenWallet(ctx, playerID, "BRL", 50000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	token := getKeycloakToken(t, "provider-a", "provider-a-secret")

	extID := fmt.Sprintf("ext-cross-prov-%s", uuid.New().String()[:8])
	idemKey := fmt.Sprintf("idem-cross-prov-%s", uuid.New().String()[:8])

	// Submete com providerId: "provider-b" usando credenciais de provider-a
	payload := map[string]any{
		"providerId":            "provider-b",
		"externalTransactionId": extID,
		"playerId":              playerID,
		"walletId":              w.ID.String(),
		"roundId":               "round-1",
		"gameId":                "game-1",
		"kind":                  "BET",
		"money": map[string]string{
			"amount":   "50.00",
			"currency": "BRL",
		},
	}
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/wagering/transactions", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Idempotency-Key", idemKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden for cross-provider submission, got %d", resp.StatusCode)
	}
}

// 8. TestAuth_CrossProvider_IdempotencyReplayForbidden:
// Se provider-b tentar reutilizar Idempotency-Key de aposta originada por provider-a, recebe 403 Forbidden
func TestAuth_CrossProvider_IdempotencyReplayForbidden(t *testing.T) {
	ts, svc, cleanup := setupAuthTestServer(t)
	defer cleanup()

	ctx := context.Background()
	playerID := fmt.Sprintf("player-cross-replay-%s", uuid.New().String()[:8])
	w, err := svc.OpenWallet(ctx, playerID, "BRL", 50000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	tokenA := getKeycloakToken(t, "provider-a", "provider-a-secret")
	tokenB := getKeycloakToken(t, "provider-b", "provider-b-secret")

	sharedIdemKey := fmt.Sprintf("idem-replay-key-%s", uuid.New().String()[:8])
	extIDA := fmt.Sprintf("ext-replay-a-%s", uuid.New().String()[:8])

	// 1. provider-a submete com sharedIdemKey -> 200 OK
	payloadA := map[string]any{
		"providerId":            "provider-a",
		"externalTransactionId": extIDA,
		"playerId":              playerID,
		"walletId":              w.ID.String(),
		"roundId":               "round-1",
		"gameId":                "game-1",
		"kind":                  "BET",
		"money": map[string]string{
			"amount":   "50.00",
			"currency": "BRL",
		},
	}
	bodyA, _ := json.Marshal(payloadA)

	reqA, _ := http.NewRequest(http.MethodPost, ts.URL+"/wagering/transactions", bytes.NewReader(bodyA))
	reqA.Header.Set("Authorization", "Bearer "+tokenA)
	reqA.Header.Set("Idempotency-Key", sharedIdemKey)
	reqA.Header.Set("Content-Type", "application/json")

	respA, err := http.DefaultClient.Do(reqA)
	if err != nil {
		t.Fatalf("request A failed: %v", err)
	}
	defer respA.Body.Close()
	if respA.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(respA.Body)
		t.Fatalf("expected 200 OK for provider-a, got %d: %s", respA.StatusCode, string(respBody))
	}

	// 2. provider-b tenta submeter requisição com o mesmo Idempotency-Key -> deve rejeitar com 403 Forbidden
	extIDB := fmt.Sprintf("ext-replay-b-%s", uuid.New().String()[:8])
	payloadB := map[string]any{
		"providerId":            "provider-b",
		"externalTransactionId": extIDB,
		"playerId":              playerID,
		"walletId":              w.ID.String(),
		"roundId":               "round-1",
		"gameId":                "game-1",
		"kind":                  "BET",
		"money": map[string]string{
			"amount":   "50.00",
			"currency": "BRL",
		},
	}
	bodyB, _ := json.Marshal(payloadB)

	reqB, _ := http.NewRequest(http.MethodPost, ts.URL+"/wagering/transactions", bytes.NewReader(bodyB))
	reqB.Header.Set("Authorization", "Bearer "+tokenB)
	reqB.Header.Set("Idempotency-Key", sharedIdemKey) // Reutilizando a chave de provider-a!
	reqB.Header.Set("Content-Type", "application/json")

	respB, err := http.DefaultClient.Do(reqB)
	if err != nil {
		t.Fatalf("request B failed: %v", err)
	}
	defer respB.Body.Close()

	if respB.StatusCode != http.StatusForbidden {
		body, _ := io.ReadAll(respB.Body)
		t.Errorf("expected 403 Forbidden on cross-provider idempotency replay, got %d: %s", respB.StatusCode, string(body))
	}
}

// 9. TestAuth_Provider_ExternalQueryIsolation: GET /providers/:providerId/... com isolamento
func TestAuth_Provider_ExternalQueryIsolation(t *testing.T) {
	ts, svc, cleanup := setupAuthTestServer(t)
	defer cleanup()

	ctx := context.Background()
	playerID := fmt.Sprintf("player-query-iso-%s", uuid.New().String()[:8])
	w, err := svc.OpenWallet(ctx, playerID, "BRL", 50000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	extIDA := fmt.Sprintf("ext-tx-a-%s", uuid.New().String()[:8])
	providerIDA := "provider-a"

	// Cria aposta via service para provider-a
	amt, _ := domain.NewWagerTransaction(domain.OriginExternal, w.ID, playerID, &providerIDA, &extIDA, domain.TransactionTypeBet, w.Balance(), "BRL")
	amt.Process()
	_ = svc

	tokenA := getKeycloakToken(t, "provider-a", "provider-a-secret")

	// Criar transação via API para ter dados reais
	idemKey := fmt.Sprintf("idem-iso-%s", uuid.New().String()[:8])
	payloadA := map[string]any{
		"providerId":            "provider-a",
		"externalTransactionId": extIDA,
		"playerId":              playerID,
		"walletId":              w.ID.String(),
		"roundId":               "round-1",
		"gameId":                "game-1",
		"kind":                  "BET",
		"money": map[string]string{
			"amount":   "10.00",
			"currency": "BRL",
		},
	}
	bodyA, _ := json.Marshal(payloadA)

	reqPost, _ := http.NewRequest(http.MethodPost, ts.URL+"/wagering/transactions", bytes.NewReader(bodyA))
	reqPost.Header.Set("Authorization", "Bearer "+tokenA)
	reqPost.Header.Set("Idempotency-Key", idemKey)
	reqPost.Header.Set("Content-Type", "application/json")
	respPost, _ := http.DefaultClient.Do(reqPost)
	respPost.Body.Close()

	// 1. provider-a consultando sua própria transação -> 200 OK
	reqOwn, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/providers/provider-a/wagering/transactions/%s", ts.URL, extIDA), nil)
	reqOwn.Header.Set("Authorization", "Bearer "+tokenA)

	respOwn, err := http.DefaultClient.Do(reqOwn)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer respOwn.Body.Close()
	if respOwn.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for own transaction query, got %d", respOwn.StatusCode)
	}

	// 2. provider-a tentando consultar transação sob o path de provider-b -> 403 Forbidden
	reqCross, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/providers/provider-b/wagering/transactions/%s", ts.URL, extIDA), nil)
	reqCross.Header.Set("Authorization", "Bearer "+tokenA)

	respCross, err := http.DefaultClient.Do(reqCross)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer respCross.Body.Close()
	if respCross.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden for cross provider path query, got %d", respCross.StatusCode)
	}
}

// 10. TestAuth_Provider_UUIDQueryIsolation: GET /wagering/transactions/:id com isolamento por provedor
func TestAuth_Provider_UUIDQueryIsolation(t *testing.T) {
	ts, svc, cleanup := setupAuthTestServer(t)
	defer cleanup()

	ctx := context.Background()
	playerID := fmt.Sprintf("player-uuid-iso-%s", uuid.New().String()[:8])
	w, err := svc.OpenWallet(ctx, playerID, "BRL", 50000)
	if err != nil {
		t.Fatalf("failed to open wallet: %v", err)
	}

	tokenB := getKeycloakToken(t, "provider-b", "provider-b-secret")
	tokenA := getKeycloakToken(t, "provider-a", "provider-a-secret")
	tokenInt := getKeycloakToken(t, "internal-service", "internal-service-secret")

	// Criar aposta para provider-b
	extIDB := fmt.Sprintf("ext-uuid-b-%s", uuid.New().String()[:8])
	idemKeyB := fmt.Sprintf("idem-uuid-b-%s", uuid.New().String()[:8])
	payloadB := map[string]any{
		"providerId":            "provider-b",
		"externalTransactionId": extIDB,
		"playerId":              playerID,
		"walletId":              w.ID.String(),
		"roundId":               "round-1",
		"gameId":                "game-1",
		"kind":                  "BET",
		"money": map[string]string{
			"amount":   "10.00",
			"currency": "BRL",
		},
	}
	bodyB, _ := json.Marshal(payloadB)

	reqB, _ := http.NewRequest(http.MethodPost, ts.URL+"/wagering/transactions", bytes.NewReader(bodyB))
	reqB.Header.Set("Authorization", "Bearer "+tokenB)
	reqB.Header.Set("Idempotency-Key", idemKeyB)
	reqB.Header.Set("Content-Type", "application/json")

	respB, _ := http.DefaultClient.Do(reqB)
	var txResp struct {
		TransactionID string `json:"transactionId"`
	}
	json.NewDecoder(respB.Body).Decode(&txResp)
	respB.Body.Close()

	if txResp.TransactionID == "" {
		t.Fatalf("failed to create transaction for provider-b")
	}

	// 1. provider-a tentando inspecionar transação de provider-b por UUID -> 403 Forbidden
	reqProvA, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/wagering/transactions/%s", ts.URL, txResp.TransactionID), nil)
	reqProvA.Header.Set("Authorization", "Bearer "+tokenA)

	respProvA, err := http.DefaultClient.Do(reqProvA)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer respProvA.Body.Close()
	if respProvA.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden when provider-a inspects provider-b transaction, got %d", respProvA.StatusCode)
	}

	// 2. internal-service inspecionando a mesma transação -> 200 OK
	reqInt, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/wagering/transactions/%s", ts.URL, txResp.TransactionID), nil)
	reqInt.Header.Set("Authorization", "Bearer "+tokenInt)

	respInt, err := http.DefaultClient.Do(reqInt)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer respInt.Body.Close()
	if respInt.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK when internal-service inspects transaction, got %d", respInt.StatusCode)
	}
}

// 11. TestAuth_PublicHealthEndpoints: /health/live e /health/ready acessíveis sem token -> 200 OK
func TestAuth_PublicHealthEndpoints(t *testing.T) {
	ts, _, cleanup := setupAuthTestServer(t)
	defer cleanup()

	respLive, err := http.Get(ts.URL + "/health/live")
	if err != nil {
		t.Fatalf("failed to call /health/live: %v", err)
	}
	defer respLive.Body.Close()
	if respLive.StatusCode != http.StatusOK {
		t.Errorf("expected 200 for /health/live without token, got %d", respLive.StatusCode)
	}

	respReady, err := http.Get(ts.URL + "/health/ready")
	if err != nil {
		t.Fatalf("failed to call /health/ready: %v", err)
	}
	defer respReady.Body.Close()
	if respReady.StatusCode != http.StatusOK {
		t.Errorf("expected 200 for /health/ready without token, got %d", respReady.StatusCode)
	}
}
