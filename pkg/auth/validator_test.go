package auth_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/moaaskt/jungle-test-challenge/pkg/auth"
	"github.com/moaaskt/jungle-test-challenge/pkg/config"
)

func TestJWKSTokenValidator_RealKeycloak(t *testing.T) {
	cfg := &config.Config{
		KeycloakURL:   "http://localhost:8085",
		KeycloakRealm: "jungle",
		AuthEnabled:   true,
	}

	validator, err := auth.NewJWKSTokenValidator(cfg)
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	// 1. Obter token real via client_credentials para provider-a
	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_id", "provider-a")
	data.Set("client_secret", "provider-a-secret")

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		"http://localhost:8085/realms/jungle/protocol/openid-connect/token",
		strings.NewReader(data.Encode()))
	if err != nil {
		t.Fatalf("failed to create token request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to request token: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		t.Fatalf("failed to decode token response: %v", err)
	}

	// 2. Validar token real usando o validador JWKS
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	authCtx, err := validator.ValidateToken(ctx, tokenResp.AccessToken)
	if err != nil {
		t.Fatalf("expected valid token, got error: %v", err)
	}

	if authCtx.ClientID != "provider-a" {
		t.Errorf("expected ClientID provider-a, got %s", authCtx.ClientID)
	}
	if authCtx.ProviderID != "provider-a" {
		t.Errorf("expected ProviderID provider-a, got %s", authCtx.ProviderID)
	}
	if authCtx.IsInternal {
		t.Errorf("expected IsInternal false for provider-a")
	}

	// 3. Testar token para internal-service
	dataInternal := url.Values{}
	dataInternal.Set("grant_type", "client_credentials")
	dataInternal.Set("client_id", "internal-service")
	dataInternal.Set("client_secret", "internal-service-secret")

	reqInt, _ := http.NewRequestWithContext(context.Background(), http.MethodPost,
		"http://localhost:8085/realms/jungle/protocol/openid-connect/token",
		strings.NewReader(dataInternal.Encode()))
	reqInt.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	respInt, err := http.DefaultClient.Do(reqInt)
	if err != nil {
		t.Fatalf("failed to request internal token: %v", err)
	}
	defer respInt.Body.Close()

	var tokenRespInt struct {
		AccessToken string `json:"access_token"`
	}
	json.NewDecoder(respInt.Body).Decode(&tokenRespInt)

	authCtxInt, err := validator.ValidateToken(ctx, tokenRespInt.AccessToken)
	if err != nil {
		t.Fatalf("expected valid internal token, got error: %v", err)
	}

	if !authCtxInt.IsInternal {
		t.Errorf("expected IsInternal true for internal-service")
	}
}
