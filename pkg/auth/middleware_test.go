package auth_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/moaaskt/jungle-test-challenge/pkg/auth"
)

func TestAuthMiddleware_Unit(t *testing.T) {
	mockVal := auth.NewTestTokenValidator()
	mockVal.Tokens["valid-internal"] = &auth.AuthContext{ClientID: "internal-service", IsInternal: true}
	mockVal.Tokens["valid-provider"] = &auth.AuthContext{ClientID: "provider-a", ProviderID: "provider-a", IsInternal: false}

	middleware := auth.AuthMiddleware(mockVal, true)

	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authCtx, ok := auth.GetAuthContext(r.Context())
		if !ok {
			t.Errorf("expected auth context in request")
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(authCtx.ClientID))
	}))

	// 1. Health check bypass
	reqHealth := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	recHealth := httptest.NewRecorder()
	middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(recHealth, reqHealth)
	if recHealth.Code != http.StatusOK {
		t.Errorf("expected 200 for /health/live without token, got %d", recHealth.Code)
	}

	// 2. Missing authorization header
	reqNoAuth := httptest.NewRequest(http.MethodPost, "/wagering/transactions", nil)
	recNoAuth := httptest.NewRecorder()
	handler.ServeHTTP(recNoAuth, reqNoAuth)
	if recNoAuth.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for missing auth header, got %d", recNoAuth.Code)
	}

	// 3. Invalid token
	reqInvalid := httptest.NewRequest(http.MethodPost, "/wagering/transactions", nil)
	reqInvalid.Header.Set("Authorization", "Bearer invalid-token")
	recInvalid := httptest.NewRecorder()
	handler.ServeHTTP(recInvalid, reqInvalid)
	if recInvalid.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for invalid token, got %d", recInvalid.Code)
	}

	// 4. Expired token
	reqExpired := httptest.NewRequest(http.MethodPost, "/wagering/transactions", nil)
	reqExpired.Header.Set("Authorization", "Bearer expired")
	recExpired := httptest.NewRecorder()
	handler.ServeHTTP(recExpired, reqExpired)
	if recExpired.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for expired token, got %d", recExpired.Code)
	}

	// 5. Valid internal token
	reqValidInt := httptest.NewRequest(http.MethodPost, "/wallets", nil)
	reqValidInt.Header.Set("Authorization", "Bearer valid-internal")
	recValidInt := httptest.NewRecorder()
	handler.ServeHTTP(recValidInt, reqValidInt)
	if recValidInt.Code != http.StatusOK {
		t.Errorf("expected 200 for valid internal token, got %d", recValidInt.Code)
	}
	if recValidInt.Body.String() != "internal-service" {
		t.Errorf("expected body 'internal-service', got %s", recValidInt.Body.String())
	}
}

func TestRequireInternal_Unit(t *testing.T) {
	internalHandler := auth.RequireInternal(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// 1. Without auth context -> 403 Forbidden
	reqNoCtx := httptest.NewRequest(http.MethodPost, "/wallets", nil)
	recNoCtx := httptest.NewRecorder()
	internalHandler(recNoCtx, reqNoCtx)
	if recNoCtx.Code != http.StatusForbidden {
		t.Errorf("expected 403 for missing auth context, got %d", recNoCtx.Code)
	}

	// 2. Provider auth context -> 403 Forbidden
	reqProv := httptest.NewRequest(http.MethodPost, "/wallets", nil)
	ctxProv := auth.WithAuthContext(context.Background(), &auth.AuthContext{ClientID: "provider-a", ProviderID: "provider-a", IsInternal: false})
	reqProv = reqProv.WithContext(ctxProv)
	recProv := httptest.NewRecorder()
	internalHandler(recProv, reqProv)
	if recProv.Code != http.StatusForbidden {
		t.Errorf("expected 403 for provider accessing internal route, got %d", recProv.Code)
	}

	// 3. Internal auth context -> 200 OK
	reqInt := httptest.NewRequest(http.MethodPost, "/wallets", nil)
	ctxInt := auth.WithAuthContext(context.Background(), &auth.AuthContext{ClientID: "internal-service", IsInternal: true})
	reqInt = reqInt.WithContext(ctxInt)
	recInt := httptest.NewRecorder()
	internalHandler(recInt, reqInt)
	if recInt.Code != http.StatusOK {
		t.Errorf("expected 200 for internal access, got %d", recInt.Code)
	}
}
