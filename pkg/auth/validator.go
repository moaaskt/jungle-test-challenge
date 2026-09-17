package auth

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/moaaskt/jungle-test-challenge/pkg/config"
)

var (
	ErrInvalidToken   = errors.New("unauthorized: invalid token")
	ErrExpiredToken   = errors.New("unauthorized: token has expired")
	ErrUnknownKeyID   = errors.New("unauthorized: unknown key id")
	ErrInvalidIssuer  = errors.New("unauthorized: invalid token issuer")
	ErrMissingSubject = errors.New("unauthorized: missing client identifier in token")
)

// TokenValidator define a interface para validação e extração de identidade de tokens JWT.
type TokenValidator interface {
	ValidateToken(ctx context.Context, tokenString string) (*AuthContext, error)
}

// JWKItem representa uma chave pública no formato JWK retornado pelo IdP.
type JWKItem struct {
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	N   string `json:"n"`
	E   string `json:"e"`
}

// JWKSet representa o conjunto de chaves públicas retornado pelo endpoint JWKS.
type JWKSet struct {
	Keys []JWKItem `json:"keys"`
}

// KeycloakClaims representa os claims OIDC comumente gerados pelo Keycloak via client_credentials.
type KeycloakClaims struct {
	jwt.RegisteredClaims
	Azp      string `json:"azp,omitempty"`
	ClientID string `json:"client_id,omitempty"`
}

// JWKSTokenValidator implementa TokenValidator buscando chaves públicas via JWKS real.
type JWKSTokenValidator struct {
	keycloakURL string
	realm       string
	httpClient  *http.Client

	mu          sync.RWMutex
	keys        map[string]*rsa.PublicKey
	lastRefresh time.Time
}

// NewJWKSTokenValidator cria um novo validador com suporte a cache e refresh dinâmico de JWKS.
func NewJWKSTokenValidator(cfg *config.Config) (*JWKSTokenValidator, error) {
	validator := &JWKSTokenValidator{
		keycloakURL: strings.TrimRight(cfg.KeycloakURL, "/"),
		realm:       cfg.KeycloakRealm,
		httpClient:  &http.Client{Timeout: 5 * time.Second},
		keys:        make(map[string]*rsa.PublicKey),
	}
	return validator, nil
}

// ValidateToken decodifica o JWT, valida a assinatura com as chaves públicas JWKS e extrai o AuthContext.
func (v *JWKSTokenValidator) ValidateToken(ctx context.Context, tokenString string) (*AuthContext, error) {
	if tokenString == "" {
		return nil, ErrInvalidToken
	}

	claims := &KeycloakClaims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(t *jwt.Token) (any, error) {
		// Validar algoritmo RS256
		if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}

		kidRaw, ok := t.Header["kid"]
		if !ok {
			return nil, ErrUnknownKeyID
		}
		kid, ok := kidRaw.(string)
		if !ok || kid == "" {
			return nil, ErrUnknownKeyID
		}

		pubKey, err := v.getKey(ctx, kid)
		if err != nil {
			return nil, err
		}
		return pubKey, nil
	})

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrExpiredToken
		}
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	if !token.Valid {
		return nil, ErrInvalidToken
	}

	// Validar Emissor (iss) flexível para suportar URLs de host ou container
	expectedIssuerSuffix := fmt.Sprintf("/realms/%s", v.realm)
	if !strings.HasSuffix(claims.Issuer, expectedIssuerSuffix) {
		return nil, fmt.Errorf("%w: expected realm %s, got %s", ErrInvalidIssuer, v.realm, claims.Issuer)
	}

	// Extrair client_id do token (Keycloak coloca em azp ou client_id)
	clientID := claims.Azp
	if clientID == "" {
		clientID = claims.ClientID
	}
	if clientID == "" {
		clientID = claims.Subject
	}
	if clientID == "" {
		return nil, ErrMissingSubject
	}

	isInternal := clientID == "internal-service"
	var providerID string
	if !isInternal {
		providerID = clientID
	}

	return &AuthContext{
		ClientID:   clientID,
		ProviderID: providerID,
		IsInternal: isInternal,
	}, nil
}

// getKey recupera a chave pública do cache ou dispara um refresh se não encontrada.
func (v *JWKSTokenValidator) getKey(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	v.mu.RLock()
	key, exists := v.keys[kid]
	needsRefresh := !exists || time.Since(v.lastRefresh) > 1*time.Hour
	v.mu.RUnlock()

	if !needsRefresh && key != nil {
		return key, nil
	}

	// Atualizar chaves
	if err := v.refreshKeys(ctx); err != nil {
		// Se falhou o refresh mas já temos a chave em cache, podemos continuar
		if key != nil {
			return key, nil
		}
		return nil, fmt.Errorf("failed to refresh jwks keys: %w", err)
	}

	v.mu.RLock()
	defer v.mu.RUnlock()
	key, exists = v.keys[kid]
	if !exists || key == nil {
		return nil, fmt.Errorf("%w: %s", ErrUnknownKeyID, kid)
	}
	return key, nil
}

// refreshKeys busca o endpoint JWKS do Keycloak e atualiza o mapa de chaves RSA.
func (v *JWKSTokenValidator) refreshKeys(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	// Rate limit de refresh: no máximo 1 refresh a cada 2 segundos
	if time.Since(v.lastRefresh) < 2*time.Second && len(v.keys) > 0 {
		return nil
	}

	certsURL := fmt.Sprintf("%s/realms/%s/protocol/openid-connect/certs", v.keycloakURL, v.realm)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, certsURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request for jwks: %w", err)
	}

	resp, err := v.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to call jwks endpoint: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("jwks endpoint returned status %d", resp.StatusCode)
	}

	var jwks JWKSet
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		return fmt.Errorf("failed to decode jwks response: %w", err)
	}

	newKeys := make(map[string]*rsa.PublicKey)
	for _, k := range jwks.Keys {
		if k.Kty != "RSA" || k.Kid == "" {
			continue
		}
		pubKey, err := parseRSAPublicKey(k.N, k.E)
		if err != nil {
			continue
		}
		newKeys[k.Kid] = pubKey
	}

	v.keys = newKeys
	v.lastRefresh = time.Now()
	return nil
}

// parseRSAPublicKey converte os componentes base64url n e e em uma chave *rsa.PublicKey.
func parseRSAPublicKey(nStr, eStr string) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(nStr)
	if err != nil {
		return nil, fmt.Errorf("invalid modulus: %w", err)
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(eStr)
	if err != nil {
		return nil, fmt.Errorf("invalid exponent: %w", err)
	}

	n := new(big.Int).SetBytes(nBytes)
	e := int(new(big.Int).SetBytes(eBytes).Int64())

	return &rsa.PublicKey{
		N: n,
		E: e,
	}, nil
}

// TestTokenValidator é um validador determinístico restrito a testes unitários internos de pacote.
type TestTokenValidator struct {
	Tokens map[string]*AuthContext
}

func NewTestTokenValidator() *TestTokenValidator {
	return &TestTokenValidator{
		Tokens: make(map[string]*AuthContext),
	}
}

func (t *TestTokenValidator) ValidateToken(ctx context.Context, tokenString string) (*AuthContext, error) {
	if tokenString == "expired" {
		return nil, ErrExpiredToken
	}
	if authCtx, ok := t.Tokens[tokenString]; ok {
		return authCtx, nil
	}
	return nil, ErrInvalidToken
}
