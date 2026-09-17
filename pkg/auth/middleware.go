package auth

import (
	"encoding/json"
	"net/http"
	"strings"
)

// AuthMiddleware intercepta requisições HTTP, valida o token JWT do header Authorization e injeta o AuthContext.
func AuthMiddleware(validator TokenValidator, authEnabled bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Bypass para endpoints públicos de saúde
			if r.URL.Path == "/health/live" || r.URL.Path == "/health/ready" {
				next.ServeHTTP(w, r)
				return
			}

			if !authEnabled {
				// Se autenticação estiver desabilitada por configuração, assume internal
				ctx := WithAuthContext(r.Context(), &AuthContext{
					ClientID:   "internal-service",
					IsInternal: true,
				})
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}

			authHeader := r.Header.Get("Authorization")
			if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
				respondJSONError(w, http.StatusUnauthorized, "unauthorized: missing or invalid authorization header")
				return
			}

			tokenString := strings.TrimPrefix(authHeader, "Bearer ")
			tokenString = strings.TrimSpace(tokenString)
			if tokenString == "" {
				respondJSONError(w, http.StatusUnauthorized, "unauthorized: missing or invalid authorization header")
				return
			}

			authCtx, err := validator.ValidateToken(r.Context(), tokenString)
			if err != nil {
				respondJSONError(w, http.StatusUnauthorized, err.Error())
				return
			}

			ctx := WithAuthContext(r.Context(), authCtx)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireInternal garante que a rota seja acessada apenas por clientes com privilégio interno (internal-service).
func RequireInternal(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authCtx, ok := GetAuthContext(r.Context())
		if !ok || !authCtx.IsInternal {
			respondJSONError(w, http.StatusForbidden, "forbidden: wallet operations are restricted to internal service")
			return
		}
		next(w, r)
	}
}

func respondJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}
