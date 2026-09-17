package auth

import "context"

type authContextKey struct{}

// AuthContext encapsula os dados de identidade autenticados a partir do token JWT.
type AuthContext struct {
	ClientID   string
	ProviderID string
	IsInternal bool
}

// WithAuthContext injeta o AuthContext no contexto da requisição.
func WithAuthContext(ctx context.Context, authCtx *AuthContext) context.Context {
	return context.WithValue(ctx, authContextKey{}, authCtx)
}

// GetAuthContext recupera o AuthContext do contexto da requisição, se presente.
func GetAuthContext(ctx context.Context) (*AuthContext, bool) {
	val := ctx.Value(authContextKey{})
	if val == nil {
		return nil, false
	}
	authCtx, ok := val.(*AuthContext)
	return authCtx, ok
}
