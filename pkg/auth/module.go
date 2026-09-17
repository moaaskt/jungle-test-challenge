package auth

import (
	"github.com/moaaskt/jungle-test-challenge/pkg/config"
	"go.uber.org/fx"
)

var Module = fx.Provide(
	ProvideTokenValidator,
)

// ProvideTokenValidator instancia o JWKSTokenValidator como implementação padrão de TokenValidator.
func ProvideTokenValidator(cfg *config.Config) (TokenValidator, error) {
	return NewJWKSTokenValidator(cfg)
}
