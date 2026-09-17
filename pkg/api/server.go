package api

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/moaaskt/jungle-test-challenge/pkg/auth"
	"github.com/moaaskt/jungle-test-challenge/pkg/config"
	"go.uber.org/fx"
)

var Module = fx.Provide(
	NewHandlers,
	NewServer,
)

func NewMux(handlers *Handlers) *http.ServeMux {
	mux := http.NewServeMux()

	// Endpoints de Carteira: Restritos exclusivamente ao serviço interno (Seção 2)
	mux.HandleFunc("POST /wallets", auth.RequireInternal(handlers.HandleOpenWallet))
	mux.HandleFunc("GET /wallets/{walletId}", auth.RequireInternal(handlers.HandleGetWallet))
	mux.HandleFunc("GET /wallets/{walletId}/ledger", auth.RequireInternal(handlers.HandleGetLedger))

	// Endpoints de Wagering (autenticados, com controle por provedor nos handlers)
	mux.HandleFunc("POST /wagering/transactions", handlers.HandleWagerTransaction)
	mux.HandleFunc("GET /wagering/transactions/{transactionId}", handlers.HandleGetTransaction)
	mux.HandleFunc("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", handlers.HandleGetTransactionByExternal)

	// Endpoints de Saúde (Públicos)
	mux.HandleFunc("GET /health/live", handlers.HandleLiveness)
	mux.HandleFunc("GET /health/ready", handlers.HandleReadiness)

	return mux
}

func NewServer(lc fx.Lifecycle, cfg *config.Config, handlers *Handlers, validator auth.TokenValidator) *http.Server {
	mux := NewMux(handlers)

	var rootHandler http.Handler = mux
	if validator != nil {
		rootHandler = auth.AuthMiddleware(validator, cfg.AuthEnabled)(mux)
	}

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%s", cfg.Port),
		Handler: rootHandler,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			log.Printf("Starting HTTP server on %s", srv.Addr)
			go func() {
				if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					log.Fatalf("listen: %s\n", err)
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			log.Println("Stopping HTTP server gracefully...")
			return srv.Shutdown(ctx)
		},
	})

	return srv
}
