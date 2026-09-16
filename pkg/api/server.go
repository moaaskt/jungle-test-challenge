package api

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/moaaskt/jungle-test-challenge/pkg/config"
	"go.uber.org/fx"
)

var Module = fx.Provide(
	NewHandlers,
	NewServer,
)

func NewServer(lc fx.Lifecycle, cfg *config.Config, handlers *Handlers) *http.Server {
	mux := http.NewServeMux()

	// Endpoints exigidos pelo desafio
	mux.HandleFunc("POST /wallets", handlers.HandleOpenWallet)
	mux.HandleFunc("POST /wagering/transactions", handlers.HandleWagerTransaction)
	mux.HandleFunc("GET /health/live", handlers.HandleLiveness)
	mux.HandleFunc("GET /health/ready", handlers.HandleReadiness)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%s", cfg.Port),
		Handler: mux,
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
