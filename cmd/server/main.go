package main

import (
	"log"
	"log/slog"
	"net/http"

	"github.com/moaaskt/jungle-test-challenge/pkg/api"
	"github.com/moaaskt/jungle-test-challenge/pkg/auth"
	"github.com/moaaskt/jungle-test-challenge/pkg/config"
	"github.com/moaaskt/jungle-test-challenge/pkg/database"
	"github.com/moaaskt/jungle-test-challenge/pkg/messaging"
	"github.com/moaaskt/jungle-test-challenge/pkg/repository"
	"github.com/moaaskt/jungle-test-challenge/pkg/service"
	"go.uber.org/fx"
)

func main() {
	// A função config.MustLoad garantirá o fail-fast se as variáveis estiverem ausentes.
	cfg := config.MustLoad()

	app := fx.New(
		fx.Supply(cfg),
		fx.Provide(slog.Default),
		auth.Module,
		database.Module,
		repository.Module,
		messaging.Module,
		service.Module,
		api.Module,
		fx.Invoke(func(*http.Server) {
			log.Println("Application wired successfully")
		}),
	)

	app.Run()
}
