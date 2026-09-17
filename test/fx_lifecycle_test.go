package test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/moaaskt/jungle-test-challenge/pkg/api"
	"github.com/moaaskt/jungle-test-challenge/pkg/auth"
	"github.com/moaaskt/jungle-test-challenge/pkg/config"
	"github.com/moaaskt/jungle-test-challenge/pkg/database"
	"github.com/moaaskt/jungle-test-challenge/pkg/messaging"
	"github.com/moaaskt/jungle-test-challenge/pkg/repository"
	"github.com/moaaskt/jungle-test-challenge/pkg/service"
	"go.uber.org/fx"
)

// getFreePort reserva uma porta TCP livre do SO e a libera imediatamente para uso no teste,
// evitando colisões com a porta 8080 do Docker Compose ou de outros serviços.
func getFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to obtain free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

// TestFx_Composition_StartAndStop atende à exigência da Seção 13 do Edital:
// "Adicione uma verificação da composição Fx e de seu início e encerramento, incluindo liberação de recursos dos workers."
func TestFx_Composition_StartAndStop(t *testing.T) {
	port := getFreePort(t)
	baseCfg := getTestSQSConfig()

	cfg := &config.Config{
		DatabaseURL:           baseCfg.DatabaseURL,
		Port:                  fmt.Sprintf("%d", port),
		AWSEndpoint:           baseCfg.AWSEndpoint,
		AWSRegion:             baseCfg.AWSRegion,
		SQSWagerRequestsQueue: fmt.Sprintf("test-fx-req-%d.fifo", time.Now().UnixNano()),
		SQSWagerEventsQueue:   fmt.Sprintf("test-fx-ev-%d.fifo", time.Now().UnixNano()),
		SQSDLQQueue:           fmt.Sprintf("test-fx-dlq-%d.fifo", time.Now().UnixNano()),
		KeycloakURL:           "http://localhost:8085",
		KeycloakRealm:         "jungle",
		AuthEnabled:           true,
	}

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
			// Invocação que força a montagem de todo o grafo de dependências
		}),
	)

	// 1. Início do ciclo de vida Fx
	startCtx, cancelStart := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelStart()

	if err := app.Start(startCtx); err != nil {
		t.Fatalf("failed to start Fx application: %v", err)
	}

	// Aguarda estabilização do listener HTTP
	time.Sleep(100 * time.Millisecond)

	// 2. Validação dos endpoints ativos via HTTP
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)

	// Valida Liveness
	respLive, err := http.Get(baseURL + "/health/live")
	if err != nil {
		t.Fatalf("failed to call /health/live on started app: %v", err)
	}
	respLive.Body.Close()
	if respLive.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK from /health/live, got %d", respLive.StatusCode)
	}

	// Valida Readiness (banco e SQS reais em operação)
	respReady, err := http.Get(baseURL + "/health/ready")
	if err != nil {
		t.Fatalf("failed to call /health/ready on started app: %v", err)
	}
	respReady.Body.Close()
	if respReady.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK from /health/ready, got %d", respReady.StatusCode)
	}

	// 3. Encerramento gracioso do Fx e liberação ordenada de recursos
	stopCtx, cancelStop := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelStop()

	if err := app.Stop(stopCtx); err != nil {
		t.Fatalf("failed to gracefully stop Fx application: %v", err)
	}

	// 4. Comprova liberação de porta e parada efetiva do servidor HTTP
	time.Sleep(50 * time.Millisecond)
	client := &http.Client{Timeout: 1 * time.Second}
	_, err = client.Get(baseURL + "/health/live")
	if err == nil {
		t.Errorf("expected connection error after app.Stop(), but HTTP request succeeded")
	}
}
