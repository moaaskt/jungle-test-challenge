package config

import (
	"errors"
	"fmt"
	"os"
)

// Config centraliza as variáveis de ambiente essenciais para a inicialização da aplicação.
type Config struct {
	DatabaseURL          string
	Port                 string
	AWSEndpoint          string
	AWSRegion            string
	SQSWagerRequestsQueue string
	SQSWagerEventsQueue  string
	SQSDLQQueue          string
}

// Load lê e valida as variáveis de ambiente obrigatórias.
// Retorna erro imediatamente (fail-fast) se algo estiver faltando.
func Load() (*Config, error) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return nil, errors.New("DATABASE_URL environment variable is required")
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // Valor padrão caso não seja informado
	}

	awsEndpoint := os.Getenv("AWS_ENDPOINT_URL")
	if awsEndpoint == "" {
		awsEndpoint = os.Getenv("AWS_ENDPOINT")
	}
	if awsEndpoint == "" {
		awsEndpoint = "http://localhost:4566"
	}

	awsRegion := os.Getenv("AWS_REGION")
	if awsRegion == "" {
		awsRegion = "us-east-1"
	}

	reqQueue := os.Getenv("SQS_WAGER_REQUESTS_QUEUE")
	if reqQueue == "" {
		reqQueue = "wager-transactions.fifo"
	}

	eventsQueue := os.Getenv("SQS_WAGER_EVENTS_QUEUE")
	if eventsQueue == "" {
		eventsQueue = "wager-events.fifo"
	}

	dlqQueue := os.Getenv("SQS_DLQ_QUEUE")
	if dlqQueue == "" {
		dlqQueue = "wager-transactions-dlq.fifo"
	}

	return &Config{
		DatabaseURL:           dbURL,
		Port:                  port,
		AWSEndpoint:           awsEndpoint,
		AWSRegion:             awsRegion,
		SQSWagerRequestsQueue: reqQueue,
		SQSWagerEventsQueue:   eventsQueue,
		SQSDLQQueue:           dlqQueue,
	}, nil
}

// MustLoad tenta carregar as configurações e aciona um panic em caso de falha.
// Útil para o OnStart do Uber Fx garantir o fail-fast.
func MustLoad() *Config {
	cfg, err := Load()
	if err != nil {
		panic(fmt.Errorf("failed to load configuration: %w", err))
	}
	return cfg
}
