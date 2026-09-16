package config

import (
	"errors"
	"fmt"
	"os"
)

// Config centraliza as variáveis de ambiente essenciais para a inicialização da aplicação.
type Config struct {
	DatabaseURL string
	Port        string
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

	return &Config{
		DatabaseURL: dbURL,
		Port:        port,
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
