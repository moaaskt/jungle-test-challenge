package database

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moaaskt/jungle-test-challenge/pkg/config"
	"go.uber.org/fx"
)

// Module expõe os construtores deste pacote para o Fx.
var Module = fx.Provide(NewPostgresPool)

// NewPostgresPool cria o pool de conexões utilizando o lifecycle do Fx.
func NewPostgresPool(lc fx.Lifecycle, cfg *config.Config) (*pgxpool.Pool, error) {
	// Cria a configuração do pool a partir da string de conexão do env
	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database configuration: %w", err)
	}

	// Tenta criar o pool
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create database pool: %w", err)
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			log.Println("Starting database pool...")
			// Validar conexão real com Ping
			if err := pool.Ping(ctx); err != nil {
				return fmt.Errorf("database ping failed: %w", err)
			}
			log.Println("Database connection established")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			log.Println("Closing database pool...")
			pool.Close()
			return nil
		},
	})

	return pool, nil
}
