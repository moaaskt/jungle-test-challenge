package service

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
	"github.com/moaaskt/jungle-test-challenge/pkg/repository"
)

// StaleTxRecoveryWorker é um worker em background responsável por encontrar transações
// de aposta que ficaram travadas no estado PENDING após crash ou interrupção abrupta de processo
// (Requisito Seção 13, item 8 & OBS-01).
type StaleTxRecoveryWorker struct {
	pool      *pgxpool.Pool
	wagerRepo repository.WagerTransactionRepository
	cfg       StaleTxRecoveryConfig
	logger    *slog.Logger

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// StaleTxRecoveryConfig define as configurações operacionais de timeout e polling do worker.
type StaleTxRecoveryConfig struct {
	Interval       time.Duration // Frequência de execução do polling (default: 5s)
	StaleThreshold time.Duration // Limiar de inatividade para considerar uma transação como presa (default: 30s)
	BatchSize      int           // Quantidade máxima de transações por lote (default: 100)
}

// DefaultStaleTxRecoveryConfig retorna as configurações padrão de recuperação de transações presas.
func DefaultStaleTxRecoveryConfig() StaleTxRecoveryConfig {
	return StaleTxRecoveryConfig{
		Interval:       5 * time.Second,
		StaleThreshold: 30 * time.Second,
		BatchSize:      100,
	}
}

// NewStaleTxRecoveryWorker instancia o worker de recuperação de transações pendentes presas.
func NewStaleTxRecoveryWorker(
	pool *pgxpool.Pool,
	wagerRepo repository.WagerTransactionRepository,
	cfg StaleTxRecoveryConfig,
	logger *slog.Logger,
) *StaleTxRecoveryWorker {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	if cfg.StaleThreshold <= 0 {
		cfg.StaleThreshold = 30 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	return &StaleTxRecoveryWorker{
		pool:      pool,
		wagerRepo: wagerRepo,
		cfg:       cfg,
		logger:    logger,
		stopCh:    make(chan struct{}),
	}
}

// Start inicia o loop de monitoramento periódico em background.
func (w *StaleTxRecoveryWorker) Start(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.logger.Info("StaleTxRecoveryWorker started", "interval", w.cfg.Interval, "staleThreshold", w.cfg.StaleThreshold)

		ticker := time.NewTicker(w.cfg.Interval)
		defer ticker.Stop()

		for {
			select {
			case <-w.stopCh:
				w.logger.Info("StaleTxRecoveryWorker stopping")
				return
			case <-ctx.Done():
				w.logger.Info("StaleTxRecoveryWorker context cancelled")
				return
			case <-ticker.C:
				if _, err := w.RecoverOnce(ctx); err != nil {
					w.logger.Error("StaleTxRecoveryWorker error during recovery batch", "error", err)
				}
			}
		}
	}()
}

// Stop encerra o loop de recuperação graciosamente aguardando a rodada corrente.
func (w *StaleTxRecoveryWorker) Stop() {
	close(w.stopCh)
	w.wg.Wait()
	w.logger.Info("StaleTxRecoveryWorker stopped")
}

// RecoverOnce executa uma rodada síncrona de recuperação de transações pendentes presas.
// Fornecido também como método público para testes automatizados sem latência de ticker.
func (w *StaleTxRecoveryWorker) RecoverOnce(ctx context.Context) (int, error) {
	tx, err := w.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	staleTxs, err := w.wagerRepo.GetStalePending(ctx, tx, w.cfg.StaleThreshold, w.cfg.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("failed to query stale pending transactions: %w", err)
	}

	if len(staleTxs) == 0 {
		return 0, nil
	}

	w.logger.Warn("StaleTxRecoveryWorker found stale pending transactions", "count", len(staleTxs))

	recoveredCount := 0
	for _, wt := range staleTxs {
		// 1. Checa se o ledger já havia sido gravado para este transaction_id
		var hasLedger bool
		checkLedgerQuery := `SELECT EXISTS (SELECT 1 FROM wallet_ledger_entries WHERE transaction_id = $1)`
		if err := tx.QueryRow(ctx, checkLedgerQuery, wt.ID).Scan(&hasLedger); err != nil {
			w.logger.Error("Failed to check ledger for stale tx", "txId", wt.ID, "error", err)
			continue
		}

		if hasLedger {
			// Ledger existe: o processamento financeiro ocorreu, mas o processo crashou antes de atualizar o status
			if err := wt.Process(); err != nil {
				w.logger.Error("Failed to transition stale tx with ledger to PROCESSED", "txId", wt.ID, "error", err)
				continue
			}
			w.logger.Info("Recovered stale pending transaction with existing ledger entry", "txId", wt.ID, "status", wt.Status)
		} else {
			// Ledger não existe: o processo crashou antes de debitar/creditar a carteira. Timeout definitivo para falha segura.
			if err := wt.Fail(domain.FailureCodeTransactionTimeout); err != nil {
				w.logger.Error("Failed to transition stale tx to FAILED", "txId", wt.ID, "error", err)
				continue
			}
			w.logger.Warn("Failed stale pending transaction due to timeout/process interruption", "txId", wt.ID, "failureCode", domain.FailureCodeTransactionTimeout)
		}

		wt.UpdatedAt = time.Now()
		if err := w.wagerRepo.Update(ctx, tx, wt); err != nil {
			w.logger.Error("Failed to update recovered tx in repository", "txId", wt.ID, "error", err)
			continue
		}
		recoveredCount++
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit recovery transaction: %w", err)
	}

	return recoveredCount, nil
}
