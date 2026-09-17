package service

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
	"github.com/moaaskt/jungle-test-challenge/pkg/repository"
)

// PendingRefResolver é um worker periódico que busca e resolve transações
// PENDING_REFERENCE cujo BET original já foi processado (Momento B),
// ou as rejeita quando expiram por TTL / max tentativas (Seção 7 do desafio).
type PendingRefResolver struct {
	pool       *pgxpool.Pool
	wagerRepo  repository.WagerTransactionRepository
	walletRepo repository.WalletRepository
	ledgerRepo repository.LedgerRepository
	outboxRepo repository.OutboxRepository
	interval   time.Duration
	logger     *slog.Logger

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// PendingRefResolverConfig define a configuração do worker.
type PendingRefResolverConfig struct {
	Interval time.Duration // intervalo de polling (default: 10s)
}

// DefaultPendingRefResolverConfig retorna a configuração padrão.
func DefaultPendingRefResolverConfig() PendingRefResolverConfig {
	return PendingRefResolverConfig{
		Interval: 10 * time.Second,
	}
}

func NewPendingRefResolver(
	pool *pgxpool.Pool,
	wagerRepo repository.WagerTransactionRepository,
	walletRepo repository.WalletRepository,
	ledgerRepo repository.LedgerRepository,
	outboxRepo repository.OutboxRepository,
	cfg PendingRefResolverConfig,
	logger *slog.Logger,
) *PendingRefResolver {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Interval == 0 {
		cfg.Interval = 10 * time.Second
	}
	return &PendingRefResolver{
		pool:       pool,
		wagerRepo:  wagerRepo,
		walletRepo: walletRepo,
		ledgerRepo: ledgerRepo,
		outboxRepo: outboxRepo,
		interval:   cfg.Interval,
		logger:     logger,
		stopCh:     make(chan struct{}),
	}
}

// Start inicia o loop de resolução periódica em background.
func (r *PendingRefResolver) Start(ctx context.Context) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.logger.Info("PendingRefResolver started", "interval", r.interval)

		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()

		for {
			select {
			case <-r.stopCh:
				r.logger.Info("PendingRefResolver stopping")
				return
			case <-ctx.Done():
				r.logger.Info("PendingRefResolver context cancelled")
				return
			case <-ticker.C:
				if err := r.resolveOnce(ctx); err != nil {
					r.logger.Error("PendingRefResolver error during resolution round", "error", err)
				}
			}
		}
	}()
}

// Stop encerra o loop graciosamente aguardando a rodada corrente.
func (r *PendingRefResolver) Stop() {
	close(r.stopCh)
	r.wg.Wait()
	r.logger.Info("PendingRefResolver stopped")
}

// ResolveOnce executa uma rodada completa de resolução. Público para testes.
func (r *PendingRefResolver) ResolveOnce(ctx context.Context) error {
	return r.resolveOnce(ctx)
}

func (r *PendingRefResolver) resolveOnce(ctx context.Context) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	resolvables, err := r.wagerRepo.GetAllResolvable(ctx, tx)
	if err != nil {
		return fmt.Errorf("failed to get resolvable pending refs: %w", err)
	}

	if len(resolvables) == 0 {
		return nil
	}

	r.logger.Info("PendingRefResolver found resolvable items", "count", len(resolvables))

	// Construímos um wagerService temporário para reutilizar resolveSinglePendingRef.
	// Como o resolver opera em sua própria pgx.Tx, precisamos de acesso ao mesmo set
	// de repositórios. Usamos uma struct local que implementa a lógica de resolução.
	svc := &wagerService{
		pool:       r.pool,
		walletRepo: r.walletRepo,
		wagerRepo:  r.wagerRepo,
		ledgerRepo: r.ledgerRepo,
		outboxRepo: r.outboxRepo,
	}

	for _, item := range resolvables {
		pending := item.Pending
		resolved := item.ResolvedBy

		correlationID := pending.ID.String()
		if pending.IdempotencyKey != nil && *pending.IdempotencyKey != "" {
			correlationID = *pending.IdempotencyKey
		}

		if resolved == nil {
			// TTL expirado ou max tentativas atingido — rejeitar
			if err := svc.resolveSinglePendingRef(ctx, tx, nil, pending, nil, correlationID, nil); err != nil {
				r.logger.Error("Failed to reject expired pending ref", "txId", pending.ID, "error", err)
				continue
			}
			r.logger.Info("Rejected expired PENDING_REFERENCE", "txId", pending.ID, "failureCode", domain.FailureCodeReferenceNotFound)
			continue
		}

		// Referência encontrada — verificar se está em estado processável
		if resolved.Status == domain.StatusProcessed {
			// Obter a carteira com lock para aplicar movimento financeiro
			w, err := r.walletRepo.GetByIDForUpdate(ctx, tx, pending.WalletID)
			if err != nil {
				r.logger.Error("Failed to get wallet for pending ref resolution", "walletId", pending.WalletID, "error", err)
				continue
			}

			if err := svc.resolveSinglePendingRef(ctx, tx, w, pending, resolved, correlationID, nil); err != nil {
				r.logger.Error("Failed to resolve pending ref", "txId", pending.ID, "originalId", resolved.ID, "error", err)
				continue
			}
			r.logger.Info("Resolved PENDING_REFERENCE", "txId", pending.ID, "originalId", resolved.ID, "direction", pending.ResolutionDirection(resolved.Type))
		} else if resolved.Status == domain.StatusRejected || resolved.Status == domain.StatusFailed {
			// Referência original terminou em estado de falha
			if err := svc.resolveSinglePendingRef(ctx, tx, nil, pending, resolved, correlationID, nil); err != nil {
				r.logger.Error("Failed to reject pending ref due to failed original", "txId", pending.ID, "error", err)
				continue
			}
			r.logger.Info("Rejected PENDING_REFERENCE due to failed original", "txId", pending.ID, "failureCode", domain.FailureCodeOriginalTransactionFailed)
		} else {
			// Referência original ainda em PENDING ou PENDING_REFERENCE — incrementar tentativa com backoff
			pending.Attempts++
			backoffSec := math.Min(math.Pow(2, float64(pending.Attempts)), 300)
			nextAttempt := time.Now().Add(time.Duration(backoffSec) * time.Second)
			pending.NextAttemptAt = &nextAttempt
			pending.UpdatedAt = time.Now()

			if err := r.wagerRepo.Update(ctx, tx, pending); err != nil {
				r.logger.Error("Failed to update pending ref with backoff", "txId", pending.ID, "error", err)
				continue
			}
			r.logger.Info("Deferred PENDING_REFERENCE with backoff", "txId", pending.ID, "attempt", pending.Attempts, "nextAttempt", nextAttempt)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit resolution round: %w", err)
	}

	return nil
}
