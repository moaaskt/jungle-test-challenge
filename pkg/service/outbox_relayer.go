package service

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/moaaskt/jungle-test-challenge/pkg/repository"
)

// OutboxRelayerConfig define os parâmetros de operação do worker de outbox.
type OutboxRelayerConfig struct {
	BatchSize     int
	PollInterval  time.Duration
	LeaseDuration time.Duration
	MaxRetries    int
	BaseBackoff   time.Duration
	MaxBackoff    time.Duration
}

// DefaultOutboxRelayerConfig retorna uma configuração padrão segura para produção.
func DefaultOutboxRelayerConfig() OutboxRelayerConfig {
	return OutboxRelayerConfig{
		BatchSize:     50,
		PollInterval:  200 * time.Millisecond,
		LeaseDuration: 30 * time.Second,
		MaxRetries:    5,
		BaseBackoff:   500 * time.Millisecond,
		MaxBackoff:    5 * time.Minute,
	}
}

// OutboxRelayer é o background worker responsável por processar e despachar eventos pendentes da outbox.
type OutboxRelayer struct {
	repo      repository.OutboxRepository
	publisher EventPublisher
	cfg       OutboxRelayerConfig

	running atomic.Bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewOutboxRelayer instancia um novo OutboxRelayer.
func NewOutboxRelayer(
	repo repository.OutboxRepository,
	publisher EventPublisher,
	cfg *OutboxRelayerConfig,
) *OutboxRelayer {
	c := DefaultOutboxRelayerConfig()
	if cfg != nil {
		if cfg.BatchSize > 0 {
			c.BatchSize = cfg.BatchSize
		}
		if cfg.PollInterval > 0 {
			c.PollInterval = cfg.PollInterval
		}
		if cfg.LeaseDuration > 0 {
			c.LeaseDuration = cfg.LeaseDuration
		}
		if cfg.MaxRetries > 0 {
			c.MaxRetries = cfg.MaxRetries
		}
		if cfg.BaseBackoff > 0 {
			c.BaseBackoff = cfg.BaseBackoff
		}
		if cfg.MaxBackoff > 0 {
			c.MaxBackoff = cfg.MaxBackoff
		}
	}

	return &OutboxRelayer{
		repo:      repo,
		publisher: publisher,
		cfg:       c,
		stopCh:    make(chan struct{}),
	}
}

// Start inicia o loop de processamento em background.
func (r *OutboxRelayer) Start() {
	if !r.running.CompareAndSwap(false, true) {
		return
	}

	r.wg.Add(1)
	go r.pollLoop()
}

// Stop realiza o encerramento gracioso do relayer.
func (r *OutboxRelayer) Stop(ctx context.Context) error {
	if !r.running.CompareAndSwap(true, false) {
		return nil
	}

	close(r.stopCh)

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("outbox relayer shutdown timed out: %w", ctx.Err())
	}
}

func (r *OutboxRelayer) pollLoop() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			n, err := r.ProcessBatch(ctx)
			cancel()
			if err != nil {
				log.Printf("[OUTBOX RELAYER ERROR] failed to process batch: %v", err)
			}
			// Se o lote estava cheio, pode processar imediatamente o próximo sem esperar o ticker
			if n >= r.cfg.BatchSize {
				ticker.Reset(10 * time.Millisecond)
			} else {
				ticker.Reset(r.cfg.PollInterval)
			}
		}
	}
}

// ProcessBatch executa a reivindicação e despacho de um único lote de eventos pendentes.
func (r *OutboxRelayer) ProcessBatch(ctx context.Context) (int, error) {
	events, err := r.repo.ClaimPending(ctx, r.cfg.BatchSize, r.cfg.LeaseDuration)
	if err != nil {
		return 0, fmt.Errorf("failed to claim pending events: %w", err)
	}

	if len(events) == 0 {
		return 0, nil
	}

	processedCount := 0
	for _, ev := range events {
		pubErr := r.publisher.Publish(ctx, ev)
		if pubErr == nil {
			if err := r.repo.MarkPublished(ctx, ev.ID); err != nil {
				log.Printf("[OUTBOX ERROR] failed to mark published for event %s: %v", ev.ID, err)
			}
			processedCount++
		} else {
			// Calcular backoff exponencial: base * 2^(retry_count - 1)
			shift := ev.RetryCount - 1
			if shift < 0 {
				shift = 0
			}
			if shift > 10 {
				shift = 10
			}
			backoff := r.cfg.BaseBackoff * (1 << shift)
			if backoff > r.cfg.MaxBackoff {
				backoff = r.cfg.MaxBackoff
			}

			nextAttempt := time.Now().Add(backoff)
			isFinal := ev.RetryCount >= r.cfg.MaxRetries

			if err := r.repo.MarkFailed(ctx, ev.ID, pubErr.Error(), nextAttempt, isFinal); err != nil {
				log.Printf("[OUTBOX ERROR] failed to mark failed for event %s: %v", ev.ID, err)
			}
		}
	}

	return processedCount, nil
}
