package service

import (
	"context"
	"log"

	"github.com/moaaskt/jungle-test-challenge/pkg/domain"
)

// EventPublisher define o contrato para despacho de eventos da outbox para mensageria externa.
// Na Fase 7, este contrato será conectado ao produtor AWS SQS.
type EventPublisher interface {
	Publish(ctx context.Context, event *domain.OutboxEvent) error
}

// LogPublisher é a implementação padrão para a Fase 6, registrando a publicação em logs estruturados.
type LogPublisher struct{}

func NewLogPublisher() EventPublisher {
	return &LogPublisher{}
}

func (p *LogPublisher) Publish(ctx context.Context, event *domain.OutboxEvent) error {
	log.Printf("[OUTBOX PUBLISHED] EventID=%s Type=%s AggregateID=%s Attempt=%d",
		event.ID, event.EventType, event.AggregateID, event.RetryCount)
	return nil
}
