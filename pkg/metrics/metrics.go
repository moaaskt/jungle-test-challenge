package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Catálogo de métricas mandatórias da Seção 12 da especificação do desafio Jungle.
var (
	// TransactionsTotal conta os resultados de transações financeiras por status, kind e provider.
	TransactionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "wager_transactions_total",
			Help: "Total de transações de apostas processadas por status, kind e provider",
		},
		[]string{"status", "kind", "provider"},
	)

	// DuplicatesTotal conta requisições duplicadas detectadas por idempotência (HTTP e SQS).
	DuplicatesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "wager_duplicates_total",
			Help: "Total de requisições duplicadas deduplicadas por idempotência",
		},
		[]string{"origin"},
	)

	// SQSRetriesTotal conta o total de retries de consumo de mensagens no SQS.
	SQSRetriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sqs_retries_total",
			Help: "Total de retries de processamento de mensagens no consumidor SQS",
		},
		[]string{"queue"},
	)

	// SQSDLQTotal conta o número de mensagens movidas para a Dead Letter Queue.
	SQSDLQTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sqs_dlq_total",
			Help: "Total de mensagens enviadas para a Dead Letter Queue (DLQ)",
		},
		[]string{"queue"},
	)

	// ConcurrencyConflictsTotal conta conflitos de concorrência e optimistic lock.
	ConcurrencyConflictsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "concurrency_conflicts_total",
			Help: "Total de conflitos de concorrência detectados (optimistic lock ou chave duplicada)",
		},
		[]string{"type"},
	)

	// OutboxLagSeconds mede o atraso entre a criação do evento na outbox e a publicação efetiva no SQS.
	OutboxLagSeconds = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "outbox_lag_seconds",
			Help: "Atraso atual do relayer da outbox em segundos",
		},
	)

	// ProcessingDuration registra a latência de processamento de transações em segundos.
	ProcessingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "wager_processing_duration_seconds",
			Help:    "Latência de processamento de operações de wagering em segundos",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
		[]string{"kind"},
	)

	// ReconciliationDivergencesTotal conta divergências entre saldo armazenado e reconstruído do ledger.
	ReconciliationDivergencesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "reconciliation_divergences_total",
			Help: "Total de divergências de saldo detectadas pelo endpoint de reconciliação",
		},
	)
)

func init() {
	// Pré-inicializa as séries com labels padrão para exposição imediata no endpoint /metrics
	TransactionsTotal.WithLabelValues("PROCESSED", "BET", "unknown")
	DuplicatesTotal.WithLabelValues("http")
	SQSRetriesTotal.WithLabelValues("wager-transactions")
	SQSDLQTotal.WithLabelValues("wager-transactions-dlq")
	ConcurrencyConflictsTotal.WithLabelValues("optimistic_lock")
	ProcessingDuration.WithLabelValues("BET")
}
