package messaging

import (
	"context"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/moaaskt/jungle-test-challenge/pkg/service"
	"go.uber.org/fx"
)

// Module fornece o cliente SQS, provisionador, filas e consumidor com lifecycle hooks.
var Module = fx.Options(
	fx.Provide(
		NewSQSClient,
		NewQueueProvisioner,
		provideQueuesConfig,
		provideSQSPublisher,
		provideSQSConsumer,
	),
	fx.Invoke(registerMessagingLifecycle),
)

func provideQueuesConfig(p QueueProvisioner) (*QueuesConfig, error) {
	return p.EnsureQueues(context.Background())
}

func provideSQSPublisher(client *sqs.Client, queues *QueuesConfig) service.EventPublisher {
	return NewSQSPublisher(client, queues.WagerEventsQueueURL)
}

func provideSQSConsumer(
	client *sqs.Client,
	wagerService service.WagerService,
	queues *QueuesConfig,
	logger *slog.Logger,
) *SQSConsumer {
	cfg := DefaultSQSConsumerConfig(queues.WagerRequestsQueueURL)
	return NewSQSConsumer(client, wagerService, cfg, logger)
}

func registerMessagingLifecycle(lc fx.Lifecycle, consumer *SQSConsumer) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			consumer.Start()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return consumer.Stop(ctx)
		},
	})
}
