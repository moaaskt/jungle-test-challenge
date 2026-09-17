package service

import (
	"context"

	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(
		NewWagerService,
		NewLogPublisher,
		func() *OutboxRelayerConfig {
			cfg := DefaultOutboxRelayerConfig()
			return &cfg
		},
		NewOutboxRelayer,
	),
	fx.Invoke(registerOutboxRelayerLifecycle),
)

func registerOutboxRelayerLifecycle(lc fx.Lifecycle, relayer *OutboxRelayer) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			relayer.Start()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return relayer.Stop(ctx)
		},
	})
}

