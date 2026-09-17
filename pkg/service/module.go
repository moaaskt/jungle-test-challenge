package service

import (
	"context"

	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(
		NewWagerService,
		func() *OutboxRelayerConfig {
			cfg := DefaultOutboxRelayerConfig()
			return &cfg
		},
		NewOutboxRelayer,
		DefaultPendingRefResolverConfig,
		NewPendingRefResolver,
	),
	fx.Invoke(registerOutboxRelayerLifecycle),
	fx.Invoke(registerPendingRefResolverLifecycle),
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

func registerPendingRefResolverLifecycle(lc fx.Lifecycle, resolver *PendingRefResolver) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			resolver.Start(context.Background())
			return nil
		},
		OnStop: func(ctx context.Context) error {
			resolver.Stop()
			return nil
		},
	})
}

