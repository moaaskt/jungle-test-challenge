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
		DefaultStaleTxRecoveryConfig,
		NewStaleTxRecoveryWorker,
	),
	fx.Invoke(registerOutboxRelayerLifecycle),
	fx.Invoke(registerPendingRefResolverLifecycle),
	fx.Invoke(registerStaleTxRecoveryWorkerLifecycle),
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

func registerStaleTxRecoveryWorkerLifecycle(lc fx.Lifecycle, worker *StaleTxRecoveryWorker) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			worker.Start(context.Background())
			return nil
		},
		OnStop: func(ctx context.Context) error {
			worker.Stop()
			return nil
		},
	})
}

