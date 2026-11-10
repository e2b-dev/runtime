package pool

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/e2b-dev/infra/packages/db/pkg/retry"
)

type Option func(config *pgxpool.Config, retryConfig *retry.Config)

func WithMaxConnections(maxConns int32) Option {
	return func(config *pgxpool.Config, _ *retry.Config) {
		config.MaxConns = maxConns
	}
}

func WithMinIdle(minIdle int32) Option {
	return func(config *pgxpool.Config, _ *retry.Config) {
		config.MinIdleConns = minIdle
	}
}

func WithRuntimeParam(name string, value string) Option {
	return func(config *pgxpool.Config, _ *retry.Config) {
		config.ConnConfig.RuntimeParams[name] = value
	}
}

func WithMaxConnLifetime(lifetime time.Duration) Option {
	return func(config *pgxpool.Config, _ *retry.Config) {
		if lifetime > 0 {
			config.MaxConnLifetime = lifetime
		}
	}
}

func WithMaxConnLifetimeJitter(jitter time.Duration) Option {
	return func(config *pgxpool.Config, _ *retry.Config) {
		if jitter > 0 {
			config.MaxConnLifetimeJitter = jitter
		}
	}
}
