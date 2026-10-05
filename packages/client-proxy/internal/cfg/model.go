package cfg

import (
	"errors"

	"github.com/caarlos0/env/v11"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

type Config struct {
	featureflags.Config

	HealthPort            uint16 `env:"HEALTH_PORT"             envDefault:"3003"`
	ProxyPort             uint16 `env:"PROXY_PORT"              envDefault:"3002"`
	OrchestratorProxyPort uint16 `env:"ORCHESTRATOR_PROXY_PORT" envDefault:"5007"`

	RedisURL         string `env:"REDIS_URL"`
	RedisClusterURL  string `env:"REDIS_CLUSTER_URL"`
	RedisTLSCABase64 string `env:"REDIS_TLS_CA_BASE64"`
	RedisTLSEnabled  bool   `env:"REDIS_TLS_ENABLED"`
	RedisPassword    string `env:"REDIS_PASSWORD"`
	RedisPoolSize    int    `env:"REDIS_POOL_SIZE"     envDefault:"40"`

	APIInternalGRPCAddress string `env:"API_INTERNAL_GRPC_ADDRESS"`
	APIEdgeGRPCAddress     string `env:"API_EDGE_GRPC_ADDRESS"`

	APIEdgeGRPCOAuthClientID     string `env:"API_EDGE_GRPC_OAUTH_CLIENT_ID"`
	APIEdgeGRPCOAuthClientSecret string `env:"API_EDGE_GRPC_OAUTH_CLIENT_SECRET"`
	APIEdgeGRPCOAuthTokenURL     string `env:"API_EDGE_GRPC_OAUTH_TOKEN_URL"`
}

func Parse() (Config, error) {
	config, err := env.ParseAsWithOptions[Config](env.Options{})
	if err != nil {
		return Config{}, err
	}
	if config.OrchestratorProxyPort == 0 {
		return Config{}, errors.New("ORCHESTRATOR_PROXY_PORT must be greater than zero")
	}

	return config, nil
}
