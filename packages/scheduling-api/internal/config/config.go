package config

import (
	"errors"
	"fmt"
	"os"

	"github.com/caarlos0/env/v11"

	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
)

type Config struct {
	TLS         mtls.ListenerSettings `envPrefix:"E2B_TLS_GRPC_"`
	GRPCAddress string                `env:"SCHEDULING_GRPC_ADDRESS,required,notEmpty"`

	InfraRedisURL         string `env:"INFRA_REDIS_URL"`
	InfraRedisClusterURL  string `env:"INFRA_REDIS_CLUSTER_URL"`
	InfraRedisTLSCABase64 string `env:"INFRA_REDIS_TLS_CA_BASE64"`
	InfraRedisTLSEnabled  bool   `env:"INFRA_REDIS_TLS_ENABLED"`
	InfraRedisPassword    string `env:"INFRA_REDIS_PASSWORD"`
	InfraRedisPoolSize    int    `env:"INFRA_REDIS_POOL_SIZE"     envDefault:"160"`
}

func Parse() (Config, error) {
	return parseConfig(env.Options{Environment: env.ToMap(os.Environ())})
}

func parseConfig(options env.Options) (Config, error) {
	cfg, err := env.ParseAsWithOptions[Config](options)
	if err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	if cfg.InfraRedisURL == "" && cfg.InfraRedisClusterURL == "" {
		return Config{}, errors.New("invalid configuration: set INFRA_REDIS_URL or INFRA_REDIS_CLUSTER_URL")
	}

	return cfg, nil
}
