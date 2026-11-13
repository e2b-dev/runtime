package config

import (
	"fmt"
	"os"

	"github.com/caarlos0/env/v11"

	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
)

type Config struct {
	TLS         mtls.ListenerSettings `envPrefix:"E2B_TLS_GRPC_"`
	GRPCAddress string                `env:"SCHEDULING_GRPC_ADDRESS,required,notEmpty"`
}

func Parse() (Config, error) {
	return parseConfig(env.Options{Environment: env.ToMap(os.Environ())})
}

func parseConfig(options env.Options) (Config, error) {
	cfg, err := env.ParseAsWithOptions[Config](options)
	if err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}
