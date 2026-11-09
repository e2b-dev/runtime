package config

import (
	"errors"
	"fmt"
	"os"

	"github.com/caarlos0/env/v11"
)

type Config struct {
	GRPCAddress       string `env:"SCHEDULING_GRPC_ADDRESS,required,notEmpty"`
	DatabaseURL       string `env:"SCHEDULING_DATABASE_URL,required,notEmpty"`
	WritePoolMaxConns int32  `env:"SCHEDULING_WRITE_POOL_MAX_CONNS"           envDefault:"24"`
	ReadPoolMaxConns  int32  `env:"SCHEDULING_READ_POOL_MAX_CONNS"            envDefault:"8"`
}

type MigratorConfig struct {
	DatabaseURL string `env:"SCHEDULING_MIGRATOR_DATABASE_URL,required,notEmpty"`
}

func Parse() (Config, error) {
	return parseConfig(env.Options{Environment: env.ToMap(os.Environ())})
}

func parseConfig(options env.Options) (Config, error) {
	cfg, err := parse[Config](options)
	if err != nil {
		return Config{}, err
	}
	if cfg.WritePoolMaxConns <= 0 {
		return Config{}, errors.New("SCHEDULING_WRITE_POOL_MAX_CONNS must be positive")
	}
	if cfg.ReadPoolMaxConns <= 0 {
		return Config{}, errors.New("SCHEDULING_READ_POOL_MAX_CONNS must be positive")
	}

	return cfg, nil
}

func ParseMigrator() (MigratorConfig, error) {
	return parse[MigratorConfig](env.Options{Environment: env.ToMap(os.Environ())})
}

func parse[T any](options env.Options) (T, error) {
	config, err := env.ParseAsWithOptions[T](options)
	if err != nil {
		var zero T

		return zero, fmt.Errorf("invalid configuration: %w", err)
	}

	return config, nil
}
