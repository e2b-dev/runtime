package cfg

import (
	"errors"
	"reflect"

	"github.com/caarlos0/env/v11"

	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

type Config struct {
	featureflags.Config

	Port                        int                       `env:"PORT"                                         envDefault:"3010"`
	PostgresConnectionString    string                    `env:"POSTGRES_CONNECTION_STRING,required,notEmpty"`
	ClickhouseConnectionString  string                    `env:"CLICKHOUSE_CONNECTION_STRING"`
	ClickhouseConnectionStrings []string                  `env:"CLICKHOUSE_CONNECTION_STRINGS"                envSeparator:";"`
	AdminToken                  string                    `env:"ADMIN_TOKEN,required,notEmpty"`
	AuthProvider                sharedauth.ProviderConfig `env:"AUTH_PROVIDER_CONFIG"`
	AdminAuthProvider           sharedauth.ProviderConfig `env:"ADMIN_AUTH_PROVIDER_CONFIG"`

	AuthDBConnectionString string `env:"AUTH_DB_CONNECTION_STRING"`

	RedisURL         string `env:"REDIS_URL"`
	RedisClusterURL  string `env:"REDIS_CLUSTER_URL"`
	RedisTLSCABase64 string `env:"REDIS_TLS_CA_BASE64"`
	RedisTLSEnabled  bool   `env:"REDIS_TLS_ENABLED"`
	RedisPassword    string `env:"REDIS_PASSWORD"`

	// TLS is the mutual TLS settings of the HTTP port.
	TLS TLSConfig
}

type FailureCondition string

const (
	FailureConditionMissingRedisConnection FailureCondition = "missing_redis_connection"
)

type FailureError struct {
	Condition FailureCondition
	err       error
}

func (e *FailureError) Error() string {
	return e.err.Error()
}

func (e *FailureError) Unwrap() error {
	return e.err
}

func ParseFailureCondition(err error) (FailureCondition, bool) {
	var failureErr *FailureError
	if !errors.As(err, &failureErr) {
		return "", false
	}

	return failureErr.Condition, true
}

func newFailureError(condition FailureCondition, message string) error {
	return &FailureError{
		Condition: condition,
		err:       errors.New(message),
	}
}

func Parse() (Config, error) {
	var config Config
	err := env.ParseWithOptions(&config, env.Options{
		FuncMap: map[reflect.Type]env.ParserFunc{
			reflect.TypeFor[sharedauth.ProviderConfig](): func(v string) (any, error) {
				return sharedauth.ParseProviderConfig(v)
			},
		},
	})

	if config.AuthDBConnectionString == "" {
		config.AuthDBConnectionString = config.PostgresConnectionString
	}

	if err == nil && config.RedisURL == "" && config.RedisClusterURL == "" {
		err = newFailureError(FailureConditionMissingRedisConnection, "at least one of REDIS_URL or REDIS_CLUSTER_URL must be set")
	}

	return config, err
}
