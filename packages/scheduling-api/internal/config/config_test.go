package config

import (
	"maps"
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	t.Parallel()

	const variable = "SCHEDULING_GRPC_ADDRESS"
	valid := map[string]string{variable: ":50051", "INFRA_REDIS_URL": "redis://localhost:6379"}
	config, err := parseConfig(env.Options{Environment: valid})
	require.NoError(t, err)
	require.Equal(t, Config{GRPCAddress: ":50051", InfraRedisURL: "redis://localhost:6379", InfraRedisPoolSize: 160}, config)

	for name, value := range map[string]*string{"missing": nil, "empty": new("")} {
		t.Run(variable+" "+name, func(t *testing.T) {
			t.Parallel()
			environment := maps.Clone(valid)
			delete(environment, variable)
			if value != nil {
				environment[variable] = *value
			}
			_, err := parseConfig(env.Options{Environment: environment})
			require.ErrorContains(t, err, variable)
		})
	}
}

func TestParseRedis(t *testing.T) {
	t.Parallel()

	config, err := parseConfig(env.Options{Environment: map[string]string{
		"SCHEDULING_GRPC_ADDRESS":   ":50051",
		"INFRA_REDIS_CLUSTER_URL":   "redis-cluster:6379",
		"INFRA_REDIS_TLS_CA_BASE64": "Y2E=",
		"INFRA_REDIS_TLS_ENABLED":   "true",
		"INFRA_REDIS_PASSWORD":      "secret",
		"INFRA_REDIS_POOL_SIZE":     "20",
	}})
	require.NoError(t, err)
	require.Equal(t, Config{
		GRPCAddress:           ":50051",
		InfraRedisClusterURL:  "redis-cluster:6379",
		InfraRedisTLSCABase64: "Y2E=",
		InfraRedisTLSEnabled:  true,
		InfraRedisPassword:    "secret",
		InfraRedisPoolSize:    20,
	}, config)

	for name, environment := range map[string]map[string]string{
		"missing": {"SCHEDULING_GRPC_ADDRESS": ":50051"},
		"empty":   {"SCHEDULING_GRPC_ADDRESS": ":50051", "INFRA_REDIS_URL": "", "INFRA_REDIS_CLUSTER_URL": ""},
	} {
		t.Run("INFRA_REDIS_URL and INFRA_REDIS_CLUSTER_URL "+name, func(t *testing.T) {
			t.Parallel()
			_, err := parseConfig(env.Options{Environment: environment})
			require.EqualError(t, err, "invalid configuration: set INFRA_REDIS_URL or INFRA_REDIS_CLUSTER_URL")
		})
	}
}
