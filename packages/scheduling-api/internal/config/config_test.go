package config

import (
	"maps"
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	t.Parallel()

	const url = "postgres://localhost/scheduling"
	valid := map[string]string{"SCHEDULING_GRPC_ADDRESS": ":50051", "SCHEDULING_DATABASE_URL": url}
	config, err := parseConfig(env.Options{Environment: valid})
	require.NoError(t, err)
	require.Equal(t, Config{GRPCAddress: ":50051", DatabaseURL: url, WritePoolMaxConns: 24, ReadPoolMaxConns: 8}, config)

	for _, variable := range []string{"SCHEDULING_GRPC_ADDRESS", "SCHEDULING_DATABASE_URL"} {
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
}

func TestPoolLimits(t *testing.T) {
	t.Parallel()
	valid := map[string]string{
		"SCHEDULING_GRPC_ADDRESS":         ":50051",
		"SCHEDULING_DATABASE_URL":         "postgres://localhost/scheduling",
		"SCHEDULING_WRITE_POOL_MAX_CONNS": "12",
		"SCHEDULING_READ_POOL_MAX_CONNS":  "4",
	}
	cfg, err := parseConfig(env.Options{Environment: valid})
	require.NoError(t, err)
	require.Equal(t, int32(12), cfg.WritePoolMaxConns)
	require.Equal(t, int32(4), cfg.ReadPoolMaxConns)
	for _, variable := range []string{"SCHEDULING_WRITE_POOL_MAX_CONNS", "SCHEDULING_READ_POOL_MAX_CONNS"} {
		for _, value := range []string{"0", "-1", "invalid", "2147483648"} {
			t.Run(variable+"/"+value, func(t *testing.T) {
				t.Parallel()
				environment := maps.Clone(valid)
				environment[variable] = value
				_, err := parseConfig(env.Options{Environment: environment})
				require.Error(t, err)
			})
		}
	}
}

func TestParseMigrator(t *testing.T) {
	t.Parallel()

	const url = "postgres://localhost/scheduling"
	config, err := parse[MigratorConfig](env.Options{Environment: map[string]string{"SCHEDULING_MIGRATOR_DATABASE_URL": url}})
	require.NoError(t, err)
	require.Equal(t, MigratorConfig{DatabaseURL: url}, config)

	for name, environment := range map[string]map[string]string{
		"missing": {},
		"empty":   {"SCHEDULING_MIGRATOR_DATABASE_URL": ""},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := parse[MigratorConfig](env.Options{Environment: environment})
			require.ErrorContains(t, err, "SCHEDULING_MIGRATOR_DATABASE_URL")
		})
	}
}
