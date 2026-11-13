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
	valid := map[string]string{variable: ":50051"}
	config, err := parseConfig(env.Options{Environment: valid})
	require.NoError(t, err)
	require.Equal(t, Config{GRPCAddress: ":50051"}, config)

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
