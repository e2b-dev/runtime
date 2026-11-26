//go:build linux

//nolint:paralleltest // many tests set env, which may cause issues
package cfg

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	t.Run("embedded structs get defaults", func(t *testing.T) {
		config, err := Parse()
		require.NoError(t, err)

		assert.Equal(t, "/fc-vm", config.SandboxDir)
	})

	t.Run("embedded structs get overrides", func(t *testing.T) {
		t.Setenv("SANDBOX_DIR", "/fc-vm2")

		config, err := Parse()
		require.NoError(t, err)

		assert.Equal(t, "/fc-vm2", config.SandboxDir)
	})

	t.Run("multiple services parses correctly", func(t *testing.T) {
		t.Setenv("ORCHESTRATOR_SERVICES", "service1,service2")

		config, err := Parse()
		require.NoError(t, err)

		assert.Equal(t, []string{"service1", "service2"}, config.Services)
	})

	t.Run("env defaults get defaults before expansion", func(t *testing.T) {
		config, err := Parse()
		require.NoError(t, err)
		assert.Equal(t, "/orchestrator/build", config.DefaultCacheDir)
	})

	t.Run("env defaults get expanded", func(t *testing.T) {
		t.Setenv("ORCHESTRATOR_BASE_PATH", "/a/b/c")
		config, err := Parse()
		require.NoError(t, err)
		assert.Equal(t, "/a/b/c/build", config.DefaultCacheDir)
		assert.Equal(t, "/a/b/c/sandbox", config.StorageConfig.SandboxCacheDir)
	})

	t.Run("nfs proxy metrics is enabled by default", func(t *testing.T) {
		config, err := Parse()
		require.NoError(t, err)
		assert.True(t, config.NFSProxyMetrics)
	})

	t.Run("nfs proxy metrics can be disabled", func(t *testing.T) {
		t.Setenv("NFS_PROXY_METRICS", "false")

		config, err := Parse()
		require.NoError(t, err)
		assert.False(t, config.NFSProxyMetrics)
	})

	t.Run("startup reclaim can be disabled", func(t *testing.T) {
		t.Setenv("DISABLE_STARTUP_RECLAIM", "true")

		config, err := Parse()
		require.NoError(t, err)
		assert.True(t, config.DisableStartupReclaim)
	})

	t.Run("volume read dir concurrency gets defaults", func(t *testing.T) {
		config, err := Parse()
		require.NoError(t, err)

		assert.Equal(t, ReadDirConcurrencyConfig{Threshold: 1000, Base: 8, Step: 5000, Max: 16}, config.VolumeReadDirConcurrency)
	})

	t.Run("volume read dir concurrency can be overridden", func(t *testing.T) {
		t.Setenv("VOLUME_READ_DIR_CONCURRENCY_THRESHOLD", "100")
		t.Setenv("VOLUME_READ_DIR_CONCURRENCY_BASE", "2")
		t.Setenv("VOLUME_READ_DIR_CONCURRENCY_STEP", "50")
		t.Setenv("VOLUME_READ_DIR_CONCURRENCY_MAX", "6")

		config, err := Parse()
		require.NoError(t, err)

		assert.Equal(t, ReadDirConcurrencyConfig{Threshold: 100, Base: 2, Step: 50, Max: 6}, config.VolumeReadDirConcurrency)
	})

	t.Run("volume read dir concurrency rejects unusable scaling", func(t *testing.T) {
		tests := []struct {
			name string
			envs map[string]string
		}{
			{"negative threshold", map[string]string{"VOLUME_READ_DIR_CONCURRENCY_THRESHOLD": "-1"}},
			{"zero base", map[string]string{"VOLUME_READ_DIR_CONCURRENCY_BASE": "0"}},
			{"zero step", map[string]string{"VOLUME_READ_DIR_CONCURRENCY_STEP": "0"}},
			{"max below base", map[string]string{"VOLUME_READ_DIR_CONCURRENCY_BASE": "4", "VOLUME_READ_DIR_CONCURRENCY_MAX": "3"}},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				for key, value := range test.envs {
					t.Setenv(key, value)
				}

				_, err := Parse()
				require.Error(t, err)
				assert.ErrorContains(t, err, "VOLUME_READ_DIR_CONCURRENCY")
			})
		}
	})
}

func TestAdditionalClickhouseEndpoints(t *testing.T) {
	t.Run("returns nil when neither var is set", func(t *testing.T) {
		c := Config{}
		endpoints, dropped := c.AdditionalClickhouseEndpoints()
		assert.Nil(t, endpoints)
		assert.Nil(t, dropped)
	})

	t.Run("singular only — returns nil", func(t *testing.T) {
		c := Config{ClickhouseConnectionString: "A"}
		endpoints, dropped := c.AdditionalClickhouseEndpoints()
		assert.Nil(t, endpoints)
		assert.Nil(t, dropped)
	})

	t.Run("plural only", func(t *testing.T) {
		c := Config{ClickhouseConnectionStrings: []string{"B", "C"}}
		endpoints, dropped := c.AdditionalClickhouseEndpoints()
		assert.Equal(t, []string{"B", "C"}, endpoints)
		assert.Nil(t, dropped)
	})

	t.Run("singular filters matching plural entry, reported as dropped", func(t *testing.T) {
		c := Config{
			ClickhouseConnectionString:  "A",
			ClickhouseConnectionStrings: []string{"A", "B"},
		}
		endpoints, dropped := c.AdditionalClickhouseEndpoints()
		assert.Equal(t, []string{"B"}, endpoints)
		assert.Equal(t, []string{"A"}, dropped)
	})

	t.Run("plural internal duplicates deduped, reported as dropped", func(t *testing.T) {
		c := Config{ClickhouseConnectionStrings: []string{"A", "B", "A"}}
		endpoints, dropped := c.AdditionalClickhouseEndpoints()
		assert.Equal(t, []string{"A", "B"}, endpoints)
		assert.Equal(t, []string{"A"}, dropped)
	})

	t.Run("blank/whitespace entries dropped silently (not reported)", func(t *testing.T) {
		c := Config{ClickhouseConnectionStrings: []string{"A", "", "  ", "B"}}
		endpoints, dropped := c.AdditionalClickhouseEndpoints()
		assert.Equal(t, []string{"A", "B"}, endpoints)
		assert.Nil(t, dropped)
	})

	t.Run("whitespace-trimmed duplicate of singular dropped, reported", func(t *testing.T) {
		c := Config{
			ClickhouseConnectionString:  "A",
			ClickhouseConnectionStrings: []string{"  A  ", "B"},
		}
		endpoints, dropped := c.AdditionalClickhouseEndpoints()
		assert.Equal(t, []string{"B"}, endpoints)
		assert.Equal(t, []string{"A"}, dropped)
	})

	t.Run("env-var parse: basic split", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_CONNECTION_STRING", "")
		t.Setenv("CLICKHOUSE_CONNECTION_STRINGS", "dsn1;dsn2;dsn3")
		config, err := Parse()
		require.NoError(t, err)
		endpoints, dropped := config.AdditionalClickhouseEndpoints()
		assert.Equal(t, []string{"dsn1", "dsn2", "dsn3"}, endpoints)
		assert.Nil(t, dropped)
	})

	t.Run("env-var parse: empty tokens and trailing/leading separators", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_CONNECTION_STRING", "")
		t.Setenv("CLICKHOUSE_CONNECTION_STRINGS", ";dsn1;;dsn2;")
		config, err := Parse()
		require.NoError(t, err)
		endpoints, dropped := config.AdditionalClickhouseEndpoints()
		assert.Equal(t, []string{"dsn1", "dsn2"}, endpoints)
		assert.Nil(t, dropped)
	})

	t.Run("env-var parse: whitespace around entries trimmed", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_CONNECTION_STRING", "")
		t.Setenv("CLICKHOUSE_CONNECTION_STRINGS", " dsn1 ; dsn2 ")
		config, err := Parse()
		require.NoError(t, err)
		endpoints, dropped := config.AdditionalClickhouseEndpoints()
		assert.Equal(t, []string{"dsn1", "dsn2"}, endpoints)
		assert.Nil(t, dropped)
	})

	t.Run("env-var parse: all-blank yields nil", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_CONNECTION_STRING", "")
		t.Setenv("CLICKHOUSE_CONNECTION_STRINGS", ";;  ;;")
		config, err := Parse()
		require.NoError(t, err)
		endpoints, dropped := config.AdditionalClickhouseEndpoints()
		assert.Nil(t, endpoints)
		assert.Nil(t, dropped)
	})

	t.Run("env-var parse: dup of singular reported, dup of earlier plural reported", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_CONNECTION_STRING", "dsn1")
		t.Setenv("CLICKHOUSE_CONNECTION_STRINGS", "dsn1;dsn2;dsn2")
		config, err := Parse()
		require.NoError(t, err)
		endpoints, dropped := config.AdditionalClickhouseEndpoints()
		assert.Equal(t, []string{"dsn2"}, endpoints)
		assert.Equal(t, []string{"dsn1", "dsn2"}, dropped)
	})
}

// TestParseInstanceGroupName pins the variable name rather than the field: it
// is the contract with the deployment that supplies it, and an unset value
// disables the instance group flag context silently.
func TestParseInstanceGroupName(t *testing.T) {
	t.Run("unset", func(t *testing.T) {
		config, err := Parse()
		require.NoError(t, err)

		assert.Empty(t, config.InstanceGroupName)
	})

	t.Run("set", func(t *testing.T) {
		t.Setenv("INSTANCE_GROUP_NAME", "orch-client-pool-region-rig")

		config, err := Parse()
		require.NoError(t, err)

		assert.Equal(t, "orch-client-pool-region-rig", config.InstanceGroupName)
	})
}

func TestParseTLS(t *testing.T) {
	t.Setenv("E2B_TLS_GRPC_MODE", "permissive")
	t.Setenv("E2B_TLS_GRPC_HEALTH_PATHS", "/health")
	t.Setenv("E2B_TLS_PEERS_CLIENT_MODE", "on")
	t.Setenv("E2B_TLS_PEERS_EXPECT", "spiffe://td.example.internal/ns/nodes/sa/node")
	t.Setenv("E2B_TLS_PEERS_SERVER_NAME", "node.td.example.internal")

	config, err := Parse()
	require.NoError(t, err)
	assert.Equal(t, "permissive", config.TLS.GRPC.Mode)
	assert.Equal(t, []string{"/health"}, config.TLS.GRPC.HealthPaths)
	assert.Equal(t, "on", config.TLS.Peers.Mode)
	assert.Equal(t, []string{"spiffe://td.example.internal/ns/nodes/sa/node"}, config.TLS.Peers.Expect)
	assert.Equal(t, "node.td.example.internal", config.TLS.Peers.ServerName)
}

func TestFirecrackerLogLevel(t *testing.T) {
	t.Run("a level is canonicalised", func(t *testing.T) {
		t.Setenv("FIRECRACKER_LOG_LEVEL", "debug")

		config, err := ParseBuilder()
		require.NoError(t, err)
		assert.Equal(t, "Debug", config.FirecrackerLogLevel)
	})

	t.Run("an unknown level is dropped", func(t *testing.T) {
		t.Setenv("FIRECRACKER_LOG_LEVEL", "Verbose; touch /tmp/x")

		config, err := ParseBuilder()
		require.NoError(t, err)
		assert.Empty(t, config.FirecrackerLogLevel)
	})

	t.Run("unset stays unset", func(t *testing.T) {
		config, err := ParseBuilder()
		require.NoError(t, err)
		assert.Empty(t, config.FirecrackerLogLevel)
	})
}

func TestNormalizeFirecrackerLogLevel(t *testing.T) {
	for in, want := range map[string]string{"": "", "off": "Off", "ERROR": "Error", "warning": "Warning", "warn": "Warning", "WARN": "Warning", "info": "Info", "Trace": "Trace"} {
		got, ok := normalizeFirecrackerLogLevel(in)
		assert.True(t, ok, in)
		assert.Equal(t, want, got, in)
	}
	got, ok := normalizeFirecrackerLogLevel(" Warning ")
	assert.True(t, ok)
	assert.Equal(t, "Warning", got, "surrounding whitespace is trimmed")
	for _, in := range []string{"verbose", "Debug --no-seccomp", "Info;", "D"} {
		_, ok := normalizeFirecrackerLogLevel(in)
		assert.False(t, ok, in)
	}
}
