package cfg

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
)

func TestParse_Defaults(t *testing.T) {
	t.Setenv("HEALTH_PORT", "")
	t.Setenv("PROXY_PORT", "")
	t.Setenv("ORCHESTRATOR_PROXY_PORT", "")
	t.Setenv("REDIS_URL", "")
	t.Setenv("REDIS_CLUSTER_URL", "")
	t.Setenv("REDIS_TLS_CA_BASE64", "")
	t.Setenv("REDIS_POOL_SIZE", "")
	t.Setenv("API_INTERNAL_GRPC_ADDRESS", "")
	t.Setenv("API_EDGE_GRPC_ADDRESS", "")
	t.Setenv("API_EDGE_GRPC_OAUTH_CLIENT_ID", "")
	t.Setenv("API_EDGE_GRPC_OAUTH_CLIENT_SECRET", "")
	t.Setenv("API_EDGE_GRPC_OAUTH_TOKEN_URL", "")

	cfg, err := Parse()
	require.NoError(t, err)
	require.EqualValues(t, 3003, cfg.HealthPort)
	require.EqualValues(t, 3002, cfg.ProxyPort)
	require.EqualValues(t, 5007, cfg.OrchestratorProxyPort)
	require.Equal(t, 40, cfg.RedisPoolSize)
	require.Empty(t, cfg.RedisURL)
	require.Empty(t, cfg.RedisClusterURL)
	require.Empty(t, cfg.RedisTLSCABase64)
	require.Empty(t, cfg.APIInternalGRPCAddress)
	require.Empty(t, cfg.APIEdgeGRPCAddress)
	require.Empty(t, cfg.APIEdgeGRPCOAuthClientID)
	require.Empty(t, cfg.APIEdgeGRPCOAuthClientSecret)
	require.Empty(t, cfg.APIEdgeGRPCOAuthTokenURL)
}

func TestParse_OverridesFromEnv(t *testing.T) {
	t.Setenv("HEALTH_PORT", "9001")
	t.Setenv("PROXY_PORT", "9002")
	t.Setenv("ORCHESTRATOR_PROXY_PORT", "25007")
	t.Setenv("REDIS_URL", "redis://localhost:6379")
	t.Setenv("REDIS_CLUSTER_URL", "redis://cluster:6379")
	t.Setenv("REDIS_TLS_CA_BASE64", "Y2EtZGF0YQ==")
	t.Setenv("REDIS_POOL_SIZE", "12")
	t.Setenv("API_INTERNAL_GRPC_ADDRESS", "internal:5005")
	t.Setenv("API_EDGE_GRPC_ADDRESS", "edge:5006")
	t.Setenv("API_EDGE_GRPC_OAUTH_CLIENT_ID", "client-id")
	t.Setenv("API_EDGE_GRPC_OAUTH_CLIENT_SECRET", "client-secret")
	t.Setenv("API_EDGE_GRPC_OAUTH_TOKEN_URL", "https://tokens.example.com")

	cfg, err := Parse()
	require.NoError(t, err)
	require.EqualValues(t, 9001, cfg.HealthPort)
	require.EqualValues(t, 9002, cfg.ProxyPort)
	require.EqualValues(t, 25007, cfg.OrchestratorProxyPort)
	require.Equal(t, "redis://localhost:6379", cfg.RedisURL)
	require.Equal(t, "redis://cluster:6379", cfg.RedisClusterURL)
	require.Equal(t, "Y2EtZGF0YQ==", cfg.RedisTLSCABase64)
	require.Equal(t, 12, cfg.RedisPoolSize)
	require.Equal(t, "internal:5005", cfg.APIInternalGRPCAddress)
	require.Equal(t, "edge:5006", cfg.APIEdgeGRPCAddress)
	require.Equal(t, "client-id", cfg.APIEdgeGRPCOAuthClientID)
	require.Equal(t, "client-secret", cfg.APIEdgeGRPCOAuthClientSecret)
	require.Equal(t, "https://tokens.example.com", cfg.APIEdgeGRPCOAuthTokenURL)
}

func TestParse_InvalidIntegerReturnsError(t *testing.T) {
	t.Setenv("HEALTH_PORT", "not-a-number")

	_, err := Parse()
	require.Error(t, err)
}

func TestParse_InvalidOrchestratorPortReturnsError(t *testing.T) {
	for _, value := range []string{"0", "-1", "65536", "invalid"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("ORCHESTRATOR_PROXY_PORT", value)
			_, err := Parse()
			require.Error(t, err)
		})
	}
}

func TestParse_TLS(t *testing.T) {
	t.Setenv("E2B_TLS_API_CLIENT_MODE", "on")
	t.Setenv("E2B_TLS_API_EXPECT", "spiffe://td.example.internal/ns/control/sa/api")
	t.Setenv("E2B_TLS_API_SERVER_NAME", "api.example.internal")
	t.Setenv("E2B_TLS_ROUTER_CLIENT_MODE", "on")
	t.Setenv("E2B_TLS_ROUTER_EXPECT", "spiffe://td.example.internal/ns/router/sa/router")
	t.Setenv("E2B_TLS_ROUTER_SERVER_NAME", "")

	cfg, err := Parse()
	require.NoError(t, err)
	require.Equal(t, "on", cfg.TLS.API.Mode)
	require.Equal(t, []string{"spiffe://td.example.internal/ns/control/sa/api"}, cfg.TLS.API.Expect)
	require.Equal(t, "api.example.internal", cfg.TLS.API.ServerName)
	require.Equal(t, mtls.HopSettings{Mode: "on", Expect: []string{"spiffe://td.example.internal/ns/router/sa/router"}}, cfg.TLS.Router)
}
