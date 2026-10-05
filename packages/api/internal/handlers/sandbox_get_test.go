package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
)

// The read-back is the one place stored proxy configuration travels back to
// the caller, so what it omits matters as much as what it returns.
func TestDBNetworkConfigToAPI_EgressProxy(t *testing.T) {
	t.Parallel()

	t.Run("credentials and CA bundle are withheld", func(t *testing.T) {
		t.Parallel()

		result := dbNetworkConfigToAPI(&dbtypes.SandboxNetworkConfig{
			Egress: &dbtypes.SandboxNetworkEgressConfig{
				EgressProxyAddress:  "proxy.example.com:1080",
				EgressProxyUsername: "alice",
				EgressProxyPassword: "s3cret",
				EgressProxyTLS: &dbtypes.SandboxEgressProxyTLSConfig{
					Enabled:    true,
					ServerName: "cert-name.test",
					CACert:     "-----BEGIN CERTIFICATE-----\nstub\n-----END CERTIFICATE-----",
				},
			},
		})

		require.NotNil(t, result.EgressProxy)
		assert.Equal(t, "proxy.example.com:1080", result.EgressProxy.Address)
		require.NotNil(t, result.EgressProxy.Username)
		assert.Equal(t, "alice", *result.EgressProxy.Username)
		assert.Nil(t, result.EgressProxy.Password, "the proxy password must never be read back")

		require.NotNil(t, result.EgressProxy.Tls)
		assert.True(t, result.EgressProxy.Tls.Enabled)
		require.NotNil(t, result.EgressProxy.Tls.ServerName)
		assert.Equal(t, "cert-name.test", *result.EgressProxy.Tls.ServerName)
		assert.Nil(t, result.EgressProxy.Tls.CaCert,
			"the bundle is public material but multi-kilobyte, and the caller supplied it")
	})

	t.Run("a plaintext proxy reports no TLS", func(t *testing.T) {
		t.Parallel()

		result := dbNetworkConfigToAPI(&dbtypes.SandboxNetworkConfig{
			Egress: &dbtypes.SandboxNetworkEgressConfig{
				EgressProxyAddress: "proxy.example.com:1080",
			},
		})

		require.NotNil(t, result.EgressProxy)
		assert.Nil(t, result.EgressProxy.Tls)
	})

	t.Run("no proxy reports nothing", func(t *testing.T) {
		t.Parallel()

		result := dbNetworkConfigToAPI(&dbtypes.SandboxNetworkConfig{
			Egress: &dbtypes.SandboxNetworkEgressConfig{},
		})

		assert.Nil(t, result.EgressProxy)
	})
}
