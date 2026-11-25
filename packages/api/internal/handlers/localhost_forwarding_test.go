package handlers

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	dbtypes "github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func TestSandboxNetworkConfigLocalhostForwarding(t *testing.T) {
	t.Parallel()

	t.Run("off is persisted and survives the stored form", func(t *testing.T) {
		t.Parallel()

		off := false
		got := sandboxNetworkConfig(&api.SandboxNetworkConfig{ExposeLocalhostPorts: &off}, dbtypes.SandboxSettings{})
		require.NotNil(t, got)
		require.NotNil(t, got.Ingress)

		stored, err := json.Marshal(got)
		require.NoError(t, err)
		var resumed dbtypes.SandboxNetworkConfig
		require.NoError(t, json.Unmarshal(stored, &resumed))
		require.NotNil(t, resumed.Ingress)
		require.NotNil(t, resumed.Ingress.ExposeLocalhostPorts, "a resume must read back what the create stored")
		assert.False(t, *resumed.Ingress.ExposeLocalhostPorts)
	})

	t.Run("absent stays absent", func(t *testing.T) {
		t.Parallel()

		got := sandboxNetworkConfig(&api.SandboxNetworkConfig{}, dbtypes.SandboxSettings{})
		require.NotNil(t, got)
		require.NotNil(t, got.Ingress)
		assert.Nil(t, got.Ingress.ExposeLocalhostPorts)
	})
}

func TestDBNetworkConfigToAPILocalhostForwarding(t *testing.T) {
	t.Parallel()

	off := false
	result := dbNetworkConfigToAPI(&dbtypes.SandboxNetworkConfig{
		Ingress: &dbtypes.SandboxNetworkIngressConfig{ExposeLocalhostPorts: &off},
	})

	require.NotNil(t, result)
	require.NotNil(t, result.ExposeLocalhostPorts)
	assert.False(t, *result.ExposeLocalhostPorts)
}

func TestGateLocalhostForwarding(t *testing.T) {
	t.Parallel()

	off, on := false, true
	withForwarding := func(v *bool) *dbtypes.SandboxNetworkConfig {
		return &dbtypes.SandboxNetworkConfig{Ingress: &dbtypes.SandboxNetworkIngressConfig{ExposeLocalhostPorts: v}}
	}

	require.Nil(t, gateLocalhostForwarding(withForwarding(&off), true), "enabled keeps an asked-for off")
	require.Nil(t, gateLocalhostForwarding(nil, false))

	apiErr := gateLocalhostForwarding(withForwarding(&off), false)
	require.NotNil(t, apiErr, "a disabled control refuses off rather than store a request a node may ignore")
	assert.Equal(t, http.StatusBadRequest, apiErr.Code)

	dropped := withForwarding(&on)
	require.Nil(t, gateLocalhostForwarding(dropped, false))
	assert.Nil(t, dropped.Ingress.ExposeLocalhostPorts, "a disabled control sends nothing new")
}

func TestRequireEnvdForExposeLocalhostPortsOff(t *testing.T) {
	t.Parallel()

	off, on := false, true
	tests := map[string]struct {
		requested   *api.SandboxNetworkConfig
		envdVersion string
		refused     bool
	}{
		"off on an envd that applies it":               {requested: &api.SandboxNetworkConfig{ExposeLocalhostPorts: &off}, envdVersion: sharedUtils.MinEnvdVersionForExposeLocalhostPorts},
		"off on an older envd is refused":              {requested: &api.SandboxNetworkConfig{ExposeLocalhostPorts: &off}, envdVersion: "0.11.1", refused: true},
		"off on a build without an envd version":       {requested: &api.SandboxNetworkConfig{ExposeLocalhostPorts: &off}, envdVersion: "", refused: true},
		"on on an older envd carries on":               {requested: &api.SandboxNetworkConfig{ExposeLocalhostPorts: &on}, envdVersion: "0.11.1"},
		"nothing asked on an older envd carries on":    {requested: &api.SandboxNetworkConfig{}, envdVersion: "0.11.1"},
		"no network block on an older envd carries on": {requested: nil, envdVersion: "0.11.1"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			apiErr := requireEnvdForExposeLocalhostPortsOff(test.requested, test.envdVersion)
			if !test.refused {
				assert.Nil(t, apiErr)

				return
			}
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Code)
			assert.Contains(t, apiErr.ClientMsg, "template must be rebuilt")
		})
	}
}
