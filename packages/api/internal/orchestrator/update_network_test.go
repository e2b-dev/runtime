package orchestrator

import (
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	sandbox_network "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-network"
)

// Node versions as orchestrator-ee reports them: the build that added
// egress_proxy_tls, and the one just before it.
const (
	buildWithEgressProxyTLS   = "0.16.202610011012-6bbb0090f80-ee"
	buildBeforeEgressProxyTLS = "0.16.202610011007-5dfa8dfb8aa-ee"
)

func addRunningSandbox(t *testing.T, o *Orchestrator) sandbox.Sandbox {
	t.Helper()

	return addSandboxInState(t, o, sandbox.StateRunning, "node-1")
}

func addSandboxInState(t *testing.T, o *Orchestrator, state sandbox.State, nodeID string) sandbox.Sandbox {
	t.Helper()

	sbx := sandbox.Sandbox{
		SandboxID:         "sbx-" + uuid.NewString()[:8],
		TemplateID:        "tpl-1",
		ExecutionID:       uuid.NewString(),
		TeamID:            uuid.New(),
		BuildID:           uuid.New(),
		MaxInstanceLength: time.Hour,
		StartTime:         time.Now(),
		EndTime:           time.Now().Add(time.Hour),
		VCpu:              2,
		RamMB:             512,
		NodeID:            nodeID,
		ClusterID:         uuid.Nil,
		State:             state,
	}
	require.NoError(t, o.sandboxStore.Add(t.Context(), sbx, nil))

	return sbx
}

func tlsProxy() *sandbox_network.EgressProxyConfig {
	return &sandbox_network.EgressProxyConfig{
		Address: "proxy.example.com:1080",
		TLS:     &sandbox_network.EgressProxyTLSConfig{Enabled: true},
	}
}

// A live network update never goes through placement, so the node the sandbox
// already runs on has to be checked for the capability instead.
func TestUpdateSandboxNetworkConfig_EgressProxyTLSGate(t *testing.T) {
	t.Parallel()

	t.Run("old node refuses tls and stores nothing", func(t *testing.T) {
		t.Parallel()

		o := newCreateSandboxTestOrchestrator(t, nodemanager.WithOrchestratorVersion(buildBeforeEgressProxyTLS))
		sbx := addRunningSandbox(t, o)

		apiErr := o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID, nil, nil, nil, nil, tlsProxy())
		require.NotNil(t, apiErr, "a node that would drop the TLS block must not be sent it")
		assert.Equal(t, http.StatusServiceUnavailable, apiErr.Code)
		assert.Equal(t, errCodeFeatureUnsupported, apiErr.ErrorCode)

		stored, err := o.sandboxStore.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
		require.NoError(t, err)
		assert.False(t, stored.Network.HasEgressProxyTLS(),
			"a refused config must not be stored, or GET would report TLS the hop does not use")
	})

	t.Run("new node accepts tls", func(t *testing.T) {
		t.Parallel()

		o := newCreateSandboxTestOrchestrator(t, nodemanager.WithOrchestratorVersion(buildWithEgressProxyTLS))
		sbx := addRunningSandbox(t, o)

		apiErr := o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID, nil, nil, nil, nil, tlsProxy())
		require.Nil(t, apiErr)

		stored, err := o.sandboxStore.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
		require.NoError(t, err)
		assert.True(t, stored.Network.HasEgressProxyTLS())
	})

	t.Run("old node still accepts a proxy without tls", func(t *testing.T) {
		t.Parallel()

		o := newCreateSandboxTestOrchestrator(t, nodemanager.WithOrchestratorVersion(buildBeforeEgressProxyTLS))
		sbx := addRunningSandbox(t, o)

		apiErr := o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID, nil, nil, nil, nil,
			&sandbox_network.EgressProxyConfig{Address: "proxy.example.com:1080"})
		require.Nil(t, apiErr, "the gate applies only to configs that ask for TLS")
	})

	t.Run("sandbox that is not running refuses tls and stores nothing", func(t *testing.T) {
		t.Parallel()

		o := newCreateSandboxTestOrchestrator(t, nodemanager.WithOrchestratorVersion(buildBeforeEgressProxyTLS))
		sbx := addSandboxInState(t, o, sandbox.StateSnapshotting, "node-1")

		apiErr := o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID, nil, nil, nil, nil, tlsProxy())
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusConflict, apiErr.Code)

		stored, err := o.sandboxStore.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
		require.NoError(t, err)
		assert.False(t, stored.Network.HasEgressProxyTLS())
	})

	t.Run("node missing from the cache refuses tls and stores nothing", func(t *testing.T) {
		t.Parallel()

		o := newCreateSandboxTestOrchestrator(t, nodemanager.WithOrchestratorVersion(buildWithEgressProxyTLS))
		sbx := addSandboxInState(t, o, sandbox.StateRunning, "node-unknown")

		apiErr := o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID, nil, nil, nil, nil, tlsProxy())
		require.NotNil(t, apiErr, "a node whose version cannot be read must not be sent TLS")
		assert.Equal(t, http.StatusServiceUnavailable, apiErr.Code)

		stored, err := o.sandboxStore.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
		require.NoError(t, err)
		assert.False(t, stored.Network.HasEgressProxyTLS())
	})

	t.Run("missing sandbox is not found", func(t *testing.T) {
		t.Parallel()

		o := newCreateSandboxTestOrchestrator(t, nodemanager.WithOrchestratorVersion(buildWithEgressProxyTLS))

		apiErr := o.UpdateSandboxNetworkConfig(t.Context(), uuid.New(), "sbx-missing", nil, nil, nil, nil, tlsProxy())
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusNotFound, apiErr.Code)
	})
}
