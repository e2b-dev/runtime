package nodemanager

import (
	"testing"

	"github.com/stretchr/testify/require"

	sandbox_network "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-network"
)

func TestLegacyProxyEndpointAdmission(t *testing.T) {
	t.Parallel()
	node := &Node{}
	const address = "10.20.0.5:1080"
	require.ErrorIs(t, node.ValidateLegacyProxyEndpoint(t.Context(), address), sandbox_network.ErrEgressProxyInternalEndpoint)
	node.setCapabilities([]string{sandbox_network.CapabilityProxyEndpointAdmission})
	require.NoError(t, node.ValidateLegacyProxyEndpoint(t.Context(), address))
}
