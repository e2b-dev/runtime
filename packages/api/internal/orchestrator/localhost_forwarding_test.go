package orchestrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbtypes "github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

func TestBuildNetworkConfigLocalhostForwarding(t *testing.T) {
	t.Parallel()

	off := false
	network := buildNetworkConfig(&dbtypes.SandboxNetworkConfig{
		Ingress: &dbtypes.SandboxNetworkIngressConfig{ExposeLocalhostPorts: &off},
	}, nil, nil)
	require.NotNil(t, network.GetIngress().ExposeLocalhostPorts)
	assert.False(t, network.GetIngress().GetExposeLocalhostPorts())

	unset := buildNetworkConfig(&dbtypes.SandboxNetworkConfig{Ingress: &dbtypes.SandboxNetworkIngressConfig{}}, nil, nil)
	assert.Nil(t, unset.GetIngress().ExposeLocalhostPorts, "absent must stay absent so envd keeps forwarding")
}

func TestAskNewSandboxForwardingOn(t *testing.T) {
	t.Parallel()

	off := false
	unset := &orchestrator.SandboxNetworkIngressConfig{}
	askNewSandboxForwardingOn(unset)
	require.NotNil(t, unset.ExposeLocalhostPorts, "a snapshot template's off must not carry over")
	assert.True(t, unset.GetExposeLocalhostPorts())

	asked := &orchestrator.SandboxNetworkIngressConfig{ExposeLocalhostPorts: &off}
	askNewSandboxForwardingOn(asked)
	assert.False(t, asked.GetExposeLocalhostPorts(), "an asked-for off is kept")

	askNewSandboxForwardingOn(nil)
}
