package sandbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func TestEnvdInitRequestCarriesExposeLocalhostPorts(t *testing.T) {
	t.Parallel()

	off, on := false, true
	withIngress := func(ingress *orchestrator.SandboxNetworkIngressConfig) *Sandbox {
		s := newTestGuestSandbox(nil)
		s.Config.Network = &orchestrator.SandboxNetworkConfig{Ingress: ingress}
		s.egressProxy = &mockEgressProxy{}

		return s
	}
	asked := withIngress(&orchestrator.SandboxNetworkIngressConfig{ExposeLocalhostPorts: &off})

	tests := map[string]struct {
		sandbox     *Sandbox
		envdVersion string
		sent        *bool
	}{
		"an envd that applies it gets the setting":   {sandbox: asked, envdVersion: utils.MinEnvdVersionForExposeLocalhostPorts, sent: &off},
		"a newer envd gets it too":                   {sandbox: asked, envdVersion: "0.99.0", sent: &off},
		"on is sent as on":                           {sandbox: withIngress(&orchestrator.SandboxNetworkIngressConfig{ExposeLocalhostPorts: &on}), envdVersion: "0.99.0", sent: &on},
		"an older envd keeps forwarding":             {sandbox: asked, envdVersion: "0.11.0"},
		"an unknown version is treated as older":     {sandbox: asked, envdVersion: ""},
		"a sandbox that asked nothing sends nothing": {sandbox: withIngress(&orchestrator.SandboxNetworkIngressConfig{}), envdVersion: "0.99.0"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			sent := test.sandbox.newEnvdInitRequest("http://envd/init", test.envdVersion).body.ExposeLocalhostPorts
			if test.sent == nil {
				assert.Nil(t, sent)

				return
			}
			require.NotNil(t, sent)
			assert.Equal(t, *test.sent, *sent)
		})
	}
}
