package sandboxconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSandboxNetworkConfigHasEgressProxy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		network  *SandboxNetworkConfig
		wantAny  bool
		wantAuth bool
	}{
		{
			name:    "nil config",
			network: nil,
		},
		{
			name:    "nil egress",
			network: &SandboxNetworkConfig{},
		},
		{
			name:    "egress without proxy",
			network: &SandboxNetworkConfig{Egress: &SandboxNetworkEgressConfig{DeniedAddresses: []string{"1.1.1.1"}}},
		},
		{
			name: "proxy without credentials",
			network: &SandboxNetworkConfig{Egress: &SandboxNetworkEgressConfig{
				EgressProxyAddress: "proxy.example.com:1080",
			}},
			wantAny: true,
		},
		{
			name: "proxy with credentials",
			network: &SandboxNetworkConfig{Egress: &SandboxNetworkEgressConfig{
				EgressProxyAddress:  "proxy.example.com:1080",
				EgressProxyUsername: "user",
				EgressProxyPassword: "secret",
			}},
			wantAny:  true,
			wantAuth: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.wantAny, tt.network.HasEgressProxy())
			assert.Equal(t, tt.wantAuth, tt.network.HasEgressProxyAuth())
		})
	}
}
