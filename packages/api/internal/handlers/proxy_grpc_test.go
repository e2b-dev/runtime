package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	dbtypes "github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
)

func TestIsNonEnvdTrafficRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		md       metadata.MD
		expected bool
	}{
		{
			name:     "no port metadata returns true",
			md:       metadata.MD{},
			expected: true,
		},
		{
			name:     "envd port returns false",
			md:       metadata.Pairs(proxygrpc.MetadataSandboxRequestPort, "49983"),
			expected: false,
		},
		{
			name:     "non-envd port returns true",
			md:       metadata.Pairs(proxygrpc.MetadataSandboxRequestPort, "8080"),
			expected: true,
		},
		{
			name:     "invalid port string returns true",
			md:       metadata.Pairs(proxygrpc.MetadataSandboxRequestPort, "not-a-number"),
			expected: true,
		},
		{
			name:     "empty port string returns true",
			md:       metadata.Pairs(proxygrpc.MetadataSandboxRequestPort, ""),
			expected: true,
		},
		{
			name:     "port 0 returns true",
			md:       metadata.Pairs(proxygrpc.MetadataSandboxRequestPort, "0"),
			expected: true,
		},
		{
			name:     "port 443 returns true",
			md:       metadata.Pairs(proxygrpc.MetadataSandboxRequestPort, "443"),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := isNonEnvdTrafficRequest(t.Context(), tt.md, "test-sandbox")
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsPrivateIngressTraffic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		network  *dbtypes.SandboxNetworkConfig
		expected bool
	}{
		{
			name:     "nil network",
			network:  nil,
			expected: false,
		},
		{
			name:     "nil ingress",
			network:  &dbtypes.SandboxNetworkConfig{Ingress: nil},
			expected: false,
		},
		{
			name:     "nil AllowPublicAccess",
			network:  &dbtypes.SandboxNetworkConfig{Ingress: &dbtypes.SandboxNetworkIngressConfig{AllowPublicAccess: nil}},
			expected: false,
		},
		{
			name:     "AllowPublicAccess true",
			network:  &dbtypes.SandboxNetworkConfig{Ingress: &dbtypes.SandboxNetworkIngressConfig{AllowPublicAccess: new(true)}},
			expected: false,
		},
		{
			name:     "AllowPublicAccess false",
			network:  &dbtypes.SandboxNetworkConfig{Ingress: &dbtypes.SandboxNetworkIngressConfig{AllowPublicAccess: new(false)}},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := isPrivateIngressTraffic(tt.network)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTokensMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		provided string
		expected string
		match    bool
	}{
		{
			name:     "identical tokens match",
			provided: "secret-token-123",
			expected: "secret-token-123",
			match:    true,
		},
		{
			name:     "different tokens do not match",
			provided: "wrong-token",
			expected: "secret-token-123",
			match:    false,
		},
		{
			name:     "empty provided does not match",
			provided: "",
			expected: "secret-token-123",
			match:    false,
		},
		{
			name:     "empty expected does not match",
			provided: "secret-token-123",
			expected: "",
			match:    false,
		},
		{
			name:     "both empty match",
			provided: "",
			expected: "",
			match:    true,
		},
		{
			name:     "different length tokens do not match",
			provided: "short",
			expected: "much-longer-token-value",
			match:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := tokensMatch(tt.provided, tt.expected)
			assert.Equal(t, tt.match, result)
		})
	}
}
