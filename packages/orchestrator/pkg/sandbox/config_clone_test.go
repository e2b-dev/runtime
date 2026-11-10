package sandbox

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// A clone carries the network config as it was at clone time and its own lock;
// updates to either side afterwards do not cross over.
func TestConfigClone_IsolatesNetwork(t *testing.T) {
	t.Parallel()

	src := NewConfig(Config{Network: &orchestrator.SandboxNetworkConfig{
		Egress: &orchestrator.SandboxNetworkEgressConfig{AllowedDomains: []string{"a.example"}},
	}})

	clone := src.Clone()
	src.SetNetworkEgress(&orchestrator.SandboxNetworkEgressConfig{AllowedDomains: []string{"b.example"}})
	clone.SetNetworkEgress(&orchestrator.SandboxNetworkEgressConfig{AllowedDomains: []string{"c.example"}})

	require.Equal(t, []string{"b.example"}, src.GetNetworkEgress().GetAllowedDomains())
	require.Equal(t, []string{"c.example"}, clone.GetNetworkEgress().GetAllowedDomains())
	require.NotSame(t, src.Network, clone.Network)
}

// Cloning while another goroutine rewrites the egress must be race-free; the
// race detector is the assertion here.
func TestConfigClone_ConcurrentWithEgressUpdate(t *testing.T) {
	t.Parallel()

	src := NewConfig(Config{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 500 {
			src.SetNetworkEgress(&orchestrator.SandboxNetworkEgressConfig{AllowedDomains: []string{string(rune('a' + i%26))}})
		}
	}()
	go func() {
		defer wg.Done()
		for range 500 {
			c := src.Clone()
			_ = c.GetNetworkEgress().GetAllowedDomains()
		}
	}()
	wg.Wait()
}

// A nil network on the source still yields a usable, non-nil network on the
// clone, matching NewConfig's normalisation.
func TestConfigClone_NilNetworkNormalised(t *testing.T) {
	t.Parallel()

	src := NewConfig(Config{})
	require.NotNil(t, src.Clone().Network)
}
