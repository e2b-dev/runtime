//go:build linux

package v2

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
)

// Slot indices for tests that create real netns/veth state. The v1 package
// reserves 30000+ for the same purpose and `go test ./...` runs the two
// packages concurrently, so the ranges must not overlap.
var nsTestIdx atomic.Int32

func reserveNSTestIdx(t *testing.T) int {
	t.Helper()

	return 31000 + int(nsTestIdx.Add(1))
}

// newTestHostFirewall opens the singleton host firewall and registers its
// close first, so it runs after every later-registered slot teardown (t.Cleanup
// is LIFO). Closing it while a slot is still registered deliberately preserves
// the table, which would leak that slot's elements into the next test.
func newTestHostFirewall(t *testing.T, config network.Config) *HostFirewall {
	t.Helper()

	return newTestHostFirewallOn(t, "lo", config)
}

func newTestHostFirewallOn(t *testing.T, gw string, config network.Config) *HostFirewall {
	t.Helper()

	hf, err := NewHostFirewall(gw, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = hf.Close() })

	return hf
}

func makeTestSlot(t *testing.T, idx int) *network.Slot {
	t.Helper()
	cfg := network.Config{
		OrchestratorInSandboxIPAddress: "192.0.2.1",
		HyperloopProxyPort:             5010,
		NFSProxyPort:                   5011,
		PortmapperPort:                 5012,
		SandboxTCPFirewallHTTPPort:     5016,
		SandboxTCPFirewallTLSPort:      5017,
		SandboxTCPFirewallOtherPort:    5018,
	}
	slot, err := network.NewSlot("test-key", idx, cfg, network.NewNoopEgressProxy())
	require.NoError(t, err)

	return slot
}

func TestSlotV2_Creation(t *testing.T) {
	t.Parallel()

	slot := makeTestSlot(t, 1)
	sv2 := NewSlotV2(slot)

	assert.Equal(t, 2, sv2.NetworkVersion)
	assert.Equal(t, slot, sv2.Slot)
	assert.Empty(t, sv2.SandboxID)
}

func TestSlotV2_String(t *testing.T) {
	t.Parallel()

	slot := makeTestSlot(t, 5)
	sv2 := NewSlotV2(slot)

	assert.Contains(t, sv2.String(), "idx=5")
}

func TestSlotV2Registry(t *testing.T) {
	t.Parallel()

	reg := NewSlotV2Registry()

	slot1 := makeTestSlot(t, 1)
	sv2_1 := NewSlotV2(slot1)
	sv2_1.SandboxID = "sb-1"

	slot2 := makeTestSlot(t, 2)
	sv2_2 := NewSlotV2(slot2)
	sv2_2.SandboxID = "sb-2"

	// Store
	reg.Store(sv2_1)
	reg.Store(sv2_2)

	// Load
	loaded, ok := reg.Load(1)
	assert.True(t, ok)
	assert.Equal(t, "sb-1", loaded.SandboxID)

	_, ok = reg.Load(99)
	assert.False(t, ok)

	// Delete
	reg.Delete(1)
	_, ok = reg.Load(1)
	assert.False(t, ok)

	// Range
	count := 0
	reg.Range(func(_ int, _ *SlotV2) bool {
		count++

		return true
	})
	assert.Equal(t, 1, count)
}

func TestSlotV2_EmbeddedSlotMethods(t *testing.T) {
	t.Parallel()

	slot := makeTestSlot(t, 3)
	sv2 := NewSlotV2(slot)

	// Access base slot methods through composition
	assert.Equal(t, "e2b-3", sv2.Slot.VethName())
	assert.Equal(t, "ns-3", sv2.Slot.NamespaceID())
	assert.NotNil(t, sv2.Slot.HostIP)
}
