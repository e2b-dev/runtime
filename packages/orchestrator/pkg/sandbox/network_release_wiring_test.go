//go:build linux

package sandbox

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

type factoryNetworkPool struct {
	slot     *network.Slot
	config   *orchestrator.SandboxNetworkConfig
	class    sandboxtypes.EgressClass
	released network.ReleaseNotify
	returns  int
}

func (p *factoryNetworkPool) Get(_ context.Context, config *orchestrator.SandboxNetworkConfig, class sandboxtypes.EgressClass) (*network.Slot, error) {
	p.config, p.class = config, class

	return p.slot, nil
}

func (p *factoryNetworkPool) ReturnAsync(ctx context.Context, slot *network.Slot, released network.ReleaseNotify, _ time.Duration) error {
	p.returns++
	p.released = released
	if slot != p.slot {
		return errors.New("cleanup returned a different slot")
	}

	return released(ctx, slot.HostIPString())
}

func (*factoryNetworkPool) Close(context.Context) error { return nil }

// Execute the factory's shared network-acquisition boundary and the cleanup it
// registers. This does not start a guest or exercise physical network teardown.
func TestFactoryNetworkReleaseKeepsCapturedLifecycle(t *testing.T) {
	t.Parallel()

	old := testMapSandbox(t, "old-lifecycle")
	replacement := testMapSandbox(t, "replacement-lifecycle")
	replacement.Slot = old.Slot
	registry := NewSandboxesMap()
	registry.TrackLifecycle(t.Context(), old)
	registry.AssignNetwork(t.Context(), old)
	failure := errors.New("old subscriber still owns users")
	calls := 0
	registry.Subscribe(&releaseSubscriber{onRelease: func(got *Sandbox) error {
		require.Same(t, old, got, "factory cleanup must notify its original lifecycle, not the IP successor")
		calls++
		if calls == 1 {
			return failure
		}

		return nil
	}})
	pool := &factoryNetworkPool{slot: old.Slot}
	factory := &Factory{Sandboxes: registry, networkPool: pool}
	cleanup := NewCleanup()
	config := NewConfig(Config{Network: &orchestrator.SandboxNetworkConfig{}})
	promise, _, _ := factory.getNetworkSlot(t.Context(), cleanup, config, old.Runtime, old.LifecycleID)
	slot, err := promise.Wait(t.Context())
	require.NoError(t, err)
	require.Same(t, old.Slot, slot)
	require.Same(t, config.Network, pool.config)
	require.Equal(t, old.Runtime.SandboxType.EgressClass(), pool.class)

	registry.AssignNetwork(t.Context(), replacement)
	require.ErrorIs(t, cleanup.Run(t.Context()), failure)
	require.Equal(t, 1, pool.returns)
	require.NotNil(t, pool.released)
	mapped, err := registry.GetByHostPort(net.JoinHostPort(slot.HostIPString(), "80"))
	require.NoError(t, err, "factory's old cleanup must preserve the replacement route")
	require.Same(t, replacement, mapped)

	registry.MarkStopped(t.Context(), old)
	require.NoError(t, pool.released(t.Context(), slot.HostIPString()), "the same returned callback must retry its failed old subscriber")
	require.NoError(t, pool.released(t.Context(), slot.HostIPString()))
	require.Equal(t, 2, calls, "successful old subscriber must not replay or notify the replacement")
	mapped, err = registry.GetByHostPort(net.JoinHostPort(slot.HostIPString(), "80"))
	require.NoError(t, err)
	require.Same(t, replacement, mapped)
}
