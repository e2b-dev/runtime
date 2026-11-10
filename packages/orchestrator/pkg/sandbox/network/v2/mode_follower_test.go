//go:build linux

package v2

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// onceStorage hands out one prepared slot, then parks Populate until its
// context ends.
type onceStorage struct {
	slot  *network.Slot
	taken atomic.Bool
}

func (s *onceStorage) Acquire(ctx context.Context) (*network.Slot, error) {
	if s.taken.CompareAndSwap(false, true) {
		return s.slot, nil
	}
	<-ctx.Done()

	return nil, ctx.Err()
}

func (s *onceStorage) Release(*network.Slot) error { return nil }

// A flag change reaches a v2 slot a tenant holds: the v2 pool follows the
// flag like the v1 pool and rebuilds the chain without a release.
func TestV2Pool_NonDNSUDPFlagChangeReachesHandedOutSlot(t *testing.T) { //nolint:paralleltest // mutates host netns state: singleton nftables table "v2-host-firewall", named netns, veth links
	skipIfNotLinuxRoot(t)

	hf := newTestHostFirewall(t, testConfig())
	ff, td := nonDNSUDPFlagClient(t, network.NonDNSUDPModeDrop)
	slot := makeTestSlot(t, reserveNSTestIdx(t))
	pool := NewV2Pool(&onceStorage{slot: slot}, testConfig(), hf, nil, WithNonDNSUDPFlag(ff))

	ctx, cancel := context.WithCancel(t.Context())
	populated := make(chan struct{})
	go func() {
		defer close(populated)
		pool.Populate(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-populated
	})

	got, err := pool.Get(t.Context(), &orchestrator.SandboxNetworkConfig{}, sandboxtypes.EgressClassSandbox)
	require.NoError(t, err)
	require.Same(t, slot, got)
	t.Cleanup(func() { _ = pool.cleanup(context.WithoutCancel(t.Context()), got) })
	require.Equal(t, network.NonDNSUDPModeDrop, got.Firewall.NonDNSUDPMode(), "handed out under drop")
	counted, dropped := findNonDNSUDPRule(slotFilterChainRules(t, got))
	require.True(t, counted && dropped)

	setNonDNSUDPFlag(td, network.NonDNSUDPModeOff)
	require.Eventually(t, func() bool { return got.Firewall.NonDNSUDPMode() == network.NonDNSUDPModeOff },
		10*time.Second, 10*time.Millisecond, "the slot did not reach mode off")
	counted, _ = findNonDNSUDPRule(slotFilterChainRules(t, got))
	require.False(t, counted, "the rule is gone without a release")
}
