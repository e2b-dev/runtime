//go:build linux

package network

import (
	"context"
	"net/netip"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/nftables"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// nonDNSUDPFlagSource serves the flag from an offline data source the test
// updates at will, through a client built like the orchestrator's.
func nonDNSUDPFlagSource(t *testing.T, value NonDNSUDPMode) (*featureflags.Client, *ldtestdata.TestDataSource) {
	t.Helper()

	td := ldtestdata.DataSource()
	setNonDNSUDPFlag(td, value)
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	return ff, td
}

func setNonDNSUDPFlag(td *ldtestdata.TestDataSource, value NonDNSUDPMode) {
	td.Update(td.Flag(featureflags.SandboxFirewallNonDNSUDPFlag.Key()).ValueForAll(ldvalue.String(string(value))))
}

// parkedStorage produces no slot: Populate waits in it until its context
// ends, so a test drives the pool with slots it placed itself while the
// follower Populate starts runs.
type parkedStorage struct{}

func (parkedStorage) Acquire(ctx context.Context) (*Slot, error) {
	<-ctx.Done()

	return nil, ctx.Err()
}

func (parkedStorage) Release(*Slot) error { return nil }

// populate runs Populate until the test ends.
func populate(t *testing.T, pool *Pool) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.Populate(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// offerReused places a slot in the reuse queue as a return would, with the
// pool's provider the slot would have received at creation.
func offerReused(t *testing.T, pool *Pool, slot *Slot) {
	t.Helper()

	slot.UseNonDNSUDPMode(pool.modeFollower.Provider())
	pool.reusedSlots <- slot
	reusableSlotsAvailableCounter.Add(t.Context(), 1)
}

func awaitNonDNSUDPMode(t *testing.T, slot *Slot, mode NonDNSUDPMode) {
	t.Helper()

	require.Eventuallyf(t, func() bool { return slot.Firewall.NonDNSUDPMode() == mode },
		10*time.Second, 10*time.Millisecond, "the slot did not reach mode %s", mode)
}

// A flag change reaches a slot a tenant holds: the pool follows the flag and
// rebuilds the chain in the new mode without a release, in both directions,
// and what each chain counted is exported under it at the end.
func TestPool_NonDNSUDPFlagChangeReachesHandedOutSlot(t *testing.T) { //nolint:paralleltest // replaces the package counter and builds netns state; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + nftables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	idx := reserveNSTestIdx(t)
	h := newUDPHarness(t, idx)
	reader := swapNonDNSUDPCounter(t)
	ff, td := nonDNSUDPFlagSource(t, NonDNSUDPModeDrop)

	// A pooled slot built before the flag was set.
	fw, err := NewFirewall(h.fwNSPath(), h.tap, config.OrchestratorInSandboxIPAddress, nil, nil, NonDNSUDPModeOff)
	require.NoError(t, err)
	slot := &Slot{Idx: idx, Firewall: fw, config: config}
	t.Cleanup(func() { _ = slot.CloseFirewall(context.WithoutCancel(t.Context())) })

	pool := NewPool(NewSlotsPoolSize, ReusedSlotsPoolSize, parkedStorage{}, config, WithNonDNSUDPFlag(ff))
	offerReused(t, pool, slot)
	populate(t, pool)

	got, err := pool.Get(t.Context(), &orchestrator.SandboxNetworkConfig{}, sandboxtypes.EgressClassSandbox)
	require.NoError(t, err)
	require.Same(t, slot, got)
	require.Equal(t, NonDNSUDPModeDrop, slot.Firewall.NonDNSUDPMode(), "handed out under drop")
	sendNonDNSUDP(t, h, 3)

	setNonDNSUDPFlag(td, NonDNSUDPModeOff)
	awaitNonDNSUDPMode(t, slot, NonDNSUDPModeOff)
	require.Equal(t, defaultChain, filterChainShapes(t, h.fwNS), "the rule is gone without a release")
	sendNonDNSUDP(t, h, 2)

	setNonDNSUDPFlag(td, NonDNSUDPModeCount)
	awaitNonDNSUDPMode(t, slot, NonDNSUDPModeCount)
	require.Equal(t, []ruleShape{nonDNSUDPCount, establishedNonUDPAccept, establishedReplyAccept, allowSetAccept, denySetDrop, nonTCPUserAllow, nonTCPUserDeny},
		filterChainShapes(t, h.fwNS), "the rule is back without a release")
	sendNonDNSUDP(t, h, 2)

	require.NoError(t, slot.CloseFirewall(t.Context()))
	require.Equal(t, map[ChainDescriptor]int64{
		{Mode: NonDNSUDPModeDrop, Posture: postureDefault}:  3,
		{Mode: NonDNSUDPModeCount, Posture: postureDefault}: 2,
	}, nonDNSUDPPointsByChain(t, reader), "the rebuilds lost nothing: each chain's datagrams are exported under it")
}

// The mode is re-evaluated per slot with its tenant's own flag contexts: a
// change targeting one team reaches that team's slot and leaves the other
// slot's chain untouched, and a change of the fallthrough reaches the
// untargeted slot only.
func TestPool_NonDNSUDPFlagChangeHonoursTeamTargetingPerSlot(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	targeted := newFirewallTestSlot(t, "fw-non-dns-udp-team")
	other := newFirewallTestSlot(t, "fw-non-dns-udp-other")

	const teamID = "team-targeted"
	variations := []NonDNSUDPMode{NonDNSUDPModeOff, NonDNSUDPModeCount, NonDNSUDPModeDrop}
	td := ldtestdata.DataSource()
	target := func(team, everyone NonDNSUDPMode) {
		td.Update(td.Flag(featureflags.SandboxFirewallNonDNSUDPFlag.Key()).
			Variations(ldvalue.String("off"), ldvalue.String("count"), ldvalue.String("drop")).
			FallthroughVariationIndex(slices.Index(variations, everyone)).
			VariationIndexForKey(featureflags.TeamKind, teamID, slices.Index(variations, team)))
	}
	target(NonDNSUDPModeDrop, NonDNSUDPModeOff)
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	pool := NewPool(NewSlotsPoolSize, ReusedSlotsPoolSize, parkedStorage{}, targeted.config, WithNonDNSUDPFlag(ff))
	offerReused(t, pool, targeted)
	offerReused(t, pool, other)
	populate(t, pool)

	noRules := &orchestrator.SandboxNetworkConfig{}
	teamCtx := featureflags.AddToContext(t.Context(), featureflags.TeamContext(teamID))
	gotTargeted, err := pool.Get(teamCtx, noRules, sandboxtypes.EgressClassSandbox)
	require.NoError(t, err)
	require.Same(t, targeted, gotTargeted)
	gotOther, err := pool.Get(t.Context(), noRules, sandboxtypes.EgressClassSandbox)
	require.NoError(t, err)
	require.Same(t, other, gotOther)
	require.Equal(t, NonDNSUDPModeDrop, targeted.Firewall.NonDNSUDPMode(), "the team's slot is handed out under the team's value")
	require.Equal(t, NonDNSUDPModeOff, other.Firewall.NonDNSUDPMode(), "the other slot under the fallthrough")
	otherHandles := ruleHandles(filterChainRules(t, other.NamespaceID()))

	target(NonDNSUDPModeCount, NonDNSUDPModeOff)
	awaitNonDNSUDPMode(t, targeted, NonDNSUDPModeCount)
	require.Equal(t, []ruleShape{nonDNSUDPCount, establishedNonUDPAccept, establishedReplyAccept, allowSetAccept, denySetDrop, nonTCPUserAllow, nonTCPUserDeny},
		filterChainShapes(t, targeted.NamespaceID()), "the team's slot follows the team's value")
	require.Equal(t, otherHandles, ruleHandles(filterChainRules(t, other.NamespaceID())), "a change targeting another team does not touch the slot")

	target(NonDNSUDPModeCount, NonDNSUDPModeDrop)
	awaitNonDNSUDPMode(t, other, NonDNSUDPModeDrop)
	require.Equal(t, NonDNSUDPModeCount, targeted.Firewall.NonDNSUDPMode(), "the team's slot keeps the team's value when the fallthrough changes")
}

// A slot already on its way back to the pool is left alone by a flag
// change: its release rebuilds the chain once, in the mode current then.
func TestPool_NonDNSUDPFlagChangeSkipsASlotBeingReleased(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	slot := newFirewallTestSlot(t, "fw-non-dns-udp-releasing")
	ff, td := nonDNSUDPFlagSource(t, NonDNSUDPModeDrop)
	pool := NewPool(NewSlotsPoolSize, ReusedSlotsPoolSize, parkedStorage{}, slot.config, WithNonDNSUDPFlag(ff))
	offerReused(t, pool, slot)

	var rebuilds atomic.Int32
	beforeFlush = func() { rebuilds.Add(1) }
	t.Cleanup(func() { beforeFlush = nil })

	withRules := &orchestrator.SandboxNetworkConfig{
		Egress: &orchestrator.SandboxNetworkEgressConfig{DeniedCidrs: []string{"10.0.0.0/8"}},
	}
	got, err := pool.Get(t.Context(), withRules, sandboxtypes.EgressClassSandbox)
	require.NoError(t, err)
	require.Same(t, slot, got)
	require.Equal(t, int32(1), rebuilds.Load(), "the hand-out built the tenant's chain")
	handles := ruleHandles(filterChainRules(t, slot.NamespaceID()))

	require.NoError(t, pool.ReturnAsync(t.Context(), slot, noopRelease, 2*time.Second))
	setNonDNSUDPFlag(td, NonDNSUDPModeOff)
	pool.modeFollower.Reapply(t.Context())
	require.Equal(t, int32(1), rebuilds.Load(), "a slot being released is not rebuilt by the flag change")
	// A walk that listed the slot before its return reaches it after: the
	// slot itself declines.
	rebuilt, err := slot.reapplyNonDNSUDPMode(t.Context())
	require.NoError(t, err)
	require.False(t, rebuilt, "a slot already on its way back declines a rebuild")
	require.Equal(t, int32(1), rebuilds.Load())
	require.Equal(t, handles, ruleHandles(filterChainRules(t, slot.NamespaceID())), "its chain is left to the release")

	require.Eventually(t, func() bool { return len(pool.reusedSlots) == 1 }, 10*time.Second, 10*time.Millisecond, "the release did not finish")
	require.Equal(t, int32(2), rebuilds.Load(), "the release rebuilt the chain once")
	require.Equal(t, defaultChain, filterChainShapes(t, slot.NamespaceID()), "in the mode current at the release")
}

// A return does not wait on the slot's firewall: sandbox cleanup relies on
// ReturnAsync coming back at once, and a flag-change rebuild stalled in
// netlink holds the firewall mutex for as long as netlink takes.
func TestPool_ReturnAsyncDoesNotWaitOnAFirewallRebuild(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	slot := newFirewallTestSlot(t, "fw-non-dns-udp-return")
	ff, _ := nonDNSUDPFlagSource(t, NonDNSUDPModeOff)
	pool := NewPool(NewSlotsPoolSize, ReusedSlotsPoolSize, parkedStorage{}, slot.config, WithNonDNSUDPFlag(ff))
	offerReused(t, pool, slot)

	got, err := pool.Get(t.Context(), &orchestrator.SandboxNetworkConfig{}, sandboxtypes.EgressClassSandbox)
	require.NoError(t, err)
	require.Same(t, slot, got)

	// A rebuild stalled in netlink holds the mutex.
	slot.firewallMu.Lock()
	returned := make(chan error, 1)
	go func() { returned <- pool.ReturnAsync(t.Context(), slot, noopRelease, 0) }()
	select {
	case err := <-returned:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		slot.firewallMu.Unlock()
		t.Fatal("the return waited on the firewall mutex")
	}
	require.False(t, slot.handedOut.Load(), "the slot is marked on its way back while the rebuild still holds the mutex")
	slot.firewallMu.Unlock()

	require.Eventually(t, func() bool { return len(pool.reusedSlots) == 1 }, 10*time.Second, 10*time.Millisecond, "the release did not finish once the rebuild let go")
}

// Populate returns only after a flag-change rebuild in progress has finished,
// so nothing changes a slot's chain after the pool reports it has stopped.
func TestPool_PopulateWaitsForAFlagChangeRebuild(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	slot := newFirewallTestSlot(t, "fw-non-dns-udp-populate-join")
	ff, td := nonDNSUDPFlagSource(t, NonDNSUDPModeDrop)
	pool := NewPool(NewSlotsPoolSize, ReusedSlotsPoolSize, parkedStorage{}, slot.config, WithNonDNSUDPFlag(ff))
	offerReused(t, pool, slot)

	ctx, cancel := context.WithCancel(t.Context())
	populated := make(chan struct{})
	go func() {
		defer close(populated)
		pool.Populate(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-populated:
		case <-time.After(10 * time.Second):
			t.Error("Populate did not return")
		}
	})

	got, err := pool.Get(t.Context(), &orchestrator.SandboxNetworkConfig{}, sandboxtypes.EgressClassSandbox)
	require.NoError(t, err)
	require.Same(t, slot, got)

	entered, release := make(chan struct{}), make(chan struct{})
	releaseRebuild := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseRebuild)
	slot.firewallMu.Lock()
	tenantMode := slot.tenantMode
	slot.tenantMode = func() NonDNSUDPMode {
		close(entered)
		<-release

		return tenantMode()
	}
	slot.firewallMu.Unlock()

	waitFor := func(ch <-chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for %s", what)
		}
	}

	setNonDNSUDPFlag(td, NonDNSUDPModeOff)
	waitFor(entered, "the rebuild to start")
	cancel()

	select {
	case <-populated:
		t.Fatal("Populate returned while a rebuild was still running")
	case <-time.After(500 * time.Millisecond):
	}
	releaseRebuild()
	waitFor(populated, "Populate to return")
	require.Equal(t, NonDNSUDPModeOff, slot.Firewall.NonDNSUDPMode(), "the rebuild finished before Populate returned")
}

type tenantRequestKey struct{}

// What a slot remembers of its tenant for a later re-evaluation is the
// request's flag contexts and nothing else: not the request context, which
// would keep its span and every value up its chain for the sandbox's lifetime.
func TestSlot_RememberTenantKeepsOnlyTheFlagContexts(t *testing.T) {
	t.Parallel()

	const teamID = "team-remembered"
	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.SandboxFirewallNonDNSUDPFlag.Key()).
		Variations(ldvalue.String("off"), ldvalue.String("drop")).
		FallthroughVariationIndex(0).
		VariationIndexForKey(featureflags.TeamKind, teamID, 1))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	evaluated := make(chan context.Context, 1)
	slot := &Slot{}
	slot.UseNonDNSUDPMode(func(ctx context.Context) string {
		evaluated <- ctx

		return ff.StringFlag(ctx, featureflags.SandboxFirewallNonDNSUDPFlag)
	})

	request, cancel := context.WithCancel(featureflags.AddToContext(t.Context(), featureflags.TeamContext(teamID)))
	defer cancel()
	request = context.WithValue(request, tenantRequestKey{}, "request value")
	slot.rememberTenant(request, nil)

	require.Equal(t, NonDNSUDPModeDrop, slot.tenantMode(), "the team's targeting is kept")
	evaluatedWith := <-evaluated
	require.Nil(t, evaluatedWith.Value(tenantRequestKey{}), "a value of the request is not kept, so neither the request context nor one derived from it is")
	require.Nil(t, evaluatedWith.Done(), "nor is its cancellation")
}

// failNextFlush buffers a rule for a table that does not exist, so the next
// flush on the slot's firewall fails and the kernel keeps the chain it runs.
func failNextFlush(slot *Slot) {
	missing := &nftables.Table{Name: "missing", Family: nftables.TableFamilyINet}
	slot.Firewall.conn.AddRule(&nftables.Rule{Table: missing, Chain: &nftables.Chain{Name: "missing", Table: missing}, Exprs: accept()})
}

// A failed apply leaves what the slot remembers of its tenant as it was: the
// kernel kept the previous chain, so a flag-change rebuild that follows
// installs the egress the sandbox runs under, not the one the caller was
// told failed.
func TestSlot_FailedEgressApplyKeepsThePreviousTenantMemory(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	slot := newFirewallTestSlot(t, "fw-non-dns-udp-failed-apply")
	ff, td := nonDNSUDPFlagSource(t, NonDNSUDPModeOff)
	pool := NewPool(NewSlotsPoolSize, ReusedSlotsPoolSize, parkedStorage{}, slot.config, WithNonDNSUDPFlag(ff))
	offerReused(t, pool, slot)
	populate(t, pool)

	// A hand-out whose apply fails remembers no tenant: the slot goes to
	// cleanup with its chain as the pool left it.
	failNextFlush(slot)
	require.Error(t, slot.ConfigureInternet(t.Context(), &orchestrator.SandboxNetworkConfig{
		Egress: &orchestrator.SandboxNetworkEgressConfig{DeniedCidrs: []string{"192.168.0.0/16"}},
	}))
	require.Nil(t, slot.tenantMode, "a failed hand-out leaves no tenant to re-evaluate")
	require.Nil(t, slot.tenantEgress)

	installed := &orchestrator.SandboxNetworkEgressConfig{DeniedCidrs: []string{"10.0.0.0/8"}}
	got, err := pool.Get(t.Context(), &orchestrator.SandboxNetworkConfig{Egress: installed}, sandboxtypes.EgressClassSandbox)
	require.NoError(t, err)
	require.Same(t, slot, got)
	require.Same(t, installed, slot.tenantEgress, "the hand-out's egress is remembered once its chain is built")

	failNextFlush(slot)
	require.Error(t, slot.UpdateInternet(t.Context(), &orchestrator.SandboxNetworkEgressConfig{DeniedCidrs: []string{"192.168.0.0/16"}}))
	require.Equal(t, ChainDescriptor{Mode: NonDNSUDPModeOff, Posture: postureRules}, slot.Firewall.Installed(), "the kernel kept the previous chain")
	require.Same(t, installed, slot.tenantEgress, "the failed update is not remembered")

	setNonDNSUDPFlag(td, NonDNSUDPModeCount)
	awaitNonDNSUDPMode(t, slot, NonDNSUDPModeCount)
	elements, err := slot.Firewall.conn.GetSetElements(slot.Firewall.userDenySet.Set())
	require.NoError(t, err)
	assertIPv4SetMembership(t, elements, []ipv4SetProbe{
		{netip.MustParseAddr("10.0.0.1"), true},
		{netip.MustParseAddr("192.168.0.1"), false},
	})
}

// The walk's summary counts the slots the walk reached: none at debug level
// when no slot is handed out, and only those dispatched before the context
// ended when the walk is cut short, not the handed-out count.
func TestNonDNSUDPModeFollower_ReapplySummaryCountsTheSlotsReached(t *testing.T) { //nolint:paralleltest // replaces the global logger
	core, logs := observer.New(zapcore.DebugLevel)
	t.Cleanup(logger.ReplaceGlobals(t.Context(), logger.NewTracedLoggerFromCore(core)))
	summary := func() observer.LoggedEntry {
		var entries []observer.LoggedEntry
		for _, entry := range logs.TakeAll() {
			if entry.Message == reappliedMessage {
				entries = append(entries, entry)
			}
		}
		require.Len(t, entries, 1)

		return entries[0]
	}

	follower := NewNonDNSUDPModeFollower(nil)
	follower.Reapply(t.Context())
	entry := summary()
	require.Equal(t, zapcore.DebugLevel, entry.Level, "no slot handed out: a debug line")
	require.EqualValues(t, 0, entry.ContextMap()["examined"])

	follower.HandOut(&Slot{Idx: 1})
	follower.HandOut(&Slot{Idx: 2})
	ended, cancel := context.WithCancel(t.Context())
	cancel()
	follower.Reapply(ended)
	entry = summary()
	require.Equal(t, zapcore.InfoLevel, entry.Level, "slots handed out: an info line")
	require.EqualValues(t, 0, entry.ContextMap()["examined"], "a walk cut short before its first slot examined none")
	require.EqualValues(t, 0, entry.ContextMap()["rebuilt"])
	require.EqualValues(t, 0, entry.ContextMap()["failed"])
}

func (f *NonDNSUDPModeFollower) awaitingRetry(slot *Slot) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, awaiting := f.failed[slot]

	return awaiting
}

func (f *NonDNSUDPModeFollower) reachedRetryCap() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.retryCapped
}

// requireRetryWaitAround requires a wait within the jitter of the nominal
// one: half of it either way.
func requireRetryWaitAround(t *testing.T, nominal, wait time.Duration) {
	t.Helper()

	require.GreaterOrEqual(t, wait, nominal/2, "jittered down to half the wait")
	require.LessOrEqual(t, wait, nominal+nominal/2, "and up to half again")
}

// A failed live-mode update is retried: the slot stays eligible and the
// follower reaches the flag's value without a second flag change or a
// tenant update, so a rollback recovers from one failed replacement.
func TestPool_NonDNSUDPFailedModeUpdateIsRetried(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	slot := newFirewallTestSlot(t, "fw-non-dns-udp-retry")
	ff, td := nonDNSUDPFlagSource(t, NonDNSUDPModeDrop)
	pool := NewPool(NewSlotsPoolSize, ReusedSlotsPoolSize, parkedStorage{}, slot.config, WithNonDNSUDPFlag(ff))
	offerReused(t, pool, slot)
	populate(t, pool)

	var rebuilds atomic.Int32
	beforeFlush = func() { rebuilds.Add(1) }
	t.Cleanup(func() { beforeFlush = nil })

	got, err := pool.Get(t.Context(), &orchestrator.SandboxNetworkConfig{}, sandboxtypes.EgressClassSandbox)
	require.NoError(t, err)
	require.Same(t, slot, got)
	require.Equal(t, NonDNSUDPModeDrop, slot.Firewall.NonDNSUDPMode(), "handed out under drop")
	require.Equal(t, int32(1), rebuilds.Load())

	// The rollback's first replacement fails and the kernel keeps the drop chain.
	failNextFlush(slot)
	setNonDNSUDPFlag(td, NonDNSUDPModeOff)
	awaitNonDNSUDPMode(t, slot, NonDNSUDPModeOff)
	require.Equal(t, defaultChain, filterChainShapes(t, slot.NamespaceID()), "the retry installed the flag's mode")
	require.Equal(t, int32(3), rebuilds.Load(), "one failed replacement, then one retry")
	require.Eventually(t, func() bool { return !pool.modeFollower.awaitingRetry(slot) },
		10*time.Second, 10*time.Millisecond, "a successful retry forgets the slot")
}

// follow runs the pool's follower until the test ends.
func follow(t *testing.T, pool *Pool) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.modeFollower.Follow(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// A flag change made after a hand-out but before the follower starts
// watching reaches the slot with no further change of the flag.
func TestPool_NonDNSUDPFlagChangeBeforeFollowStartsReachesHandedOutSlot(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	slot := newFirewallTestSlot(t, "fw-non-dns-udp-follow-start")
	ff, td := nonDNSUDPFlagSource(t, NonDNSUDPModeDrop)
	pool := NewPool(NewSlotsPoolSize, ReusedSlotsPoolSize, parkedStorage{}, slot.config, WithNonDNSUDPFlag(ff))
	offerReused(t, pool, slot)

	got, err := pool.Get(t.Context(), &orchestrator.SandboxNetworkConfig{}, sandboxtypes.EgressClassSandbox)
	require.NoError(t, err)
	require.Same(t, slot, got)
	require.Equal(t, NonDNSUDPModeDrop, slot.Firewall.NonDNSUDPMode(), "handed out under drop")

	setNonDNSUDPFlag(td, NonDNSUDPModeCount)
	follow(t, pool)
	awaitNonDNSUDPMode(t, slot, NonDNSUDPModeCount)
}

// The follower's first re-apply is retried when it fails, with no further
// change of the flag.
func TestPool_NonDNSUDPFailedFirstReapplyIsRetried(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	slot := newFirewallTestSlot(t, "fw-non-dns-udp-follow-retry")
	ff, td := nonDNSUDPFlagSource(t, NonDNSUDPModeDrop)
	pool := NewPool(NewSlotsPoolSize, ReusedSlotsPoolSize, parkedStorage{}, slot.config, WithNonDNSUDPFlag(ff))
	offerReused(t, pool, slot)

	var rebuilds atomic.Int32
	beforeFlush = func() { rebuilds.Add(1) }
	t.Cleanup(func() { beforeFlush = nil })

	got, err := pool.Get(t.Context(), &orchestrator.SandboxNetworkConfig{}, sandboxtypes.EgressClassSandbox)
	require.NoError(t, err)
	require.Same(t, slot, got)
	require.Equal(t, int32(1), rebuilds.Load())

	failNextFlush(slot)
	setNonDNSUDPFlag(td, NonDNSUDPModeCount)
	follow(t, pool)
	awaitNonDNSUDPMode(t, slot, NonDNSUDPModeCount)
	require.Equal(t, int32(3), rebuilds.Load(), "one failed first re-apply, then one retry")
}

// A slot returned to the pool while it awaits a retry is forgotten: the
// retry leaves its chain alone and the release rebuilds it, in the mode
// current then.
func TestPool_NonDNSUDPRetrySkipsASlotReturnedToThePool(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	slot := newFirewallTestSlot(t, "fw-non-dns-udp-retry-returned")
	ff, td := nonDNSUDPFlagSource(t, NonDNSUDPModeDrop)
	pool := NewPool(NewSlotsPoolSize, ReusedSlotsPoolSize, parkedStorage{}, slot.config, WithNonDNSUDPFlag(ff))
	offerReused(t, pool, slot)

	var rebuilds atomic.Int32
	beforeFlush = func() { rebuilds.Add(1) }
	t.Cleanup(func() { beforeFlush = nil })

	withRules := &orchestrator.SandboxNetworkConfig{
		Egress: &orchestrator.SandboxNetworkEgressConfig{DeniedCidrs: []string{"10.0.0.0/8"}},
	}
	got, err := pool.Get(t.Context(), withRules, sandboxtypes.EgressClassSandbox)
	require.NoError(t, err)
	require.Same(t, slot, got)
	require.Equal(t, int32(1), rebuilds.Load(), "the hand-out built the tenant's chain")

	failNextFlush(slot)
	setNonDNSUDPFlag(td, NonDNSUDPModeOff)
	pool.modeFollower.Reapply(t.Context())
	require.Equal(t, int32(2), rebuilds.Load(), "the flag change attempted a replacement")
	require.Equal(t, NonDNSUDPModeDrop, slot.Firewall.NonDNSUDPMode(), "which failed: the kernel kept the drop chain")
	require.True(t, pool.modeFollower.awaitingRetry(slot), "the failed slot awaits a retry")
	handles := ruleHandles(filterChainRules(t, slot.NamespaceID()))

	require.NoError(t, pool.ReturnAsync(t.Context(), slot, noopRelease, 2*time.Second))
	require.False(t, pool.modeFollower.awaitingRetry(slot), "a slot on its way back is forgotten at once")
	pool.modeFollower.retryFailed(t.Context())
	require.Equal(t, int32(2), rebuilds.Load(), "the retry does not rebuild a slot returned to the pool")
	require.Equal(t, handles, ruleHandles(filterChainRules(t, slot.NamespaceID())), "its chain is left to the release")

	require.Eventually(t, func() bool { return len(pool.reusedSlots) == 1 }, 10*time.Second, 10*time.Millisecond, "the release did not finish")
	require.Equal(t, int32(3), rebuilds.Load(), "the release rebuilt the chain once")
	require.Equal(t, defaultChain, filterChainShapes(t, slot.NamespaceID()), "in the mode current at the release")
}

// A slot in the retry set of a follower that has not walked a flag change
// yet is retried after the initial wait: the schedule starts there on its
// own, so the jitter has something to draw from.
func TestNonDNSUDPModeFollower_RetriesASlotSeededBeforeAnyFlagChange(t *testing.T) {
	t.Parallel()

	follower := NewNonDNSUDPModeFollower(nil)
	slot := &Slot{Idx: 1}
	slot.tenantMode = func() NonDNSUDPMode { return NonDNSUDPModeOff }
	follower.HandOut(slot)
	follower.mu.Lock()
	follower.failed[slot] = struct{}{}
	follower.mu.Unlock()

	delay, pending := follower.nextRetry()
	require.True(t, pending, "the seeded slot awaits a retry")
	requireRetryWaitAround(t, retryInitialDelay, delay)
	follower.retryFailed(t.Context())
	require.True(t, follower.awaitingRetry(slot), "a slot without a firewall fails its retry and stays eligible")
}

// The retry waits start at a quarter second and double to a cap of thirty
// seconds, each jittered by half either way; the cap is noted once reached;
// a flag change starts them over, and a slot taken back awaits no retry.
func TestNonDNSUDPModeFollower_RetryBackoff(t *testing.T) {
	t.Parallel()

	follower := NewNonDNSUDPModeFollower(nil)
	_, pending := follower.nextRetry()
	require.False(t, pending, "nothing failed, nothing to retry")

	// Handed out with a tenant but no firewall: every re-apply fails.
	slot := &Slot{Idx: 1}
	slot.tenantMode = func() NonDNSUDPMode { return NonDNSUDPModeOff }
	follower.HandOut(slot)
	follower.Reapply(t.Context())
	require.True(t, follower.awaitingRetry(slot), "the failed slot awaits a retry")

	want := retryInitialDelay
	for retries := range 9 {
		delay, pending := follower.nextRetry()
		require.True(t, pending)
		requireRetryWaitAround(t, want, delay)
		require.Equal(t, want == retryMaxDelay, follower.reachedRetryCap(), "the cap is noted once the wait reaches it, after %d retries", retries)
		follower.retryFailed(t.Context())
		want = min(2*want, retryMaxDelay)
	}
	require.True(t, follower.reachedRetryCap(), "the wait is capped")
	require.True(t, follower.awaitingRetry(slot), "a slot still failing stays eligible")

	follower.Reapply(t.Context())
	require.False(t, follower.reachedRetryCap(), "a flag change starts the backoff over")
	delay, pending := follower.nextRetry()
	require.True(t, pending)
	requireRetryWaitAround(t, retryInitialDelay, delay)

	follower.TakeBack(slot)
	_, pending = follower.nextRetry()
	require.False(t, pending, "a slot taken back awaits no retry")
}
