//go:build linux

package network

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coreos/go-iptables/iptables"
	"github.com/stretchr/testify/require"
	"github.com/vishvananda/netns"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// reserveNSTestIdx hands out unique high slot indexes so the root-gated netns
// tests never share host-side state, whatever order they run in.
var nsTestIdx atomic.Int32

func reserveNSTestIdx(t *testing.T) int {
	t.Helper()

	idx := 30000 + int(nsTestIdx.Add(1))
	require.Less(t, idx, vrtSlotsSize, "netns test index range exhausted")

	return idx
}

func TestConfig_EgressDSCP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		config      Config
		wantSandbox uint8
		wantBuild   uint8
	}{
		{
			name:        "unset build value falls back to the sandbox value",
			config:      Config{SandboxEgressDSCP: 8},
			wantSandbox: 8,
			wantBuild:   8,
		},
		{
			name:        "build value overrides for builds only",
			config:      Config{SandboxEgressDSCP: 8, BuildSandboxEgressDSCP: DSCP(16)},
			wantSandbox: 8,
			wantBuild:   16,
		},
		{
			name:        "explicit zero disables marking for builds only",
			config:      Config{SandboxEgressDSCP: 8, BuildSandboxEgressDSCP: DSCP(0)},
			wantSandbox: 8,
			wantBuild:   0,
		},
		{
			name:        "builds can be marked while sandboxes are not",
			config:      Config{SandboxEgressDSCP: 0, BuildSandboxEgressDSCP: DSCP(16)},
			wantSandbox: 0,
			wantBuild:   16,
		},
		{
			name:        "both disabled by default",
			config:      Config{},
			wantSandbox: 0,
			wantBuild:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.wantSandbox, tt.config.EgressDSCP(sandboxtypes.EgressClassSandbox))
			require.Equal(t, tt.wantBuild, tt.config.EgressDSCP(sandboxtypes.EgressClassBuild))
		})
	}
}

// TestConfig_EgressTOS pins the DSCP→TOS resolution both connection proxies
// consume; the SandboxType→sandboxtypes.EgressClass mapping is pinned in the shared sandboxtypes
// package's egress_class_test.go.
func TestConfig_EgressTOS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config Config
		class  sandboxtypes.EgressClass
		want   int
	}{
		{
			name:   "build class uses the build value",
			config: Config{SandboxEgressDSCP: 8, BuildSandboxEgressDSCP: DSCP(16)},
			class:  sandboxtypes.EgressClassBuild,
			want:   16 << 2, // 0x40
		},
		{
			name:   "sandbox class keeps the sandbox value when a build value is set",
			config: Config{SandboxEgressDSCP: 8, BuildSandboxEgressDSCP: DSCP(16)},
			class:  sandboxtypes.EgressClassSandbox,
			want:   8 << 2, // 0x20
		},
		{
			name:   "unset build value falls back to the sandbox value",
			config: Config{SandboxEgressDSCP: 8},
			class:  sandboxtypes.EgressClassBuild,
			want:   8 << 2,
		},
		{
			name:   "build value of 0 disables marking for builds only",
			config: Config{SandboxEgressDSCP: 8, BuildSandboxEgressDSCP: DSCP(0)},
			class:  sandboxtypes.EgressClassBuild,
			want:   0,
		},
		{
			name:   "sandbox value of 0 disables marking while builds stay marked",
			config: Config{SandboxEgressDSCP: 0, BuildSandboxEgressDSCP: DSCP(16)},
			class:  sandboxtypes.EgressClassSandbox,
			want:   0,
		},
		{
			name:   "both unset disables marking",
			config: Config{},
			class:  sandboxtypes.EgressClassBuild,
			want:   0,
		},
		{
			name:   "max DSCP maps to the top of the TOS byte",
			config: Config{BuildSandboxEgressDSCP: DSCP(63)},
			class:  sandboxtypes.EgressClassBuild,
			want:   63 << 2, // 0xFC
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, tt.config.EgressTOS().For(tt.class))
		})
	}
}

// The per-connection cost in the proxies is one For() on a resolved pair.
func BenchmarkEgressTOS_For(b *testing.B) {
	tos := Config{SandboxEgressDSCP: 8, BuildSandboxEgressDSCP: DSCP(16)}.EgressTOS()

	b.ReportAllocs()
	for range b.N {
		benchTOS = tos.For(sandboxtypes.EgressClassBuild)
	}
}

var benchTOS int

func TestConfig_Validate_EgressDSCPRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		wantErr string
	}{
		{name: "both unset", config: Config{NetworkVersion: 1}},
		{name: "both at the top of the range", config: Config{NetworkVersion: 1, SandboxEgressDSCP: 63, BuildSandboxEgressDSCP: DSCP(63)}},
		{
			name:    "sandbox value out of range",
			config:  Config{NetworkVersion: 1, SandboxEgressDSCP: 64},
			wantErr: "SANDBOX_EGRESS_DSCP=64 out of range (0..63)",
		},
		{
			name:    "build value out of range",
			config:  Config{NetworkVersion: 1, BuildSandboxEgressDSCP: DSCP(255)},
			wantErr: "BUILD_SANDBOX_EGRESS_DSCP=255 out of range (0..63)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.config.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.EqualError(t, err, tt.wantErr)
		})
	}
}

// An absent BUILD_SANDBOX_EGRESS_DSCP must leave the pointer nil, not 0.
func TestParseConfig_BuildEgressDSCP(t *testing.T) {
	// Inherited by every subtest.
	t.Setenv("SANDBOX_EGRESS_DSCP", "8")

	t.Run("absent leaves the build value unset", func(t *testing.T) { //nolint:paralleltest // the parent test sets shared process environment
		config, err := ParseConfig()
		require.NoError(t, err)
		require.Nil(t, config.BuildSandboxEgressDSCP)
		require.Equal(t, uint8(8), config.EgressDSCP(sandboxtypes.EgressClassBuild))
	})

	t.Run("set but empty behaves as unset and inherits", func(t *testing.T) {
		t.Setenv("BUILD_SANDBOX_EGRESS_DSCP", "")

		config, err := ParseConfig()
		require.NoError(t, err)
		require.Nil(t, config.BuildSandboxEgressDSCP)
		require.Equal(t, uint8(8), config.EgressDSCP(sandboxtypes.EgressClassBuild))
	})

	t.Run("explicit zero is distinct from absent", func(t *testing.T) {
		t.Setenv("BUILD_SANDBOX_EGRESS_DSCP", "0")

		config, err := ParseConfig()
		require.NoError(t, err)
		require.NotNil(t, config.BuildSandboxEgressDSCP)
		require.Equal(t, uint8(0), config.EgressDSCP(sandboxtypes.EgressClassBuild))
	})

	t.Run("set value is used for builds", func(t *testing.T) {
		t.Setenv("BUILD_SANDBOX_EGRESS_DSCP", "16")

		config, err := ParseConfig()
		require.NoError(t, err)
		require.Equal(t, uint8(8), config.EgressDSCP(sandboxtypes.EgressClassSandbox))
		require.Equal(t, uint8(16), config.EgressDSCP(sandboxtypes.EgressClassBuild))
	})

	t.Run("out of range fails loudly", func(t *testing.T) {
		t.Setenv("BUILD_SANDBOX_EGRESS_DSCP", "64")

		_, err := ParseConfig()
		require.ErrorContains(t, err, "BUILD_SANDBOX_EGRESS_DSCP=64 out of range (0..63)")
	})

	t.Run("non-numeric fails loudly", func(t *testing.T) {
		t.Setenv("BUILD_SANDBOX_EGRESS_DSCP", "scavenger")

		_, err := ParseConfig()
		require.Error(t, err)
	})
}

// A transient iptables.New() failure must not be cached: the next call has to
// retry, or one fork failure would poison every restamp for the process life.
func TestIptablesHandle_TransientFailureIsNotCached(t *testing.T) {
	// Unlike the netns tests this one runs without root, so gate on the one
	// thing it does need: the binary.
	if _, err := exec.LookPath("iptables"); err != nil {
		t.Skip("iptables binary not on PATH")
	}

	origPath := os.Getenv("PATH")

	old := cachedIptables.Load()
	cachedIptables.Store(nil)
	t.Cleanup(func() { cachedIptables.Store(old) })

	t.Setenv("PATH", "/nonexistent")
	_, err := iptablesHandle()
	require.Error(t, err, "New() must fail with no iptables on PATH")

	t.Setenv("PATH", origPath)
	tables, err := iptablesHandle()
	require.NoError(t, err, "a past transient failure must not be cached")
	require.NotNil(t, tables)
}

// TestCreateNetwork_TagsEgressWithDSCP verifies that CreateNetwork installs the
// mangle/POSTROUTING rule that stamps DSCP CS1 (8) on every packet leaving the
// sandbox netns through the vpeer uplink (eth0).
//
// It exercises the real CreateNetwork path, so it needs root (netns, iptables)
// and the xt_DSCP kernel module; it skips otherwise — same gating as the other
// privileged integration tests in the orchestrator (see cmd/smoketest).
func TestCreateNetwork_TagsEgressWithDSCP(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + iptables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	config.SandboxEgressDSCP = 8 // env default is 0 (disabled)

	slot, err := NewSlot("dscp-egress-test", reserveNSTestIdx(t), config, NewNoopEgressProxy())
	require.NoError(t, err)

	require.NoError(t, slot.CreateNetwork(t.Context()))
	t.Cleanup(func() { _ = slot.RemoveNetwork() })

	rules := dscpMangleRules(t, slot.NamespaceID())

	require.Lenf(t, rules, 1, "want exactly one DSCP mangle rule in %s, got %v", slot.NamespaceID(), rules)
	require.Containsf(t, rules[0], "-o "+slot.VpeerName(), "DSCP rule must match the vpeer uplink: %s", rules[0])
	require.Containsf(t, rules[0], "--set-dscp 0x08", "DSCP rule must set CS1 (8): %s", rules[0])
}

// Drives the real netns/iptables path; skips without root.
func TestApplyEgressDSCP_RestampsSlotRule(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + iptables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	config.SandboxEgressDSCP = 8             // regular sandboxes: CS1
	config.BuildSandboxEgressDSCP = DSCP(16) // template builds: CS2

	slot, err := NewSlot("dscp-restamp-test", reserveNSTestIdx(t), config, NewNoopEgressProxy())
	require.NoError(t, err)

	require.NoError(t, slot.CreateNetwork(t.Context()))
	t.Cleanup(func() { _ = slot.RemoveNetwork() })

	requireSingleDSCPRule(t, slot, "0x08") // CreateNetwork seeds the sandbox class

	// A build sandbox takes the slot.
	require.NoError(t, slot.applyEgressDSCP(t.Context(), config.EgressDSCP(sandboxtypes.EgressClassBuild)))
	requireSingleDSCPRule(t, slot, "0x10")

	// Re-applying the same class must not duplicate the rule.
	require.NoError(t, slot.applyEgressDSCP(t.Context(), config.EgressDSCP(sandboxtypes.EgressClassBuild)))
	requireSingleDSCPRule(t, slot, "0x10")

	// The pool recycles the slot back to the sandbox class.
	require.NoError(t, slot.applyEgressDSCP(t.Context(), config.EgressDSCP(sandboxtypes.EgressClassSandbox)))
	requireSingleDSCPRule(t, slot, "0x08")

	// 0 removes the rule outright.
	require.NoError(t, slot.applyEgressDSCP(t.Context(), 0))
	require.Emptyf(t, dscpMangleRules(t, slot.NamespaceID()), "DSCP 0 must leave no mangle rule in %s", slot.NamespaceID())
}

// Failed restamp: the new rule is rejected before the old one is touched, so
// the previous class must stay installed and cached.
func TestApplyEgressDSCP_FailedRestampKeepsOldClass(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + iptables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	config.SandboxEgressDSCP = 8

	slot, err := NewSlot("dscp-failed-restamp-test", reserveNSTestIdx(t), config, NewNoopEgressProxy())
	require.NoError(t, err)

	require.NoError(t, slot.CreateNetwork(t.Context()))
	t.Cleanup(func() { _ = slot.RemoveNetwork() })

	requireSingleDSCPRule(t, slot, "0x08")

	// 200 is outside the 6-bit range: AppendUnique's -C existence probe exits
	// with a parameter error, so the append fails before any chain mutation.
	require.Error(t, slot.applyEgressDSCP(t.Context(), 200))
	requireSingleDSCPRule(t, slot, "0x08")

	// The cache must still drive a later restamp from the old class.
	require.NoError(t, slot.applyEgressDSCP(t.Context(), 16))
	requireSingleDSCPRule(t, slot, "0x10")
}

// requireSingleDSCPRule asserts exactly one DSCP mangle rule on the uplink.
func requireSingleDSCPRule(t *testing.T, slot *Slot, wantDSCP string) {
	t.Helper()

	rules := dscpMangleRules(t, slot.NamespaceID())

	require.Lenf(t, rules, 1, "want exactly one DSCP mangle rule in %s, got %v", slot.NamespaceID(), rules)
	require.Containsf(t, rules[0], "-o "+slot.VpeerName(), "DSCP rule must match the vpeer uplink: %s", rules[0])
	require.Containsf(t, rules[0], "--set-dscp "+wantDSCP, "DSCP rule must set %s: %s", wantDSCP, rules[0])
}

// dscpMangleRules returns the mangle/POSTROUTING rules that reference the DSCP
// target inside the named network namespace.
func dscpMangleRules(t *testing.T, nsName string) []string {
	t.Helper()

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	host, err := netns.Get()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, netns.Set(host))
		_ = host.Close()
	}()

	target, err := netns.GetFromName(nsName)
	require.NoError(t, err)
	defer func() { _ = target.Close() }()
	require.NoError(t, netns.Set(target))

	tables, err := iptables.New()
	require.NoError(t, err)
	all, err := tables.List("mangle", "POSTROUTING")
	require.NoError(t, err)

	var rules []string
	for _, r := range all {
		if strings.Contains(r, "DSCP") {
			rules = append(rules, r)
		}
	}

	return rules
}

// Get's failure path: a slot whose restamp fails must come back to the pool —
// via ReturnAsync → recycle — still carrying the untenanted class.
func TestPool_GetFailureReturnsSlotWithSandboxDSCP(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + iptables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	config.SandboxEgressDSCP = 8
	// Out of the 6-bit range, bypassing ParseConfig validation on purpose:
	// the -C probe rejects it, so the build restamp fails deterministically.
	config.BuildSandboxEgressDSCP = DSCP(200)

	slot, err := NewSlot("dscp-get-failure-test", reserveNSTestIdx(t), config, NewNoopEgressProxy())
	require.NoError(t, err)

	require.NoError(t, slot.CreateNetwork(t.Context()))
	t.Cleanup(func() { _ = slot.RemoveNetwork() })

	pool := NewPool(2, 1, &fakeStorage{}, config)
	t.Cleanup(func() { _ = pool.Close(context.WithoutCancel(t.Context())) })

	requireSingleDSCPRule(t, slot, "0x08")

	pool.newSlots <- slot
	close(pool.newSlots) // Populate never runs here, and Close ranges over it.

	_, err = pool.Get(t.Context(), nil, sandboxtypes.EgressClassBuild)
	require.ErrorContains(t, err, "egress DSCP for build")

	select {
	case reused := <-pool.reusedSlots:
		require.Equal(t, slot.Idx, reused.Idx)
	case <-time.After(10 * time.Second):
		t.Fatal("slot was not returned to the pool after a failed Get")
	}

	requireSingleDSCPRule(t, slot, "0x08")
}

// The pool seam that protects tenants, through the same path production takes:
// Get stamps the build class, returnSlot strips it — with real egress rules so
// ConfigureInternet and ResetInternet run instead of short-circuiting.
func TestPool_RecycleRestoresSandboxDSCP(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns + iptables")
	}

	config, err := ParseConfig()
	require.NoError(t, err)
	config.SandboxEgressDSCP = 8             // regular sandboxes: CS1
	config.BuildSandboxEgressDSCP = DSCP(16) // template builds: CS2

	slot, err := NewSlot("dscp-pool-roundtrip-test", reserveNSTestIdx(t), config, NewNoopEgressProxy())
	require.NoError(t, err)

	require.NoError(t, slot.CreateNetwork(t.Context()))
	t.Cleanup(func() { _ = slot.RemoveNetwork() })

	pool := NewPool(2, 1, &fakeStorage{}, config)
	t.Cleanup(func() { _ = pool.Close(context.WithoutCancel(t.Context())) })

	requireSingleDSCPRule(t, slot, "0x08")

	pool.newSlots <- slot
	close(pool.newSlots) // Populate never runs here, and Close ranges over it.

	// A build takes the slot through Get, with real egress rules.
	netCfg := &orchestrator.SandboxNetworkConfig{
		Egress: &orchestrator.SandboxNetworkEgressConfig{DeniedCidrs: []string{"10.0.0.0/8"}},
	}
	got, err := pool.Get(t.Context(), netCfg, sandboxtypes.EgressClassBuild)
	require.NoError(t, err)
	require.Equal(t, slot.Idx, got.Idx)
	requireSingleDSCPRule(t, got, "0x10")
	require.True(t, got.firewallCustomRules.Load(), "custom egress rules must arm the firewall reset")

	require.NoError(t, pool.returnSlot(t.Context(), got, noopRelease, 0))
	requireSingleDSCPRule(t, got, "0x08")
	require.False(t, got.firewallCustomRules.Load(), "recycle must reset the firewall")

	select {
	case reused := <-pool.reusedSlots:
		require.Equal(t, slot.Idx, reused.Idx)
	default:
		t.Fatal("recycled slot must go back to the reuse queue, not be torn down")
	}

	// The next tenant is a regular sandbox: no change, and no duplicate rule.
	require.NoError(t, pool.configureSlot(t.Context(), slot, nil, sandboxtypes.EgressClassSandbox))
	requireSingleDSCPRule(t, slot, "0x08")
}
