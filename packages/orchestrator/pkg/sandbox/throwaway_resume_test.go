//go:build linux

package sandbox

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestThrowawayResumeOptions locks the resume option set the prefetch harvest
// uses (via sandbox.ThrowawayResumeOptions): the throwaway must be BOTH network
// isolated AND kept out of the live registry. If a future edit drops either
// option from the set, the harvested throwaway would silently regain egress or
// pollute the registry — so assert both flags result from applying the set.
func TestThrowawayResumeOptions(t *testing.T) {
	t.Parallel()

	var o resumeOptions
	for _, opt := range ThrowawayResumeOptions() {
		opt(&o)
	}

	require.True(t, o.denyEgress, "throwaway must deny egress")
	require.True(t, o.skipLiveRegistration, "throwaway must stay out of the live registry")
	require.True(t, o.skipThaw, "throwaway must ask envd to skip the thaw")
	require.False(t, o.describesCustomerStart(),
		"a throwaway resume must not be counted in the fleet's startup numbers")
}

// TestDescribesCustomerStart pins the population rule every start-path counter shares.
//
// The prefetch harvest resumes EVERY memory pause with a copy of the original config, so a
// counter that includes throwaways and one that does not cannot be divided by each other —
// and these counters are read as ratios. One definition, asserted in both directions, because
// the default (a real resume) is the case an inverted condition would silently break.
func TestDescribesCustomerStart(t *testing.T) {
	t.Parallel()

	var plain resumeOptions
	require.True(t, plain.describesCustomerStart(), "a plain resume is a customer start")

	var throwaway resumeOptions
	WithoutLiveRegistration()(&throwaway)
	require.False(t, throwaway.describesCustomerStart())

	// Deferred registration is NOT a throwaway: the sandbox does go live, just later, so
	// its start belongs in the numbers.
	var deferred resumeOptions
	WithDeferredLiveRegistration()(&deferred)
	require.True(t, deferred.describesCustomerStart(),
		"a deferred promotion still becomes a live customer sandbox")
}

// TestWithoutLiveRegistrationKeepsSandboxOutOfLiveMap locks the invariant the
// WithoutLiveRegistration option relies on. When set, ResumeSandbox assigns the
// network IP (so its own teardown stays symmetric) but never calls MarkRunning,
// so a throwaway must NOT appear in the live registry — it must not be
// addressable, counted in the node's reported allocation, or tracked as a
// lifecycle — while staying reachable by host IP. A normally-registered sandbox,
// by contrast, is in the live registry. This is what keeps the harvest throwaway
// from inflating placement metrics or emitting phantom per-sandbox telemetry.
func TestWithoutLiveRegistrationKeepsSandboxOutOfLiveMap(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()

	// Throwaway: AssignNetwork only (what ResumeSandbox does when
	// skipLiveRegistration is set), without MarkRunning.
	throwaway := testMapSandbox(t, "harvest-lifecycle")
	throwaway.Runtime.SandboxID = "prefetch-harvest-sandbox-1"
	sandboxes.AssignNetwork(t.Context(), throwaway)

	_, ok := sandboxes.Get(throwaway.Runtime.SandboxID)
	require.False(t, ok, "throwaway must not be addressable via the live map")
	require.Empty(t, sandboxes.Items(), "throwaway must not be counted as running")
	require.Zero(t, sandboxes.Count())
	require.Empty(t, sandboxes.LifecycleItems(), "throwaway must not be tracked as a lifecycle")

	// The network IP mapping IS assigned, so teardown (NetworkReleased) stays symmetric.
	found, err := sandboxes.GetByHostPort(throwaway.Slot.HostIPString() + ":49983")
	require.NoError(t, err)
	require.Equal(t, throwaway, found)

	// Contrast: a normally-registered sandbox appears in the live registry.
	live := testMapSandbox(t, "live-lifecycle")
	live.Runtime.SandboxID = "sandbox-live"
	sandboxes.MarkRunning(t.Context(), live)
	_, ok = sandboxes.Get(live.Runtime.SandboxID)
	require.True(t, ok, "a normally-registered sandbox must be in the live map")
	require.Len(t, sandboxes.Items(), 1, "only the live sandbox is counted")
}
