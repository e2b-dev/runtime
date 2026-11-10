package sandbox

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

// restrictedGuest declares none of the lifecycle capabilities and its own
// boot budgets.
type restrictedGuest struct{ guest.Profile }

func (restrictedGuest) Name() string  { return "restricted" }
func (restrictedGuest) Label() string { return "restricted" }

func (restrictedGuest) Capabilities() guest.Capabilities {
	return guest.Capabilities{NoFilesystemOnlyPause: true, NoReclaim: true, NoFreeze: true, NoQuiesce: true, NoEnvdLiveUpgrade: true}
}

func (restrictedGuest) Boot() guest.BootConfig {
	return guest.BootConfig{NoBalloon: true, NoFilesystemRecovery: true, NoEnvdOfflineUpgrade: true}
}

func (restrictedGuest) EnvdInit() guest.EnvdInit {
	return guest.EnvdInit{ColdBootEnvdTimeout: 3 * time.Minute}
}

func (restrictedGuest) Reset() guest.Reset {
	return guest.Reset{TypicalBoot: 35 * time.Second}
}

func TestGuestOfDefaultsToTheDefaultProfile(t *testing.T) {
	t.Parallel()

	assert.Equal(t, guest.DefaultLabel, (*Config)(nil).GuestProfile().Label())
	assert.Equal(t, guest.DefaultLabel, newTestGuestSandbox(nil).Config.GuestProfile().Label())
	assert.Equal(t, "restricted", newTestGuestSandbox(restrictedGuest{guest.Default()}).Config.GuestProfile().Label())
}

// The default guest keeps the orchestrator's budgets; a profile replaces them.
func TestRebootBudgetsFollowTheProfile(t *testing.T) {
	t.Parallel()

	linux := newTestGuestSandbox(nil).Config
	restricted := newTestGuestSandbox(restrictedGuest{guest.Default()}).Config

	assert.Equal(t, defaultTypicalBoot, typicalBootFor(linux))
	assert.Equal(t, 35*time.Second, typicalBootFor(restricted))
	assert.Equal(t, rebootEnvdTimeout, rebootEnvdTimeoutFor(linux))
	assert.Equal(t, 3*time.Minute, rebootEnvdTimeoutFor(restricted))
	assert.Equal(t, rebootStartBudget+3*time.Minute, rebootAttemptTimeoutFor(restricted))
}

// powerOffPatchGuest restores on a Firecracker that takes the power-off
// declaration before a snapshot load.
type powerOffPatchGuest struct{ guest.Profile }

func (powerOffPatchGuest) Restore() guest.RestoreConfig {
	return guest.RestoreConfig{PowerOffPatch: true}
}

func powerOffEndOn(t *testing.T) *featureflags.Client {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.GuestPowerOffEndFlag.Key()).ValueForAll(ldvalue.Bool(true)))
	on, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = on.Close(context.WithoutCancel(t.Context())) })

	return on
}

// A cold boot gets the ACPI power-off device only with guest-poweroff-end on,
// whatever the guest.
func TestACPIPowerOffFollowsTheFlag(t *testing.T) {
	t.Parallel()

	on := powerOffEndOn(t)
	linux, restricted := newTestGuestSandbox(nil), newTestGuestSandbox(restrictedGuest{guest.Default()})

	assert.False(t, acpiPowerOffFor(t.Context(), nil, linux.Runtime, linux.Config))
	assert.True(t, acpiPowerOffFor(t.Context(), on, linux.Runtime, linux.Config))
	assert.False(t, acpiPowerOffFor(t.Context(), nil, restricted.Runtime, restricted.Config))
	assert.True(t, acpiPowerOffFor(t.Context(), on, restricted.Runtime, restricted.Config))
}

// The fleet's Firecracker answers a declaration before the load with 400, so
// the default guest's restore sends none, flag on or off.
func TestDefaultGuestRestoreSendsNoPowerOffDeclaration(t *testing.T) {
	t.Parallel()

	linux := newTestGuestSandbox(nil)
	assert.False(t, acpiPowerOffOnRestoreFor(t.Context(), nil, linux.Runtime, linux.Config))
	assert.False(t, acpiPowerOffOnRestoreFor(t.Context(), powerOffEndOn(t), linux.Runtime, linux.Config))
}

// A profile that asks for the declaration gets it on a restore with the flag on.
func TestPowerOffPatchProfileRestoreDeclares(t *testing.T) {
	t.Parallel()

	patching := newTestGuestSandbox(powerOffPatchGuest{guest.Default()})
	assert.False(t, acpiPowerOffOnRestoreFor(t.Context(), nil, patching.Runtime, patching.Config))
	assert.True(t, acpiPowerOffOnRestoreFor(t.Context(), powerOffEndOn(t), patching.Runtime, patching.Config))
}

// A profile without the host-side steps gets no pre-boot hook at all.
func TestRebootPreBootFollowsTheProfile(t *testing.T) {
	t.Parallel()

	f := &Factory{}
	restricted := newTestGuestSandbox(restrictedGuest{guest.Default()})
	recovery, swap := f.rebootPreBoot(t.Context(), restricted.Config, restricted.Runtime, false, false, func(rootfs.RecoverOutcome) {})
	assert.Nil(t, recovery)
	assert.Nil(t, swap)
}

// A guest without the quiesce refuses the filesystem-only capture, and the
// hooks that its capabilities or its missing balloon turn off return before
// reaching envd or the balloon, with every flag that would drive them on.
func TestCapabilitiesGateTheGuestHooks(t *testing.T) {
	t.Parallel()

	var envdRequests atomic.Int32
	envd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		envdRequests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(envd.Close)

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.FreezeUserCgroupFlag.Key()).ValueForAll(ldvalue.Bool(true)))
	td.Update(td.Flag(featureflags.CollapseEnvdHeapFlag.Key()).ValueForAll(ldvalue.Bool(true)))
	td.Update(td.Flag(featureflags.FreePageHintingConfig.Key()).ValueForAll(ldvalue.Parse([]byte(`{"periodic":{"interval":1000}}`))))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	sbx := newTestGuestSandbox(restrictedGuest{guest.Default()})
	// The fixture's Firecracker cannot hint, so a periodic loop past the guard
	// records skipped-fc-unsupported instead of reaching a process.
	require.False(t, fc.FCSupportsFreePageHinting(sbx.Config.FirecrackerConfig.FirecrackerVersion))
	sbx.internalConfig.envdServerURLOverride = envd.URL
	sbx.featureFlags = ff
	hints := &outcomes{}
	sbx.fphObserve = hints.hook
	balloon := &fakeRunner{}

	frozen, err := sbx.guestPrepareFsForPause(t.Context(), NewCleanup(), nil)
	require.Error(t, err)
	assert.False(t, frozen)
	assert.Contains(t, err.Error(), "restricted guest")

	done := make(chan struct{})
	go func() {
		defer close(done)
		sbx.bestEffortReclaim(t.Context(), false)
		sbx.bestEffortFreeze(t.Context())
		sbx.bestEffortUnfreeze(t.Context())
		sbx.prePauseHintDrain(t.Context(), prePauseCfg(time.Second), balloon)
		sbx.runPeriodicHinting(t.Context())
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a hook the profile does not declare did not return")
	}

	assert.Zero(t, envdRequests.Load(), "a hook the profile does not declare reached envd")
	assert.Zero(t, balloon.drains.Load(), "the pre-pause drain reached the balloon")
	assert.Empty(t, hints.list(), "a hinting run was recorded")
}

// vcpuCappedGuest onlines every vCPU of its VM on a cold boot.
type vcpuCappedGuest struct{ guest.Profile }

func (vcpuCappedGuest) Boot() guest.BootConfig { return guest.BootConfig{RefuseVcpusAboveTarget: true} }

// A reboot in place the guest cannot take is refused before the slot and the
// overlay move: the old lifecycle still owns them, and its Close returns them.
func TestTakeForBootRefusesBeforeTheHandover(t *testing.T) { //nolint:paralleltest // its Close counts in the closed-sandbox metric the map tests read
	f := newRebootFixture(t, time.Now().Add(time.Hour))
	f.old.Config.FirecrackerConfig.Guest = vcpuCappedGuest{guest.Default()}
	f.old.Config.Vcpu = 2
	f.old.Config.ConfiguredVmVcpus = 4

	h, err := f.old.takeForBoot(t.Context())
	require.ErrorIs(t, err, ErrColdBootVcpus)
	assert.Nil(t, h)
	assert.Equal(t, [2]int32{0, 0}, f.releases(), "nothing moved and nothing was released")

	require.NoError(t, f.old.Close(t.Context()))
	assert.Equal(t, [2]int32{1, 1}, f.releases(), "the old lifecycle returned both")
}

// crashReportingGuest names a crash counter group.
type crashReportingGuest struct{ guest.Profile }

func (crashReportingGuest) Reset() guest.Reset {
	return guest.Reset{Crash: guest.CrashReport{Group: "pvcrash", CountField: "crash_count", CodeField: "crash_code"}}
}

// An exit waits for its final guest counters only when something reads them:
// with every flag off the default guest's stop does not wait, as before the
// counters.
func TestExitCountersWanted(t *testing.T) {
	t.Parallel()

	off, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	t.Cleanup(func() { _ = off.Close(context.WithoutCancel(t.Context())) })

	linux := newTestGuestSandbox(nil)
	assert.False(t, (&Factory{featureFlags: off}).exitCountersWanted(t.Context(), linux.Runtime, linux.Config))
	assert.False(t, (&Factory{}).exitCountersWanted(t.Context(), linux.Runtime, linux.Config))

	crashing := newTestGuestSandbox(crashReportingGuest{guest.Default()})
	assert.True(t, (&Factory{featureFlags: off}).exitCountersWanted(t.Context(), crashing.Runtime, crashing.Config))

	for _, flag := range []featureflags.BoolFlag{featureflags.GuestRebootInPlaceFlag, featureflags.GuestPowerOffEndFlag, featureflags.SandboxEndedOnNodeFlag} {
		td := ldtestdata.DataSource()
		td.Update(td.Flag(flag.Key()).ValueForAll(ldvalue.Bool(true)))
		on, err := featureflags.NewClientWithDatasource(td)
		require.NoError(t, err)
		t.Cleanup(func() { _ = on.Close(context.WithoutCancel(t.Context())) })

		assert.True(t, (&Factory{featureFlags: on}).exitCountersWanted(t.Context(), linux.Runtime, linux.Config), flag.Key())
	}
}
