//go:build linux

package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// A guest that powers off while the envd probe waits is kept with its disk,
// not gone: the pause is refused retryably, so the API keeps the record and
// its next pause captures the held disk. A pause that allows an unresponsive
// agent is refused the same way: proceeding would stamp a memory mark on a
// capture that has no memory left. The handler orders the events without a
// sleep: Pause read the power-off state before its probe reached envd, and
// the lifecycle installs the hold before the answer.
func TestPause_PowerOffDuringTheProbeKeepsTheHeldSandbox(t *testing.T) {
	t.Parallel()

	for name, c := range map[string]struct {
		allow bool
		slot  int
	}{
		"strict":  {allow: false, slot: 91},
		"allowed": {allow: true, slot: 97},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s, _ := admissionMetricsServer(t, -1)
			s.featureFlags = envdProbeFlagClient(t, new(envdProbeMs))
			sbx := envdProbeSandbox(t, "sbx-poweroff-probe-"+name, c.slot)
			require.NoError(t, s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx))

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				sbx.MarkExitedForTest(nil)
				sbx.HoldPoweredOff()
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			t.Cleanup(srv.Close)
			sbx.SetEnvdServerURLForTest(srv.URL)

			_, err := s.Pause(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: sbx.Runtime.SandboxID, AllowEnvdUnresponsive: c.allow})
			requireEnvdUnresponsive(t, err)
			live, ok := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
			require.True(t, ok, "the held sandbox stays live for the pause that follows")
			assert.Same(t, sbx, live)
			assert.True(t, sbx.GuestPoweredOff(), "the hold stays for the timeout action")
		})
	}
}

// envdProbeWithPowerOffHoldFlagClient enables the envd probe and the
// power-off hold.
func envdProbeWithPowerOffHoldFlagClient(t *testing.T) *featureflags.Client {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.PauseEnvdHealthTimeoutMs.Key()).ValueForAll(ldvalue.Int(envdProbeMs)))
	td.Update(td.Flag(featureflags.GuestPowerOffEndFlag.Key()).ValueForAll(ldvalue.Bool(true)))

	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	return ff
}

// The exit fires before the lifecycle installs the hold. With the hold's flag
// on, the listed execution is not gone yet: the pause is refused retryably
// and the record stays for the lifecycle's decision. With the flag off an
// exited execution is answered not found, as on a default guest
// (TestPause_EnvdProbeMissAfterTheVMExitedIsNotFound).
func TestPause_ExitDuringTheProbeBeforeTheHoldIsRetryableWithTheHoldFlagOn(t *testing.T) {
	t.Parallel()

	s, _ := admissionMetricsServer(t, -1)
	s.featureFlags = envdProbeWithPowerOffHoldFlagClient(t)
	sbx := envdProbeSandbox(t, "sbx-poweroff-exit", 92)
	require.NoError(t, s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		sbx.MarkExitedForTest(nil)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	sbx.SetEnvdServerURLForTest(srv.URL)

	_, err := s.Pause(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: sbx.Runtime.SandboxID})
	requireEnvdUnresponsive(t, err)
	live, ok := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
	require.True(t, ok, "the exited execution stays listed until the lifecycle decides")
	assert.Same(t, sbx, live)
}

// A process that took the ACPI power-off device is held by the lifecycle
// whatever the flag says now, so its exit during the probe is answered
// retryably with both flags off too.
func TestPause_ExitDuringTheProbeWithTheDeviceTakenIsRetryableWithTheFlagsOff(t *testing.T) {
	t.Parallel()

	s, _ := admissionMetricsServer(t, -1)
	s.featureFlags = envdProbeFlagClient(t, new(envdProbeMs))
	sbx := envdProbeSandbox(t, "sbx-poweroff-device", 93)
	sbx.DeclarePowerOffDeviceForTest()
	require.True(t, sbx.PowerOffDeviceDeclared())
	require.NoError(t, s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		sbx.MarkExitedForTest(nil)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	sbx.SetEnvdServerURLForTest(srv.URL)

	_, err := s.Pause(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: sbx.Runtime.SandboxID})
	requireEnvdUnresponsive(t, err)
	live, ok := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
	require.True(t, ok, "the exited execution stays listed until the lifecycle decides")
	assert.Same(t, sbx, live)
}
