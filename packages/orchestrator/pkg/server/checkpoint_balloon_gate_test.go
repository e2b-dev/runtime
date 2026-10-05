//go:build linux

package server

import (
	"context"
	"errors"
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// The cheap half names the first failing condition in order and does not
// consult the Firecracker check (which logs when it refuses) before its turn.
func TestInPlaceEarlyRoute(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		syncWP, flagOn, fcOK bool
		want                 string
		fcAsked              bool
	}{
		"all clear, balloon decides": {true, true, true, "", true},
		"sync-wp off first":          {false, true, true, routeSyncWPOff, false},
		"flag off":                   {true, false, true, routeFlagOff, false},
		"firecracker predates it":    {true, true, false, routeFCUnsupported, true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			asked := false
			fcCheck := func() bool {
				asked = true

				return tc.fcOK
			}
			assert.Equal(t, tc.want, inPlaceEarlyRoute(tc.syncWP, tc.flagOn, fcCheck))
			assert.Equal(t, tc.fcAsked, asked)
		})
	}
}

// The balloon half admits an allow-list. Reporting and unknown matter only
// while the deferred export would pause reporting; with it off they go in
// place, with it on only the re-admit flag lets them. Each flag is consulted
// only where it decides, and a mode this build does not know fails closed
// whatever the flags say.
func TestInPlaceBalloonRoute(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		mode            userfaultfd.BalloonMode
		deferred, admit bool
		want            string
		deferredAsked   bool
		admitAsked      bool
	}{
		"hinting":                       {userfaultfd.BalloonModeHinting, true, false, routeInPlace, false, false},
		"no balloon":                    {userfaultfd.BalloonModeNone, true, false, routeInPlace, false, false},
		"reporting, deferred export":    {userfaultfd.BalloonModeReporting, true, false, routeBalloonReporting, true, true},
		"reporting, synchronous export": {userfaultfd.BalloonModeReporting, false, false, routeInPlace, true, false},
		"reporting re-admitted":         {userfaultfd.BalloonModeReporting, true, true, routeInPlace, true, true},
		"unknown fails closed":          {userfaultfd.BalloonModeUnknown, true, false, routeBalloonUnknown, true, true},
		"unknown, synchronous export":   {userfaultfd.BalloonModeUnknown, false, false, routeInPlace, true, false},
		"unknown re-admitted":           {userfaultfd.BalloonModeUnknown, true, true, routeInPlace, true, true},
		"unrecognised stays closed":     {userfaultfd.BalloonMode(200), false, true, routeBalloonUnknown, false, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			deferredAsked, admitAsked := false, false
			deferred := func() bool {
				deferredAsked = true

				return tc.deferred
			}
			admit := func() bool {
				admitAsked = true

				return tc.admit
			}
			assert.Equal(t, tc.want, inPlaceBalloonRoute(tc.mode, deferred, admit))
			assert.Equal(t, tc.deferredAsked, deferredAsked, "defer-memory-export is evaluated only where it decides")
			assert.Equal(t, tc.admitAsked, admitAsked, "the re-admit flag is evaluated only where it decides")
		})
	}
}

// routeTestServer is a Server with the deferred export on (the configuration
// the exclusion is for), the in-place and re-admit flags as given, and a
// metric reader on the checkpoint counter and duration.
func routeTestServer(t *testing.T, inPlace, admitReporting bool) (*Server, *sdkmetric.ManualReader) {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.InPlaceCheckpointFlag.Key()).ValueForAll(ldvalue.Bool(inPlace)))
	td.Update(td.Flag(featureflags.DeferMemoryExportFlag.Key()).ValueForAll(ldvalue.Bool(true)))
	td.Update(td.Flag(featureflags.InPlaceCheckpointReportingFlag.Key()).ValueForAll(ldvalue.Bool(admitReporting)))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/server")

	return &Server{
		info:                      &service.ServiceInfo{},
		sandboxFactory:            &sandbox.Factory{Sandboxes: sandbox.NewSandboxesMap()},
		startingSandboxes:         utils.Must(utils.NewAdjustableSemaphore(1)),
		sandboxCheckpointCounter:  utils.Must(telemetry.GetCounter(meter, telemetry.OrchestratorSandboxCheckpointCounterName)),
		sandboxCheckpointDuration: utils.Must(telemetry.GetHistogram(meter, telemetry.CheckpointDurationName)),
		featureFlags:              ff,
	}, reader
}

// routeTestSandbox is sync-WP on a Firecracker with in-place support, so the
// cheap half passes and the balloon decides; read is what the device answers.
func routeTestSandbox(s *Server, id string, read func(context.Context) (fc.BalloonCaps, error)) *sandbox.Sandbox {
	sbx := sandbox.NewBalloonTestSandbox(id, "v1.14-0.2.1", read)
	sbx.SetSyncWPForTest(true)
	sbx.SetFeatureFlagsForTest(s.featureFlags)

	return sbx
}

func fixedCaps(caps fc.BalloonCaps, err error) func(context.Context) (fc.BalloonCaps, error) {
	return func(context.Context) (fc.BalloonCaps, error) { return caps, err }
}

// The wiring from the sandbox to the gate, end to end through the flags: the
// device's answer decides and is the mode reported next to the route, a
// stamp the device contradicts is corrected, a failed read fails closed and
// the re-admit flag is the way back for both, and the device is not read at
// all for a route the cheap half already decided.
func TestCheckpointRoute(t *testing.T) {
	t.Parallel()

	t.Run("reporting device excluded, re-admitted by the flag", func(t *testing.T) {
		t.Parallel()
		s, _ := routeTestServer(t, true, false)
		sbx := routeTestSandbox(s, "route-reporting", fixedCaps(fc.BalloonCaps{Reporting: true}, nil))
		route, mode := s.checkpointRoute(t.Context(), sbx)
		assert.Equal(t, routeBalloonReporting, route)
		assert.Equal(t, userfaultfd.BalloonModeReporting, mode)
		s2, _ := routeTestServer(t, true, true)
		sbx2 := routeTestSandbox(s2, "route-reporting-admitted", fixedCaps(fc.BalloonCaps{Reporting: true}, nil))
		route, _ = s2.checkpointRoute(t.Context(), sbx2)
		assert.Equal(t, routeInPlace, route)
	})

	t.Run("hinting device goes in place and corrects a stale stamp", func(t *testing.T) {
		t.Parallel()
		s, _ := routeTestServer(t, true, false)
		sbx := routeTestSandbox(s, "route-stale-stamp", fixedCaps(fc.BalloonCaps{Hinting: true}, nil))
		sbx.StampBalloonMode(userfaultfd.BalloonModeReporting)
		route, mode := s.checkpointRoute(t.Context(), sbx)
		assert.Equal(t, routeInPlace, route)
		assert.Equal(t, userfaultfd.BalloonModeHinting, mode)
		assert.Equal(t, "hinting", sbx.BalloonMode(), "the device's answer replaces the stamp")
	})

	t.Run("unreadable device fails closed, flag is the way back", func(t *testing.T) {
		t.Parallel()
		s, _ := routeTestServer(t, true, false)
		sbx := routeTestSandbox(s, "route-unread", fixedCaps(fc.BalloonCaps{}, errors.New("socket closed")))
		sbx.StampBalloonMode(userfaultfd.BalloonModeHinting)
		route, mode := s.checkpointRoute(t.Context(), sbx)
		assert.Equal(t, routeBalloonUnknown, route, "a stamp is a label, never a stand-in for the device")
		assert.Equal(t, userfaultfd.BalloonModeUnknown, mode)
		assert.Equal(t, "hinting", sbx.BalloonMode(), "a failed read leaves the stamp alone")
		s2, _ := routeTestServer(t, true, true)
		sbx2 := routeTestSandbox(s2, "route-unread-admitted", fixedCaps(fc.BalloonCaps{}, errors.New("socket closed")))
		route, _ = s2.checkpointRoute(t.Context(), sbx2)
		assert.Equal(t, routeInPlace, route)
	})

	t.Run("flag off decides before any device read", func(t *testing.T) {
		t.Parallel()
		reads := 0
		s, _ := routeTestServer(t, false, false)
		sbx := routeTestSandbox(s, "route-flag-off", func(context.Context) (fc.BalloonCaps, error) {
			reads++

			return fc.BalloonCaps{Reporting: true}, nil
		})
		sbx.StampBalloonMode(userfaultfd.BalloonModeHinting)
		route, mode := s.checkpointRoute(t.Context(), sbx)
		assert.Equal(t, routeFlagOff, route)
		assert.Equal(t, userfaultfd.BalloonModeHinting, mode, "an early route carries the stamp as its label")
		assert.Zero(t, reads)
	})

	t.Run("no process stays unknown", func(t *testing.T) {
		t.Parallel()
		sbx := &sandbox.Sandbox{
			Metadata: &sandbox.Metadata{
				Config:  sandbox.NewConfig(sandbox.Config{FirecrackerConfig: fc.Config{FirecrackerVersion: "v1.14-0.2.1"}}),
				Runtime: sandboxtypes.RuntimeMetadata{SandboxID: "route-no-process"},
			},
			Resources: &sandbox.Resources{},
		}
		sbx.SetSyncWPForTest(true)
		s, _ := routeTestServer(t, true, false)
		sbx.SetFeatureFlagsForTest(s.featureFlags)
		route, mode := s.checkpointRoute(t.Context(), sbx)
		assert.Equal(t, routeBalloonUnknown, route)
		assert.Equal(t, userfaultfd.BalloonModeUnknown, mode)
	})

	t.Run("synchronous export admits a reporting balloon", func(t *testing.T) {
		t.Parallel()
		s, _ := routeTestServer(t, true, false)
		td := ldtestdata.DataSource()
		td.Update(td.Flag(featureflags.InPlaceCheckpointFlag.Key()).ValueForAll(ldvalue.Bool(true)))
		td.Update(td.Flag(featureflags.DeferMemoryExportFlag.Key()).ValueForAll(ldvalue.Bool(false)))
		ff, err := featureflags.NewClientWithDatasource(td)
		require.NoError(t, err)
		t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })
		s.featureFlags = ff
		sbx := routeTestSandbox(s, "route-sync-export", fixedCaps(fc.BalloonCaps{Reporting: true}, nil))
		route, mode := s.checkpointRoute(t.Context(), sbx)
		assert.Equal(t, routeInPlace, route, "with the deferred export off nothing pauses reporting")
		assert.Equal(t, userfaultfd.BalloonModeReporting, mode)
	})
}

func checkpointCounterAttrs(t *testing.T, reader *sdkmetric.ManualReader) []map[string]string {
	t.Helper()

	return checkpointMetricAttrs(t, reader, string(telemetry.OrchestratorSandboxCheckpointCounterName))
}

func checkpointDurationAttrs(t *testing.T, reader *sdkmetric.ManualReader) []map[string]string {
	t.Helper()

	return checkpointMetricAttrs(t, reader, string(telemetry.CheckpointDurationName))
}

// checkpointMetricAttrs returns the attribute set of every data point the
// named checkpoint metric holds, whether it is the counter or the duration
// histogram.
func checkpointMetricAttrs(t *testing.T, reader *sdkmetric.ManualReader, name string) []map[string]string {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	var out []map[string]string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range data.DataPoints {
					out = append(out, attrsAsMap(t, p.Attributes))
				}
			case metricdata.Histogram[int64]:
				for _, p := range data.DataPoints {
					require.EqualValues(t, 1, p.Count, "one RPC, one sample")
					out = append(out, attrsAsMap(t, p.Attributes))
				}
			default:
				t.Fatalf("%s: unexpected data type %T", name, m.Data)
			}
		}
	}

	return out
}

// liveRouteSandbox is a routeTestSandbox registered as running, with the
// envd version and network slot the Checkpoint RPC checks on its way in.
func liveRouteSandbox(t *testing.T, s *Server, id string, slotIdx int, read func(context.Context) (fc.BalloonCaps, error)) *sandbox.Sandbox {
	t.Helper()

	slot, err := network.NewSlot("test", slotIdx, network.Config{}, network.NoopEgressProxy{})
	require.NoError(t, err)
	sbx := routeTestSandbox(s, id, read)
	sbx.LifecycleID = "lifecycle-1"
	sbx.Resources.Slot = slot
	sbx.Config.Envd.Version = "9.9.9"
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	return sbx
}

// The labels reach the counter and the duration histogram: a Checkpoint RPC
// that runs past admission records route, balloon_mode and deferred from the
// one decision the span carries. The
// in-place path is refused by its own in-flight guard, so the RPC completes
// without a template; the resume-fresh path cannot be driven to completion
// here, and its labels come from the same variables.
func TestCheckpoint_RecordsRouteAndBalloonMode(t *testing.T) {
	t.Parallel()
	s, reader := routeTestServer(t, true, false)
	sbx := liveRouteSandbox(t, s, "ckpt-labels", 41, fixedCaps(fc.BalloonCaps{Hinting: true}, nil))
	require.True(t, sbx.BeginInPlaceCheckpoint(), "hold the in-place guard so the RPC completes on the guard's refusal")

	_, ckptErr := s.Checkpoint(t.Context(), &orchestrator.SandboxCheckpointRequest{SandboxId: "ckpt-labels"})
	require.Error(t, ckptErr)
	st, ok := status.FromError(ckptErr)
	require.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, st.Code())

	want := map[string]string{"in_place": "true", "fs_only": "false", "route": routeInPlace, "balloon_mode": "hinting", "deferred": "false", "success": "false"}
	points := checkpointCounterAttrs(t, reader)
	require.Len(t, points, 1)
	assert.Equal(t, want, points[0])
	durations := checkpointDurationAttrs(t, reader)
	require.Len(t, durations, 1, "the duration histogram records the same RPC")
	assert.Equal(t, want, durations[0])
}

// A caller that is gone by the time the route is decided is answered with its
// context's status, never routed into a checkpoint path under a dead context.
func TestCheckpoint_CancelledDuringRouteIsNotCheckpointed(t *testing.T) {
	t.Parallel()
	s, reader := routeTestServer(t, true, false)
	ctx, cancel := context.WithCancel(t.Context())
	liveRouteSandbox(t, s, "ckpt-cancel", 42, func(context.Context) (fc.BalloonCaps, error) {
		cancel()

		return fc.BalloonCaps{}, context.Canceled
	})

	_, ckptErr := s.Checkpoint(ctx, &orchestrator.SandboxCheckpointRequest{SandboxId: "ckpt-cancel"})
	require.Error(t, ckptErr)
	st, ok := status.FromError(ckptErr)
	require.True(t, ok)
	assert.Equal(t, codes.Canceled, st.Code())
	assert.Empty(t, checkpointCounterAttrs(t, reader), "no path ran, so nothing was counted")
	assert.Empty(t, checkpointDurationAttrs(t, reader), "no path ran, so no duration was recorded")
	_, live := s.sandboxFactory.Sandboxes.Get("ckpt-cancel")
	assert.True(t, live, "the sandbox was never marked stopping")
}

// A filesystem-only checkpoint is routed by its own flag: with
// in-place-checkpoint off and the balloon unreadable it still goes in place,
// and the device is never consulted. The in-flight guard refuses the RPC after
// the decision, so it completes without a template.
func TestCheckpoint_FilesystemOnlyGoesInPlaceOnItsOwnFlag(t *testing.T) {
	t.Parallel()
	s, reader := routeTestServer(t, false, false)
	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.InPlaceCheckpointFlag.Key()).ValueForAll(ldvalue.Bool(false)))
	td.Update(td.Flag(featureflags.FilesystemOnlyCheckpointFlag.Key()).ValueForAll(ldvalue.Bool(true)))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })
	s.featureFlags = ff
	sbx := liveRouteSandbox(t, s, "ckpt-fs-only", 43, func(context.Context) (fc.BalloonCaps, error) {
		t.Error("a filesystem-only checkpoint must not read the balloon")

		return fc.BalloonCaps{}, nil
	})
	require.True(t, sbx.BeginInPlaceCheckpoint())

	_, ckptErr := s.Checkpoint(t.Context(), &orchestrator.SandboxCheckpointRequest{SandboxId: "ckpt-fs-only", FilesystemOnly: true})
	require.Error(t, ckptErr)
	st, ok := status.FromError(ckptErr)
	require.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
	assert.Contains(t, st.Message(), "already in progress", "the RPC must reach the in-place path, not be refused by a flag")

	points := checkpointCounterAttrs(t, reader)
	require.Len(t, points, 1)
	assert.Equal(t, "true", points[0]["in_place"])
	assert.Equal(t, "true", points[0]["fs_only"])
	assert.Equal(t, routeInPlace, points[0]["route"])
}
