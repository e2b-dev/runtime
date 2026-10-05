//go:build linux

package metrics

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func testTracker(t *testing.T, scan func(context.Context) ([]firecrackerProcess, error), metadata func() map[string]sandbox.ProcessMetadata) (*FirecrackerTracker, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })
	c, err := newFirecrackerTracker(provider, scan, metadata)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close(context.WithoutCancel(t.Context()))) })

	return c, reader
}

func trackerValues(t *testing.T, reader *sdkmetric.ManualReader) map[telemetry.GaugeIntType]int64 {
	t.Helper()
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &data))
	values := make(map[telemetry.GaugeIntType]int64)
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			require.NotEmpty(t, m.Description)
			require.NotEmpty(t, m.Unit)
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok)
			for _, point := range gauge.DataPoints {
				require.Zero(t, point.Attributes.Len(), "process identities must not become metric labels")
				values[telemetry.GaugeIntType(m.Name)] = point.Value
			}
		}
	}

	return values
}

func TestTrackerClassifiesActualProcesses(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	metadata := map[string]sandbox.ProcessMetadata{
		"tracked":          {SandboxID: "same-id", Tracked: true, StartedAt: now.Add(-2 * time.Hour), MaxLengthHours: 1},
		"network-only":     {SandboxID: "same-id", StartedAt: now.Add(-2 * time.Hour), MaxLengthHours: 1},
		"starting":         {StartedAt: now, MaxLengthHours: 1},
		"closing":          {Tracked: true, StartedAt: now.Add(-time.Minute), MaxLengthHours: 1},
		"temporary":        {Temporary: true, StartedAt: now.Add(-3 * time.Hour), MaxLengthHours: 1},
		"leaked-temporary": {Temporary: true},
		"build":            {Build: true, Tracked: true},
	}
	var processes []firecrackerProcess
	for i, socket := range []string{"tracked", "network-only", "unknown", "starting", "closing", "temporary", "leaked-temporary", "build"} {
		age := 6 * time.Minute
		if socket == "tracked" || socket == "starting" {
			age = time.Second
		}
		if socket == "temporary" {
			age = time.Minute
		}
		processes = append(processes, firecrackerProcess{identity: processIdentity{pid: i + 1, startTicks: 1}, socket: socket, age: age})
	}
	scans := 0
	c, reader := testTracker(t, func(context.Context) ([]firecrackerProcess, error) {
		scans++

		return processes, nil
	}, func() map[string]sandbox.ProcessMetadata { return metadata })
	c.sample(t.Context(), now)
	values := trackerValues(t, reader)
	require.Contains(t, values, telemetry.FirecrackerTrackerLastSuccessAge)
	require.GreaterOrEqual(t, values[telemetry.FirecrackerTrackerLastSuccessAge], int64(0))
	delete(values, telemetry.FirecrackerTrackerLastSuccessAge)
	require.Equal(t, map[telemetry.GaugeIntType]int64{
		telemetry.FirecrackerProcesses: 8, telemetry.FirecrackerProcessesUntracked: 3,
		telemetry.FirecrackerProcessesOverMaxLength: 2, telemetry.FirecrackerProcessesUnknownMaxLength: 1,
		telemetry.FirecrackerTrackerSuccess: 1,
	}, values)
	trackerValues(t, reader)
	require.Equal(t, 1, scans, "export callbacks must never scan /proc")
}

func TestTrackerRetainsLimitsButNotOwnershipAndRejectsPIDReuse(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	processes := []firecrackerProcess{{identity: processIdentity{pid: 42, startTicks: 100}, socket: "socket", age: 6 * time.Minute}}
	metadata := map[string]sandbox.ProcessMetadata{"socket": {Tracked: true, StartedAt: now.Add(-2 * time.Hour), MaxLengthHours: 1}}
	c, reader := testTracker(t, func(context.Context) ([]firecrackerProcess, error) { return processes, nil }, func() map[string]sandbox.ProcessMetadata { return metadata })
	c.sample(t.Context(), now)
	require.EqualValues(t, 0, trackerValues(t, reader)[telemetry.FirecrackerProcessesUntracked])
	metadata = nil
	c.sample(t.Context(), now.Add(time.Minute))
	values := trackerValues(t, reader)
	require.EqualValues(t, 1, values[telemetry.FirecrackerProcessesUntracked])
	require.EqualValues(t, 1, values[telemetry.FirecrackerProcessesOverMaxLength])
	processes[0].identity.startTicks++
	c.sample(t.Context(), now.Add(2*time.Minute))
	values = trackerValues(t, reader)
	require.EqualValues(t, 0, values[telemetry.FirecrackerProcessesOverMaxLength])
	require.EqualValues(t, 1, values[telemetry.FirecrackerProcessesUnknownMaxLength])
	processes = nil
	c.sample(t.Context(), now.Add(3*time.Minute))
	require.Empty(t, c.seen)
	require.EqualValues(t, 0, trackerValues(t, reader)[telemetry.FirecrackerProcesses])
}

func TestTrackerFailureIsNotHealthyZero(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var scanErr error
		c, reader := testTracker(t, func(context.Context) ([]firecrackerProcess, error) { return nil, scanErr }, func() map[string]sandbox.ProcessMetadata { return nil })
		values := trackerValues(t, reader)
		require.EqualValues(t, 0, values[telemetry.FirecrackerTrackerSuccess])
		require.NotContains(t, values, telemetry.FirecrackerProcesses)
		require.NotContains(t, values, telemetry.FirecrackerTrackerLastSuccessAge)

		c.sample(t.Context(), time.Now())
		values = trackerValues(t, reader)
		require.Contains(t, values, telemetry.FirecrackerProcesses)
		require.EqualValues(t, 0, values[telemetry.FirecrackerProcesses])
		require.EqualValues(t, 0, values[telemetry.FirecrackerTrackerLastSuccessAge])

		time.Sleep(45 * time.Second)
		values = trackerValues(t, reader)
		require.EqualValues(t, 1, values[telemetry.FirecrackerTrackerSuccess])
		require.EqualValues(t, 45, values[telemetry.FirecrackerTrackerLastSuccessAge], "age grows without another scan")

		scanErr = errors.New("process table inaccessible")
		c.sample(t.Context(), time.Now())
		values = trackerValues(t, reader)
		require.NotContains(t, values, telemetry.FirecrackerProcesses)
		require.EqualValues(t, 0, values[telemetry.FirecrackerTrackerSuccess])
		require.EqualValues(t, 45, values[telemetry.FirecrackerTrackerLastSuccessAge])

		time.Sleep(15 * time.Second)
		require.EqualValues(t, 60, trackerValues(t, reader)[telemetry.FirecrackerTrackerLastSuccessAge])
		scanErr = nil
		c.sample(t.Context(), time.Now())
		values = trackerValues(t, reader)
		require.EqualValues(t, 1, values[telemetry.FirecrackerTrackerSuccess])
		require.EqualValues(t, 0, values[telemetry.FirecrackerTrackerLastSuccessAge])
	})
}

func TestTrackerReconcilesLifecycleChangesAcrossScan(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	tracked := true
	metadata := func() map[string]sandbox.ProcessMetadata {
		return map[string]sandbox.ProcessMetadata{"socket": {Tracked: tracked, StartedAt: now, MaxLengthHours: 1}}
	}
	c, reader := testTracker(t, func(context.Context) ([]firecrackerProcess, error) {
		tracked = false

		return []firecrackerProcess{{identity: processIdentity{pid: 1}, socket: "socket", age: time.Hour}}, nil
	}, metadata)
	c.sample(t.Context(), now)
	require.EqualValues(t, 0, trackerValues(t, reader)[telemetry.FirecrackerProcessesUntracked])
	c.sample(t.Context(), now.Add(time.Minute))
	require.EqualValues(t, 1, trackerValues(t, reader)[telemetry.FirecrackerProcessesUntracked])
}

func TestTrackerUsesHardMaximumBoundary(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	meta := map[string]sandbox.ProcessMetadata{"socket": {Tracked: true, StartedAt: now.Add(-time.Hour), MaxLengthHours: 1}}
	c, reader := testTracker(t, func(context.Context) ([]firecrackerProcess, error) {
		return []firecrackerProcess{{identity: processIdentity{pid: 1}, socket: "socket", age: time.Second}}, nil
	}, func() map[string]sandbox.ProcessMetadata { return meta })
	c.sample(t.Context(), now)
	require.EqualValues(t, 0, trackerValues(t, reader)[telemetry.FirecrackerProcessesOverMaxLength])
	c.sample(t.Context(), now.Add(time.Nanosecond))
	require.EqualValues(t, 1, trackerValues(t, reader)[telemetry.FirecrackerProcessesOverMaxLength])
}

func TestTrackerUsesFiveMinuteGraceForAllUntrackedProcesses(t *testing.T) {
	t.Parallel()

	for _, temporary := range []bool{false, true} {
		t.Run(map[bool]string{false: "sandbox", true: "temporary"}[temporary], func(t *testing.T) {
			t.Parallel()
			now := time.Unix(1_800_000_000, 0)
			process := firecrackerProcess{identity: processIdentity{pid: 1}, socket: "socket"}
			tracker, reader := testTracker(t, func(context.Context) ([]firecrackerProcess, error) {
				return []firecrackerProcess{process}, nil
			}, func() map[string]sandbox.ProcessMetadata {
				return map[string]sandbox.ProcessMetadata{"socket": {Temporary: temporary}}
			})
			for _, boundary := range []struct {
				age       time.Duration
				untracked int64
			}{
				{age: 5*time.Minute - time.Nanosecond, untracked: 0},
				{age: 5 * time.Minute, untracked: 0},
				{age: 5*time.Minute + time.Nanosecond, untracked: 1},
			} {
				process.age = boundary.age
				tracker.sample(t.Context(), now)
				require.Equal(t, boundary.untracked, trackerValues(t, reader)[telemetry.FirecrackerProcessesUntracked], "age %v", boundary.age)
			}
		})
	}
}

func TestTrackerPollsRecoversAndUnregisters(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var scans atomic.Int64
		c, reader := testTracker(t, func(context.Context) ([]firecrackerProcess, error) {
			if scans.Add(1) == 1 {
				return nil, errors.New("transient read failure")
			}

			return nil, nil
		}, func() map[string]sandbox.ProcessMetadata { return nil })
		done := make(chan error, 1)
		go func() { done <- c.Start(t.Context()) }()
		synctest.Wait()
		require.EqualValues(t, 1, scans.Load())
		require.EqualValues(t, 0, trackerValues(t, reader)[telemetry.FirecrackerTrackerSuccess])
		time.Sleep(firecrackerPollInterval)
		synctest.Wait()
		require.EqualValues(t, 2, scans.Load())
		require.EqualValues(t, 1, trackerValues(t, reader)[telemetry.FirecrackerTrackerSuccess])
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.NoError(t, c.Close(ctx))
		require.NoError(t, c.Close(ctx))
		require.NoError(t, <-done)
		require.Empty(t, trackerValues(t, reader))
	})
}

func TestTrackerAgeAdvancesDuringBlockedScan(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var scans atomic.Int64
		unblock := make(chan struct{})
		release := sync.OnceFunc(func() { close(unblock) })
		defer release()
		tracker, reader := testTracker(t, func(context.Context) ([]firecrackerProcess, error) {
			if scans.Add(1) == 2 {
				<-unblock
			}

			return nil, nil
		}, func() map[string]sandbox.ProcessMetadata { return nil })
		done := make(chan error, 1)
		go func() { done <- tracker.Start(t.Context()) }()
		synctest.Wait()
		require.EqualValues(t, 0, trackerValues(t, reader)[telemetry.FirecrackerTrackerLastSuccessAge])

		time.Sleep(firecrackerPollInterval)
		synctest.Wait()
		require.EqualValues(t, 2, scans.Load())
		time.Sleep(time.Minute)
		values := trackerValues(t, reader)
		require.EqualValues(t, 1, values[telemetry.FirecrackerTrackerSuccess])
		require.EqualValues(t, 90, values[telemetry.FirecrackerTrackerLastSuccessAge])

		release()
		synctest.Wait()
		require.EqualValues(t, 0, trackerValues(t, reader)[telemetry.FirecrackerTrackerLastSuccessAge], "age resets when the scan completes")
		require.NoError(t, tracker.Close(t.Context()))
		require.NoError(t, <-done)
	})
}

func TestTrackerAgeStartsAtSuccessfulCompletion(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		tracker, reader := testTracker(t, func(context.Context) ([]firecrackerProcess, error) {
			time.Sleep(time.Minute)

			return nil, nil
		}, func() map[string]sandbox.ProcessMetadata { return nil })
		tracker.sample(t.Context(), time.Now())
		require.EqualValues(t, 0, trackerValues(t, reader)[telemetry.FirecrackerTrackerLastSuccessAge])
	})
}
