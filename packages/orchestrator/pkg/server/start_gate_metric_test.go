//go:build linux

package server

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// The wait histogram carries exactly {kind, outcome} and one sample per
// attempt; the wait is what the semaphore cost, not the timeout or the span.
func TestStartGateWait_RecordsTheWait(t *testing.T) {
	t.Parallel()

	t.Run("acquired after the slot frees", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			s, reader := startGateTestServer(t)
			require.True(t, s.startingSandboxes.TryAcquire(1))

			done := make(chan error, 1)
			go func() { done <- s.waitForAcquire(t.Context(), startGateResume) }()
			synctest.Wait()
			time.Sleep(250 * time.Millisecond)
			s.startingSandboxes.Release(1)
			require.NoError(t, <-done)
			s.startingSandboxes.Release(1)

			points := startGateWaitPoints(t, reader)
			require.Len(t, points, 1)
			point := points[startGateKey{startGateResume, startGateAcquired}]
			require.Equal(t, uint64(1), point.Count)
			require.Equal(t, int64(250), point.Sum)
		})
	})

	t.Run("cancelled by the caller", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			s, reader := startGateTestServer(t)
			require.True(t, s.startingSandboxes.TryAcquire(1))
			defer s.startingSandboxes.Release(1)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- s.waitForAcquire(ctx, startGateCheckpoint) }()
			synctest.Wait()
			time.Sleep(40 * time.Millisecond)
			cancel()
			require.Equal(t, codes.ResourceExhausted, status.Code(<-done))

			points := startGateWaitPoints(t, reader)
			require.Len(t, points, 1)
			point := points[startGateKey{startGateCheckpoint, startGateCancelled}]
			require.Equal(t, uint64(1), point.Count)
			require.Equal(t, int64(40), point.Sum)
		})
	})

	t.Run("cancelled by the acquire timeout", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			s, reader := startGateTestServer(t)
			require.True(t, s.startingSandboxes.TryAcquire(1))
			defer s.startingSandboxes.Release(1)

			require.Equal(t, codes.ResourceExhausted, status.Code(s.waitForAcquire(t.Context(), startGateHarvest)))

			points := startGateWaitPoints(t, reader)
			require.Len(t, points, 1)
			point := points[startGateKey{startGateHarvest, startGateCancelled}]
			require.Equal(t, uint64(1), point.Count)
			require.Equal(t, acquireTimeout.Milliseconds(), point.Sum)
		})
	})
}

// A create never waits: it records a zero sample, acquired or refused.
func TestStartGateTry_RecordsTheOutcome(t *testing.T) {
	t.Parallel()

	s, reader := startGateTestServer(t)
	require.True(t, s.tryAcquireStart(t.Context(), startGateCreate))
	require.False(t, s.tryAcquireStart(t.Context(), startGateCreate))
	s.startingSandboxes.Release(1)

	points := startGateWaitPoints(t, reader)
	require.Len(t, points, 2)
	for _, outcome := range []string{startGateAcquired, startGateRefused} {
		point := points[startGateKey{startGateCreate, outcome}]
		require.Equal(t, uint64(1), point.Count, outcome)
		require.Zero(t, point.Sum, outcome)
	}
}

func startGateTestServer(t *testing.T) (*Server, *sdkmetric.ManualReader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/server")

	return &Server{
		startingSandboxes:     utils.Must(utils.NewAdjustableSemaphore(1)),
		startGateWaitDuration: utils.Must(telemetry.GetHistogram(meter, telemetry.OrchestratorSandboxStartGateWaitDurationName)),
	}, reader
}

type startGateKey struct{ kind, outcome string }

func startGateWaitPoints(t *testing.T, reader *sdkmetric.ManualReader) map[startGateKey]metricdata.HistogramDataPoint[int64] {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	out := map[startGateKey]metricdata.HistogramDataPoint[int64]{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.OrchestratorSandboxStartGateWaitDurationName) {
				continue
			}
			require.Equal(t, "ms", m.Unit)
			hist, ok := m.Data.(metricdata.Histogram[int64])
			require.True(t, ok)
			for _, p := range hist.DataPoints {
				require.Equal(t, 2, p.Attributes.Len(), "the gate carries exactly kind and outcome")
				kind, _ := p.Attributes.Value("kind")
				outcome, _ := p.Attributes.Value("outcome")
				out[startGateKey{kind.AsString(), outcome.AsString()}] = p
			}
		}
	}

	return out
}
