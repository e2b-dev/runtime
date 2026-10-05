//go:build linux

package server

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func TestSandboxLimitMetric(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		source := ldtestdata.DataSource()
		flag := source.Flag(featureflags.MaxSandboxesPerNode.Key()).
			Variations(ldvalue.Int(999), ldvalue.Int(320)).
			IfMatchContext(featureflags.TeamKind, "key", ldvalue.String("limit-team")).ThenReturnIndex(0).
			IfMatchContext(featureflags.SandboxKind, "key", ldvalue.String("limit-sandbox")).ThenReturnIndex(0).
			IfMatchContext("orchestrator", "key", ldvalue.String("metric-node")).ThenReturnIndex(1).
			FallthroughVariationIndex(0)
		source.Update(flag)
		server, reader := newSandboxLimitTestServer(t, t.Context(), source)
		require.True(t, server.startingSandboxes.TryAcquire(1))
		defer server.startingSandboxes.Release(1)
		synctest.Wait()

		ctx := featureflags.AddToContext(t.Context(),
			featureflags.TeamContext("limit-team"), featureflags.SandboxContext("limit-sandbox"))
		require.Equal(t, 999, server.featureFlags.IntFlag(ctx, featureflags.MaxSandboxesPerNode))
		assertLimit := func(want int64) {
			t.Helper()
			require.Equal(t, want, server.info.MaxSandboxes.Load())
			assertSandboxLimitMetric(t, ctx, reader, want)
			_, err := server.Create(ctx, &orchestrator.SandboxCreateRequest{
				Sandbox: &orchestrator.SandboxConfig{SandboxId: "limit-sandbox", TeamId: "limit-team"},
			})
			require.Equal(t, codes.ResourceExhausted, status.Code(err))
			if want > 0 {
				require.Equal(t, "too many sandboxes starting on this node, please retry", status.Convert(err).Message())
			} else {
				require.Equal(t, fmt.Sprintf("max number of running sandboxes on node reached (%d), please retry", want), status.Convert(err).Message())
			}
		}

		previous := int64(320)
		assertLimit(previous)
		for _, limit := range []int{80, 0, -1, 200} {
			source.Update(flag.Variations(ldvalue.Int(999), ldvalue.Int(limit)))
			assertLimit(previous)
			time.Sleep(maxSandboxesLimitRefreshInterval - time.Nanosecond)
			synctest.Wait()
			assertLimit(previous)
			time.Sleep(time.Nanosecond)
			synctest.Wait()
			assertLimit(int64(limit))
			previous = int64(limit)
		}
	})
}

func TestSandboxLimitFallback(t *testing.T) {
	t.Parallel()

	server, reader := newSandboxLimitTestServer(t, t.Context(), ldtestdata.DataSource())
	require.Equal(t, int64(200), server.info.MaxSandboxes.Load())
	assertSandboxLimitMetric(t, t.Context(), reader, 200)
}

func TestSandboxLimitRefreshStops(t *testing.T) {
	t.Parallel()

	for _, stop := range []string{"close", "cancel"} {
		t.Run(stop, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				source := ldtestdata.DataSource()
				source.Update(source.Flag(featureflags.MaxSandboxesPerNode.Key()).ValueForAll(ldvalue.Int(80)))
				server, reader := newSandboxLimitTestServer(t, ctx, source)
				synctest.Wait()
				if stop == "close" {
					require.NoError(t, server.Close(t.Context()))
				} else {
					cancel()
				}
				synctest.Wait()
				source.Update(source.Flag(featureflags.MaxSandboxesPerNode.Key()).ValueForAll(ldvalue.Int(320)))
				time.Sleep(2 * maxSandboxesLimitRefreshInterval)
				synctest.Wait()
				require.Equal(t, int64(80), server.info.MaxSandboxes.Load())
				assertSandboxLimitMetric(t, t.Context(), reader, 80)
			})
		})
	}
}

func newSandboxLimitTestServer(t *testing.T, ctx context.Context, source *ldtestdata.TestDataSource) (*Server, *sdkmetric.ManualReader) {
	t.Helper()

	source.Update(source.Flag(featureflags.MaxStartingInstancesPerNode.Key()).ValueForAll(ldvalue.Int(1)))
	flags, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, flags.Close(context.WithoutCancel(ctx))) })
	flags.RegisterContextProvider(func(context.Context) ldcontext.Context {
		return ldcontext.NewBuilder("metric-node").Kind("orchestrator").Build()
	})

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(ctx))) })
	server, err := New(ctx, ServiceConfig{
		Tel:            &telemetry.Client{MeterProvider: provider},
		SandboxFactory: &sandbox.Factory{Sandboxes: sandbox.NewSandboxesMap()},
		Info:           &service.ServiceInfo{},
		FeatureFlags:   flags,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close(context.WithoutCancel(ctx))) })

	return server, reader
}

func assertSandboxLimitMetric(t *testing.T, ctx context.Context, reader *sdkmetric.ManualReader, want int64) {
	t.Helper()

	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &data))
	for _, scope := range data.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != "orchestrator.sandbox.limit" {
				continue
			}
			require.Equal(t, "{sandbox}", metric.Unit)
			gauge, ok := metric.Data.(metricdata.Gauge[int64])
			require.True(t, ok)
			require.Len(t, gauge.DataPoints, 1)
			require.Equal(t, want, gauge.DataPoints[0].Value)
			require.Zero(t, gauge.DataPoints[0].Attributes.Len())

			return
		}
	}
	t.Fatal("sandbox limit gauge missing")
}
