//go:build linux

package server

import (
	"context"
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildlogger"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

const hotplugFirecracker = "v1.17-0.4.1"

// memorySizingCounts swaps the server's memory_sizing counter for one the test reads back.
func memorySizingCounts(t *testing.T, s *ServerStore) func() map[string]int64 {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/template/server").Int64Counter("memory_sizing")
	require.NoError(t, err)
	s.memorySizing = counter

	return func() map[string]int64 {
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(t.Context(), &rm))
		got := map[string]int64{}
		for _, sm := range rm.ScopeMetrics {
			for _, md := range sm.Metrics {
				sum, ok := md.Data.(metricdata.Sum[int64])
				require.True(t, ok)
				for _, dp := range sum.DataPoints {
					result, _ := dp.Attributes.Value("result")
					reason, _ := dp.Attributes.Value("reason")
					got[result.AsString()+"/"+reason.AsString()] += dp.Value
				}
			}
		}

		return got
	}
}

// The RPC decides a region in a fixed order, each refusal with its own code, before anything
// enters the build cache, and counts every request once on memory_sizing.
func TestTemplateCreateMemoryGates(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name                string
		memoryMB, maxMemory int32
		fcVersion, kernel   string
		code                codes.Code
		message, count      string
	}{
		{name: "partial slot is invalid", memoryMB: 1024, maxMemory: 1100, fcVersion: hotplugFirecracker, code: codes.InvalidArgument, message: "multiple of 128 MiB", count: "rejected/invalid"},
		{name: "above the ratio", memoryMB: 1024, maxMemory: 8192, fcVersion: hotplugFirecracker, code: codes.InvalidArgument, message: "the largest allowed maxMemoryMB is 4096", count: "rejected/ratio"},
		{name: "old firecracker", memoryMB: 1024, maxMemory: 4096, fcVersion: "v1.14-0.3.1", code: codes.FailedPrecondition, message: "cannot build templates with a maximum memory size", count: "rejected/firecracker"},
		{name: "old guest kernel", memoryMB: 1024, maxMemory: 4096, fcVersion: hotplugFirecracker, kernel: "vmlinux-5.10.186", code: codes.FailedPrecondition, message: "cannot build templates with a maximum memory size", count: "rejected/kernel"},
		{name: "unreadable firecracker", memoryMB: 1024, maxMemory: 4096, fcVersion: "invalid", message: "invalid resolved firecracker version", count: "rejected/internal"},
		{name: "an invalid size wins over an old firecracker", memoryMB: 1024, maxMemory: 1100, fcVersion: "v1.14-0.3.1", code: codes.InvalidArgument, message: "multiple of 128 MiB", count: "rejected/invalid"},
		{name: "the ratio wins over an old firecracker", memoryMB: 1024, maxMemory: 8192, fcVersion: "v1.14-0.3.1", code: codes.InvalidArgument, message: "hotplug cap", count: "rejected/ratio"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s, _, req := newWorkTrackingServer(t, tc.fcVersion, "")
			if tc.kernel != "" {
				td := ldtestdata.DataSource()
				td.Update(td.Flag(featureflags.BuildFirecrackerVersion.Key()).ValueForAll(ldvalue.String(tc.fcVersion)))
				td.Update(td.Flag(featureflags.BuildKernelVersion.Key()).ValueForAll(ldvalue.String(tc.kernel)))
				flags, err := featureflags.NewClientWithDatasource(td)
				require.NoError(t, err)
				t.Cleanup(func() { assert.NoError(t, flags.Close(context.WithoutCancel(t.Context()))) })
				s.featureFlags = flags
			}
			counts := memorySizingCounts(t, s)
			req.Template.MemoryMB, req.Template.MaxMemoryMB = tc.memoryMB, &tc.maxMemory

			_, err := s.TemplateCreate(t.Context(), req)
			require.ErrorContains(t, err, tc.message)
			if tc.code != codes.OK {
				assert.Equal(t, tc.code, status.Code(err))
			}
			_, err = s.buildCache.Get(req.GetTemplate().GetBuildID())
			require.Error(t, err, "a refused build never enters the cache")
			assert.Equal(t, map[string]int64{tc.count: 1}, counts())
		})
	}
}

// A build counts as accepted only once it is registered, a fixed-size build is never refused for
// lacking hotplug, and an API retry of a registered build is not counted again.
func TestTemplateCreateCountsAcceptedBuildsOnce(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name                string
		memoryMB, maxMemory int32
		fcVersion, count    string
	}{
		{name: "resizable", memoryMB: 1024, maxMemory: 4096, fcVersion: hotplugFirecracker, count: "resizable/"},
		{name: "fixed size on a firecracker without hotplug", memoryMB: 1024, maxMemory: 1024, fcVersion: "v1.14-0.3.1", count: "equal/"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s, provider, req := newWorkTrackingServer(t, tc.fcVersion, "")
			provider.EXPECT().DeleteObjectsWithPrefix(mock.Anything, mock.Anything).Return(nil).Maybe()
			counts := memorySizingCounts(t, s)
			req.Template.MemoryMB, req.Template.MaxMemoryMB = tc.memoryMB, &tc.maxMemory

			_, err := s.TemplateCreate(t.Context(), req)
			require.NoError(t, err)
			_, err = s.TemplateCreate(t.Context(), req)
			require.ErrorContains(t, err, "already exists in cache")
			s.wg.Wait()

			assert.Equal(t, map[string]int64{tc.count: 1}, counts())
		})
	}
}

// A request whose build ID the cache already holds, without a first count in this test, is not counted.
func TestTemplateCreateDoesNotCountARetry(t *testing.T) {
	t.Parallel()

	s, _, req := newWorkTrackingServer(t, hotplugFirecracker, "")
	counts := memorySizingCounts(t, s)
	_, err := s.buildCache.Create(req.GetTemplate().GetTeamID(), req.GetTemplate().GetBuildID(), buildlogger.NewLogEntryLogger())
	require.NoError(t, err)

	_, err = s.TemplateCreate(t.Context(), req)
	require.ErrorContains(t, err, "already exists in cache")
	assert.Empty(t, counts())
}
