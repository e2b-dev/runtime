package template_manager

import (
	"testing"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/machineinfo"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func TestGetAvailableBuildClient_FallbackEnforcesTheMinimumVersion(t *testing.T) {
	t.Parallel()

	const (
		oldVersion = "0.1.0"
		newVersion = "99.0.0"
	)

	pinned := machineinfo.MachineInfo{CPUArchitecture: "x86_64", CPUFamily: "6", CPUModel: "85"}
	other := machineinfo.MachineInfo{CPUArchitecture: "arm64", CPUFamily: "8", CPUModel: "1"}

	source := ldtestdata.DataSource()
	source.Update(source.Flag(featureflags.BuildNodeInfo.Key()).ValueForAll(ldvalue.Parse([]byte(
		`{"cpu_architecture":"x86_64","cpu_family":"6","cpu_model":"85"}`,
	))))
	ff, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ff.Close(t.Context())) })

	clusterID := uuid.New()
	newTM := func(builders ...clusters.TestBuilder) *TemplateManager {
		return &TemplateManager{
			clusters:     clusters.NewTestPool(clusters.NewTestBuilderCluster(clusterID, builders...)),
			featureFlags: ff,
		}
	}

	t.Run("a new builder on other hardware is used and the fallback is counted", func(t *testing.T) {
		t.Parallel()

		reader := sdkmetric.NewManualReader()
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		counter, err := telemetry.GetCounter(provider.Meter("github.com/e2b-dev/infra/packages/api/internal/template-manager"), telemetry.ApiTemplateBuildBuilderPinFallback)
		require.NoError(t, err)

		tm := newTM(
			clusters.TestBuilder{Version: oldVersion, Machine: pinned},
			clusters.TestBuilder{Version: newVersion, Machine: other},
		)
		tm.builderPinFallback = counter

		got, err := tm.GetAvailableBuildClient(t.Context(), clusterID, clusters.ResizableTemplateBuildsMinVersion)
		require.NoError(t, err)
		assert.Equal(t, other, got.GetMachineInfo())

		points := sumPoints(t, reader)
		require.Len(t, points, 1)
		assert.Equal(t, int64(1), points[0].Value)
	})

	t.Run("all builders old is a version error and is counted", func(t *testing.T) {
		t.Parallel()

		reader := sdkmetric.NewManualReader()
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		counter, err := telemetry.GetCounter(provider.Meter("github.com/e2b-dev/infra/packages/api/internal/template-manager"), telemetry.ApiTemplateBuildBuilderVersionRejected)
		require.NoError(t, err)

		tm := newTM(
			clusters.TestBuilder{Version: oldVersion, Machine: pinned},
			clusters.TestBuilder{Version: oldVersion, Machine: other},
		)
		tm.builderVersionRejected = counter

		_, err = tm.GetAvailableBuildClient(t.Context(), clusterID, clusters.ResizableTemplateBuildsMinVersion)
		require.ErrorIs(t, err, clusters.ErrTemplateBuilderVersionTooOld)

		points := sumPoints(t, reader)
		require.Len(t, points, 1)
		assert.Equal(t, int64(1), points[0].Value)
		got, ok := points[0].Attributes.Value("cluster_id")
		require.True(t, ok)
		assert.Equal(t, clusterID.String(), got.AsString())
	})

	t.Run("no minimum keeps the old behaviour", func(t *testing.T) {
		t.Parallel()

		tm := newTM(
			clusters.TestBuilder{Version: oldVersion, Machine: pinned},
			clusters.TestBuilder{Version: oldVersion, Machine: other},
		)

		got, err := tm.GetAvailableBuildClient(t.Context(), clusterID, nil)
		require.NoError(t, err)
		assert.Equal(t, pinned, got.GetMachineInfo())
	})
}

// Without a build-node pin there is nothing to fall back from: old builders are
// a version rejection only, not a pin fallback.
func TestGetAvailableBuildClient_NoPinIsNotAPinFallback(t *testing.T) {
	t.Parallel()

	ff, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ff.Close(t.Context())) })

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	pinFallback, err := telemetry.GetCounter(provider.Meter("github.com/e2b-dev/infra/packages/api/internal/template-manager"), telemetry.ApiTemplateBuildBuilderPinFallback)
	require.NoError(t, err)

	clusterID := uuid.New()
	tm := &TemplateManager{
		clusters:           clusters.NewTestPool(clusters.NewTestBuilderCluster(clusterID, clusters.TestBuilder{Version: "0.1.0"})),
		featureFlags:       ff,
		builderPinFallback: pinFallback,
	}

	_, err = tm.GetAvailableBuildClient(t.Context(), clusterID, clusters.ResizableTemplateBuildsMinVersion)
	require.ErrorIs(t, err, clusters.ErrTemplateBuilderVersionTooOld)
	assert.Empty(t, sumPoints(t, reader))
}

// sumPoints collects the reader and returns the data points of its only
// counter, or nothing when no counter was touched.
func sumPoints(t *testing.T, reader *sdkmetric.ManualReader) []metricdata.DataPoint[int64] {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	if len(rm.ScopeMetrics) == 0 {
		return nil
	}

	require.Len(t, rm.ScopeMetrics, 1)
	require.Len(t, rm.ScopeMetrics[0].Metrics, 1)
	sum, ok := rm.ScopeMetrics[0].Metrics[0].Data.(metricdata.Sum[int64])
	require.True(t, ok)

	return sum.DataPoints
}

// The handler checks the version of the builder it picks but only hands the
// machine on; the process there can be replaced before the build is sent. A
// resizable build must not be sent to an older process on that machine.
func TestBuildDispatchClient_RechecksTheVersionOnTheMachine(t *testing.T) {
	t.Parallel()

	clusterID := uuid.New()
	// NewTestBuilderCluster names each builder's machine by its index.
	tm := &TemplateManager{clusters: clusters.NewTestPool(clusters.NewTestBuilderCluster(clusterID,
		clusters.TestBuilder{Version: "0.1.0"},
		clusters.TestBuilder{Version: "99.0.0"},
	))}

	t.Run("an old process is refused for a resizable build and counted", func(t *testing.T) {
		t.Parallel()

		reader := sdkmetric.NewManualReader()
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		counter, err := telemetry.GetCounter(provider.Meter("github.com/e2b-dev/infra/packages/api/internal/template-manager"), telemetry.ApiTemplateBuildBuilderVersionRejected)
		require.NoError(t, err)
		tm := &TemplateManager{clusters: tm.clusters, builderVersionRejected: counter}

		_, err = tm.buildDispatchClient(t.Context(), clusterID, "0", clusters.ResizableTemplateBuildsMinVersion)
		require.ErrorIs(t, err, clusters.ErrTemplateBuilderVersionTooOld)

		points := sumPoints(t, reader)
		require.Len(t, points, 1)
		stage, ok := points[0].Attributes.Value("stage")
		require.True(t, ok)
		assert.Equal(t, "dispatch", stage.AsString())
	})

	t.Run("a process at the version is used", func(t *testing.T) {
		t.Parallel()

		_, err := tm.buildDispatchClient(t.Context(), clusterID, "1", clusters.ResizableTemplateBuildsMinVersion)
		require.NoError(t, err)
	})

	t.Run("a fixed-size build keeps the old behaviour", func(t *testing.T) {
		t.Parallel()

		_, err := tm.buildDispatchClient(t.Context(), clusterID, "0", nil)
		require.NoError(t, err)
	})
}
