package template

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

func maxVcpuFlags(t *testing.T, enabled bool, defaultMax int) *featureflags.Client {
	t.Helper()
	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.ResizableTemplatesFlag.Key()).VariationForAll(enabled))
	td.Update(td.Flag(featureflags.ResizableTemplateDefaultMaxCpuFlag.Key()).ValueForAll(ldvalue.Int(defaultMax)))
	flags, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, flags.Close(context.WithoutCancel(t.Context()))) })

	return flags
}

func TestResolveMaxVcpu(t *testing.T) {
	t.Parallel()

	limits := &types.TeamLimits{MaxVcpu: 8}
	tests := []struct {
		name       string
		maxVcpu    int64
		enabled    bool
		defaultMax int
		cpuCount   int64
		requested  *int32
		want       *int64
		clientMsg  string
	}{
		{name: "flag off without field", cpuCount: 2},
		{name: "flag off ignores the team default", defaultMax: 4, cpuCount: 2},
		{name: "flag off with field", requested: new(int32(4)), cpuCount: 2, clientMsg: "Resizable templates are not enabled for your team"},
		{name: "flag on without field and no default", enabled: true, cpuCount: 2},
		{name: "explicit value", enabled: true, requested: new(int32(4)), cpuCount: 2, want: new(int64(4))},
		{name: "explicit equal to cpu count is kept", enabled: true, requested: new(int32(2)), cpuCount: 2, want: new(int64(2))},
		{name: "explicit wins over the team default", enabled: true, defaultMax: 8, requested: new(int32(4)), cpuCount: 2, want: new(int64(4))},
		{name: "explicit below cpu count", enabled: true, requested: new(int32(2)), cpuCount: 4, clientMsg: "Max CPU count must be at least the CPU count (4)"},
		{name: "explicit odd", enabled: true, requested: new(int32(3)), cpuCount: 2, clientMsg: "Max CPU count must be 1 or an even number"},
		{name: "explicit above team limit", enabled: true, requested: new(int32(16)), cpuCount: 2, clientMsg: "Max CPU count can't be higher than 8 (if you need to increase this limit, please contact support)"},
		{name: "team default within cap", enabled: true, defaultMax: 4, cpuCount: 2, want: new(int64(4))},
		{name: "team default equal to cpu count", enabled: true, defaultMax: 2, cpuCount: 2, want: new(int64(2))},
		{name: "team default clamped to the cap", enabled: true, defaultMax: 16, cpuCount: 2, want: new(int64(8))},
		{name: "team default raised to cpu count", enabled: true, defaultMax: 2, cpuCount: 4, want: new(int64(4))},
		{name: "team default clamped to an odd cap rounds down", maxVcpu: 9, enabled: true, defaultMax: 16, cpuCount: 2, want: new(int64(8))},
		{name: "team default clamped to cap one", maxVcpu: 1, enabled: true, defaultMax: 4, cpuCount: 1, want: new(int64(1))},
		{name: "team default one", enabled: true, defaultMax: 1, cpuCount: 1, want: new(int64(1))},
		{name: "starting count above team cap ignores default", maxVcpu: 1, enabled: true, defaultMax: 4, cpuCount: 2},
		{name: "team default odd", enabled: true, defaultMax: 3, cpuCount: 2},
		{name: "team default negative", enabled: true, defaultMax: -2, cpuCount: 2},
		{name: "team default above firecracker max", enabled: true, defaultMax: 34, cpuCount: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			limits := limits
			if tt.maxVcpu != 0 {
				limits = &types.TeamLimits{MaxVcpu: tt.maxVcpu}
			}

			flags := maxVcpuFlags(t, tt.enabled, tt.defaultMax)
			got, apiErr := resolveMaxVcpu(t.Context(), flags, limits, uuid.New(), tt.cpuCount, tt.requested)
			if tt.clientMsg != "" {
				require.NotNil(t, apiErr)
				assert.Equal(t, http.StatusBadRequest, apiErr.Code)
				assert.Equal(t, tt.clientMsg, apiErr.ClientMsg)
				assert.Nil(t, got.value)

				return
			}
			require.Nil(t, apiErr)
			assert.Equal(t, tt.want, got.value)
		})
	}
}

// Every resolution is counted once with its source and outcome, including the
// rejected ones. Not parallel: it swaps the package counter.
//
//nolint:paralleltest
func TestResolveMaxVcpu_CountsEachResolution(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/api/internal/template").Int64Counter("api.template.build.max_vcpu")
	require.NoError(t, err)
	previous := maxVcpuCounter
	maxVcpuCounter = counter
	t.Cleanup(func() { maxVcpuCounter = previous })

	flags := maxVcpuFlags(t, true, 0)
	limits := &types.TeamLimits{MaxVcpu: 8}
	_, apiErr := resolveMaxVcpu(t.Context(), flags, limits, uuid.New(), 2, new(int32(4)))
	require.Nil(t, apiErr)
	_, apiErr = resolveMaxVcpu(t.Context(), flags, limits, uuid.New(), 4, new(int32(2)))
	require.NotNil(t, apiErr)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	require.Len(t, rm.ScopeMetrics, 1)
	require.Len(t, rm.ScopeMetrics[0].Metrics, 1)
	sum, ok := rm.ScopeMetrics[0].Metrics[0].Data.(metricdata.Sum[int64])
	require.True(t, ok)

	got := map[string]int64{}
	for _, p := range sum.DataPoints {
		source, _ := p.Attributes.Value("source")
		outcome, _ := p.Attributes.Value("outcome")
		got[source.AsString()+"/"+outcome.AsString()] = p.Value
	}
	assert.Equal(t, map[string]int64{"request/accepted": 1, "request/rejected_validation": 1}, got)
}

// Targeting one team must not enable explicit or default capacity for another.
func TestResolveMaxVcpu_TeamTargeting(t *testing.T) {
	t.Parallel()

	allowed, denied := uuid.New(), uuid.New()
	source := ldtestdata.DataSource()
	source.Update(source.Flag(featureflags.ResizableTemplatesFlag.Key()).
		VariationForAll(false).VariationForKey(featureflags.TeamKind, allowed.String(), true))
	source.Update(source.Flag(featureflags.ResizableTemplateDefaultMaxCpuFlag.Key()).ValueForAll(ldvalue.Int(4)))
	flags, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, flags.Close(context.WithoutCancel(t.Context()))) })
	limits := &types.TeamLimits{MaxVcpu: 8}

	for _, teamID := range []uuid.UUID{allowed, denied} {
		t.Run(teamID.String(), func(t *testing.T) {
			t.Parallel()

			explicit, apiErr := resolveMaxVcpu(t.Context(), flags, limits, teamID, 2, new(int32(8)))
			if teamID == allowed {
				require.Nil(t, apiErr)
				assert.Equal(t, new(int64(8)), explicit.value)
			} else {
				require.NotNil(t, apiErr)
				assert.Equal(t, http.StatusBadRequest, apiErr.Code)
				assert.Nil(t, explicit.value)
			}

			inherited, apiErr := resolveMaxVcpu(t.Context(), flags, limits, teamID, 2, nil)
			require.Nil(t, apiErr)
			if teamID == allowed {
				assert.Equal(t, new(int64(4)), inherited.value)
			} else {
				assert.Nil(t, inherited.value)
			}
		})
	}
}
