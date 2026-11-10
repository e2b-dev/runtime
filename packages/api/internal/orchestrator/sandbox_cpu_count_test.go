package orchestrator

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

func TestResolveSandboxVcpu(t *testing.T) {
	t.Parallel()

	fixed := queries.EnvBuild{ID: uuid.New(), Vcpu: 2}
	resizable := queries.EnvBuild{ID: uuid.New(), Vcpu: 2, MaxVcpu: new(int64(4))}

	tests := []struct {
		name      string
		build     queries.EnvBuild
		requested *api.CPUCount
		flag      bool
		want      int64
		wantCode  string
	}{
		{"absent: resizable build remains usable with flag off", resizable, nil, false, 2, ""},
		{"absent: fixed-size build remains usable with flag off", fixed, nil, false, 2, ""},
		{"in range", resizable, new(api.CPUCount(3)), true, 3, ""},
		{"equal to max", resizable, new(api.CPUCount(4)), true, 4, ""},
		{"above max", resizable, new(api.CPUCount(8)), true, 0, errCodeSandboxCpuCountOutOfRange},
		{"below 1", resizable, new(api.CPUCount(0)), true, 0, errCodeSandboxCpuCountOutOfRange},
		{"fixed-size: mismatch", fixed, new(api.CPUCount(4)), true, 0, errCodeSandboxCpuCountNotResizable},
		{"fixed-size: equal", fixed, new(api.CPUCount(2)), true, 2, ""},
		{"flag off: rejected even when valid", resizable, new(api.CPUCount(2)), false, 0, errCodeSandboxCpuCountDisabled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			flags := sandboxCPUFlags(t, tt.flag)

			got, apiErr := resolveSandboxVcpu(t.Context(), flags, testTeam(), "sbx", "tpl", tt.build, tt.requested)
			if tt.wantCode == "" {
				require.Nil(t, apiErr)
				assert.Equal(t, tt.want, got)

				return
			}

			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Code)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode)
			assert.NotEmpty(t, apiErr.ClientMsg)
		})
	}
}

// Requests naming a cpuCount are counted by outcome, and so is an inherited
// count lowered to the team limit. Not parallel: it
// swaps the package counter.
//
//nolint:paralleltest
func TestResolveSandboxVcpu_CountsEachOverride(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator").Int64Counter("api.sandbox.cpu_count_override")
	require.NoError(t, err)
	previous := cpuCountOverrideCounter
	cpuCountOverrideCounter = counter
	t.Cleanup(func() { cpuCountOverrideCounter = previous })

	flags := sandboxCPUFlags(t, true)
	build := queries.EnvBuild{Vcpu: 2, MaxVcpu: new(int64(4))}

	_, apiErr := resolveSandboxVcpu(t.Context(), flags, testTeam(), "sbx", "tpl", build, nil)
	require.Nil(t, apiErr)
	_, apiErr = resolveSandboxVcpu(t.Context(), flags, testTeam(), "sbx", "tpl", build, new(api.CPUCount(4)))
	require.Nil(t, apiErr)
	_, apiErr = resolveSandboxVcpu(t.Context(), flags, testTeam(), "sbx", "tpl", build, new(api.CPUCount(8)))
	require.NotNil(t, apiErr)
	downgraded := testTeam()
	downgraded.Limits.MaxVcpu = 1
	_, apiErr = resolveSandboxVcpu(t.Context(), flags, downgraded, "sbx", "tpl", build, nil)
	require.Nil(t, apiErr)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	require.Len(t, rm.ScopeMetrics, 1)
	require.Len(t, rm.ScopeMetrics[0].Metrics, 1)
	sum, ok := rm.ScopeMetrics[0].Metrics[0].Data.(metricdata.Sum[int64])
	require.True(t, ok)

	got := map[string]int64{}
	for _, p := range sum.DataPoints {
		outcome, _ := p.Attributes.Value("outcome")
		got[outcome.AsString()] = p.Value
	}
	assert.Equal(t, map[string]int64{"accepted": 1, "rejected": 1, "clamped_to_team_limit": 1}, got,
		"an absent field is counted only when it is lowered to the team limit")
}

func sandboxCPUFlags(t *testing.T, enabled bool) *featureflags.Client {
	t.Helper()
	source := ldtestdata.DataSource()
	source.Update(source.Flag(featureflags.ResizableTemplatesFlag.Key()).VariationForAll(enabled))
	flags, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, flags.Close(context.WithoutCancel(t.Context()))) })

	return flags
}

func TestResolveSandboxVcpuTeamLimit(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		requested *int32
		want      int64
		rejected  bool
	}{
		{name: "public template override exceeds caller limit", requested: new(int32(8)), rejected: true},
		// Connect and auto-resume cannot name a smaller count, so refusing would
		// leave the sandbox unreachable after the team's limit is lowered.
		{name: "snapshot default exceeds downgraded limit starts at the limit", want: 3},
		{name: "capacity may exceed caller limit", requested: new(int32(3)), want: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			team := testTeam()
			team.Limits.MaxVcpu = 3
			build := queries.EnvBuild{Vcpu: 4, MaxVcpu: new(int64(8))}
			got, apiErr := resolveSandboxVcpu(t.Context(), sandboxCPUFlags(t, true), team, "sbx", "public", build, tt.requested)
			if tt.rejected {
				require.NotNil(t, apiErr)
				assert.Equal(t, http.StatusBadRequest, apiErr.Code)
				assert.Equal(t, errCodeSandboxCpuCountTeamLimit, apiErr.ErrorCode)

				return
			}
			require.Nil(t, apiErr)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestResolveSandboxVcpuTeamTargeting(t *testing.T) {
	t.Parallel()

	allowed, denied := testTeam(), testTeam()
	source := ldtestdata.DataSource()
	source.Update(source.Flag(featureflags.ResizableTemplatesFlag.Key()).
		VariationForAll(false).VariationForKey(featureflags.TeamKind, allowed.ID.String(), true))
	flags, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, flags.Close(context.WithoutCancel(t.Context()))) })
	build := queries.EnvBuild{Vcpu: 2, MaxVcpu: new(int64(4))}

	got, apiErr := resolveSandboxVcpu(t.Context(), flags, allowed, "sbx", "tpl", build, new(int32(3)))
	require.Nil(t, apiErr)
	assert.Equal(t, int64(3), got)

	_, apiErr = resolveSandboxVcpu(t.Context(), flags, denied, "sbx", "tpl", build, new(int32(3)))
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusBadRequest, apiErr.Code)
	assert.Equal(t, errCodeSandboxCpuCountDisabled, apiErr.ErrorCode)
}
