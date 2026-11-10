package orchestrator

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/db/queries"
)

// The pre-reservation check gives the same answer as the start path for every
// refusal, and accepts what the start path accepts.
func TestCheckSandboxCPUCount(t *testing.T) {
	t.Parallel()

	resizable := queries.EnvBuild{Vcpu: 2, MaxVcpu: new(int64(4))}
	fixed := queries.EnvBuild{Vcpu: 2}

	for _, tt := range []struct {
		name      string
		build     queries.EnvBuild
		requested *int32
		flag      bool
		teamMax   int64
		wantCode  string
	}{
		{name: "absent count passes without the flag", build: resizable, flag: false, teamMax: 8},
		{name: "in range", build: resizable, requested: new(int32(3)), flag: true, teamMax: 8},
		{name: "flag off", build: resizable, requested: new(int32(3)), flag: false, teamMax: 8, wantCode: errCodeSandboxCpuCountDisabled},
		{name: "above max", build: resizable, requested: new(int32(8)), flag: true, teamMax: 8, wantCode: errCodeSandboxCpuCountOutOfRange},
		{name: "fixed-size mismatch", build: fixed, requested: new(int32(4)), flag: true, teamMax: 8, wantCode: errCodeSandboxCpuCountNotResizable},
		{name: "above team limit", build: resizable, requested: new(int32(4)), flag: true, teamMax: 3, wantCode: errCodeSandboxCpuCountTeamLimit},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			team := testTeam()
			team.Limits.MaxVcpu = tt.teamMax
			apiErr := CheckSandboxCPUCount(t.Context(), sandboxCPUFlags(t, tt.flag), team, tt.build, tt.requested)
			if tt.wantCode == "" {
				require.Nil(t, apiErr)

				return
			}

			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Code)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode)
		})
	}
}

// A request that passes the pre-reservation check is counted once, by the
// start path; one it refuses is counted once, here. Not parallel: it swaps
// the package counter.
//
//nolint:paralleltest
func TestCheckSandboxCPUCount_CountsOnlyRefusals(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator").Int64Counter("api.sandbox.cpu_count_override")
	require.NoError(t, err)
	previous := cpuCountOverrideCounter
	cpuCountOverrideCounter = counter
	t.Cleanup(func() { cpuCountOverrideCounter = previous })

	flags := sandboxCPUFlags(t, true)
	build := queries.EnvBuild{Vcpu: 2, MaxVcpu: new(int64(4))}

	require.Nil(t, CheckSandboxCPUCount(t.Context(), flags, testTeam(), build, new(int32(3))))
	_, apiErr := resolveSandboxVcpu(t.Context(), flags, testTeam(), "sbx", "tpl", build, new(int32(3)))
	require.Nil(t, apiErr)
	require.NotNil(t, CheckSandboxCPUCount(t.Context(), flags, testTeam(), build, new(int32(8))))

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
	assert.Equal(t, map[string]int64{"accepted": 1, "rejected": 1}, got)
}
