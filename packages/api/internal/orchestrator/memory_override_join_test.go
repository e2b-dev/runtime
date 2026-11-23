package orchestrator

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
)

// A resume naming memoryMB that meets a start already in flight is refused, and the
// refusal counts as a rejected memory override with the in-flight code. The counter
// is a package instrument on the global meter, so this test installs the global
// provider; no other test in the package does.
func TestCreateSandbox_MemoryJoinCountsAsRejected(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	o := newCreateSandboxTestOrchestrator(t, nodemanager.WithOrchestratorVersion(clusters.SandboxMemoryResizeMinVersion.String()))
	o.featureFlagsClient = sandboxCPUFlags(t, true)
	team := memoryTeam()
	sandboxID := "sbx-memjoin-" + uuid.New().String()[:8]
	build := growableBuild(4096, 1024)
	now := time.Now()

	winnerEntered := make(chan struct{})
	winnerRelease := make(chan struct{})
	winnerDone := make(chan struct{})
	go func() {
		defer close(winnerDone)
		o.CreateSandbox(t.Context(), sandboxID, uuid.New().String(), team,
			func(_ context.Context) (SandboxMetadata, *api.APIError) {
				close(winnerEntered)
				<-winnerRelease

				return SandboxMetadata{TemplateID: "tpl", BaseTemplateID: "base-tpl", Build: build}, nil
			},
			now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{IsResume: true})
	}()
	<-winnerEntered

	_, joinErr := o.CreateSandbox(t.Context(), sandboxID, uuid.New().String(), team,
		func(_ context.Context) (SandboxMetadata, *api.APIError) {
			assert.Fail(t, "joiner must not fetch data")

			return SandboxMetadata{}, nil
		},
		now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{IsResume: true}, WithMemoryMB(new(int32(2048))))
	close(winnerRelease)
	<-winnerDone

	require.NotNil(t, joinErr)
	assert.Equal(t, http.StatusConflict, joinErr.Code)
	assert.Equal(t, ErrCodeStartInFlight, joinErr.ErrorCode)

	assert.Equal(t, int64(1), memoryOverrideCount(t, reader, memoryOverrideRejected, ErrCodeStartInFlight, memorySourceNamed))

	// A create that inherits a size above the caller's team limit from a
	// snapshot build counts too, as an inherited size.
	limited := memoryTeam()
	limited.Limits.MaxRamMb = 2048
	grown := growableBuild(4096, 3072)
	_, inheritedErr := o.CreateSandbox(t.Context(), "sbx-meminh-"+uuid.New().String()[:8], uuid.New().String(), limited,
		func(_ context.Context) (SandboxMetadata, *api.APIError) {
			return SandboxMetadata{TemplateID: "tpl", BaseTemplateID: "base-tpl", Build: grown}, nil
		},
		now, now.Add(time.Hour), time.Hour, false, false, sandbox.CreationMetadata{})
	require.NotNil(t, inheritedErr)
	assert.Equal(t, errCodeSandboxMemoryTeamLimit, inheritedErr.ErrorCode)
	assert.Contains(t, inheritedErr.ClientMsg, "this sandbox starts with")
	assert.Equal(t, int64(1), memoryOverrideCount(t, reader, memoryOverrideRejected, errCodeSandboxMemoryTeamLimit, memorySourceInherited))
}

// memoryOverrideCount sums api.sandbox.memory_override over the points with the
// given outcome, error code and source.
func memoryOverrideCount(t *testing.T, reader *sdkmetric.ManualReader, outcome, code, source string) int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "api.sandbox.memory_override" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				gotOutcome, _ := dp.Attributes.Value(attribute.Key("outcome"))
				gotCode, _ := dp.Attributes.Value(attribute.Key("error_code"))
				gotSource, _ := dp.Attributes.Value(attribute.Key("source"))
				if gotOutcome.AsString() == outcome && gotCode.AsString() == code && gotSource.AsString() == source {
					total += dp.Value
				}
			}
		}
	}

	return total
}
