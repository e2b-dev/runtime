package server

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// The two ways an in-place checkpoint loses its sandbox, as Pause reports
// them: the resume itself failed, or the resume worked and the rootfs thaw
// gave up afterwards.
func lostToResume() error {
	return fmt.Errorf("checkpoint: %w", errors.Join(sandbox.ErrSandboxLost, errors.New("resume: vm did not come back")))
}

func lostToThaw() error {
	return fmt.Errorf("rootfs thaw failed after in-place resume, sandbox torn down: %w",
		errors.Join(sandbox.ErrSandboxLost, sandbox.ErrRootfsThawFailed, errors.New("FITHAW /: simulated failure")))
}

// A thaw give-up is booked as thaw_failed and nothing else is: the rollout
// reads kill_reason="thaw_failed" as the thaw-policy signal, so a regression
// here would silently move those deaths under resume_failed.
func TestLostSandboxKillReason(t *testing.T) {
	t.Parallel()

	assert.Equal(t, killReasonThawFailed, lostSandboxKillReason(lostToThaw()))
	assert.Equal(t, killReasonResumeFailed, lostSandboxKillReason(lostToResume()))
	assert.Equal(t, killReasonResumeFailed, lostSandboxKillReason(sandbox.ErrSandboxLost))
}

// The reason reaches the kill counter as its own label value.
func TestRecordSandboxKill_ThawFailedIsItsOwnLabel(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/server")
	counter, err := telemetry.GetCounter(meter, telemetry.OrchestratorSandboxKilledCounterName)
	require.NoError(t, err)

	recordSandboxKill(t.Context(), counter, lostSandboxKillReason(lostToThaw()))
	recordSandboxKill(t.Context(), counter, lostSandboxKillReason(lostToResume()))
	recordSandboxKill(t.Context(), counter, lostSandboxKillReason(lostToResume()))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.OrchestratorSandboxKilledCounterName) {
				continue
			}

			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)

			for _, dp := range sum.DataPoints {
				v, ok := dp.Attributes.Value(attribute.Key("kill_reason"))
				require.True(t, ok)
				got[v.AsString()] += dp.Value
			}
		}
	}

	assert.Equal(t, int64(1), got[killReasonThawFailed])
	assert.Equal(t, int64(2), got[killReasonResumeFailed])
}
