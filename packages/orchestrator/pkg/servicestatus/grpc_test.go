package servicestatus

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

func TestHandleOverride_MapsCodesAndCountsReasons(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	refused, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/servicestatus").Int64Counter("refused")
	require.NoError(t, err)
	machine := New(NopStore{}, time.Now)

	resp, err := HandleOverride(t.Context(), machine.Override, &orchestratorinfo.ServiceStatusChangeRequest{ServiceStatus: standby, Owner: "x"}, refused)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Empty(t, refusals(t, reader))

	resp, err = HandleOverride(t.Context(), machine.Override, &orchestratorinfo.ServiceStatusChangeRequest{ServiceStatus: standby, Owner: "Park"}, refused)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Nil(t, resp)

	_, err = HandleOverride(t.Context(), machine.Override, &orchestratorinfo.ServiceStatusChangeRequest{ServiceStatus: standby, Owner: "y"}, refused)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, "cannot override node status to Standby: owner_mismatch", status.Convert(err).Message())
	require.Equal(t, map[string]int64{"invalid_owner": 1, "owner_mismatch": 1}, refusals(t, reader))

	_, err = HandleOverride(t.Context(), machine.Override, &orchestratorinfo.ServiceStatusChangeRequest{ServiceStatus: standby, Owner: "y"}, nil)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "a nil counter is skipped, not dereferenced")
	require.Equal(t, standby, machine.Snapshot().Status)
	require.Equal(t, "x", machine.Snapshot().Owner)
}

// refusals reads the counter back by reason.
func refusals(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				reason, _ := dp.Attributes.Value(attribute.Key("reason"))
				out[reason.AsString()] = dp.Value
			}
		}
	}

	return out
}

func TestFill_SetsOwnerAndCapabilities(t *testing.T) {
	t.Parallel()

	snap := at(t0, standby, "x")
	resp := &orchestratorinfo.ServiceInfoResponse{NodeId: "node"}
	Fill(resp, snap)

	require.Equal(t, standby, resp.GetServiceStatus())
	require.Equal(t, "x", resp.GetStatusOwner())
	require.True(t, t0.Equal(resp.GetServiceStatusChangedAt().AsTime()))
	require.Equal(t, []string{CapabilityParkOwner}, resp.GetCapabilities())
	require.Equal(t, "node", resp.GetNodeId(), "the rest of the response is left alone")
}
