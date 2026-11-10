package server_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
)

var metricReader = sdkmetric.NewManualReader()

func TestMain(m *testing.M) {
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader)))
	m.Run()
}

// A refusal inside a gRPC OK response is counted with its outcome. A gRPC
// error and a response without an outcome are not counted.
//
// The outcome counter has no attribute that tells tests apart, so this test
// is not parallel: Go starts the parallel tests of the package only after it
// has returned, and the counts it reads are its own.
func TestOutcomeCounter(t *testing.T) { //nolint:paralleltest // reads exact counts of the process-wide meter provider
	ctx := t.Context()
	client, _ := newClient(t)
	project, other := uuid.NewString(), uuid.NewString()
	before := outcomeCounts(t)

	_, err := client.Reserve(ctx, reserveRequest(project, sandboxID, uuid.NewString(), 1))
	require.NoError(t, err)
	exceeded, err := client.Reserve(ctx, reserveRequest(project, "i00000000000000000001", uuid.NewString(), 1))
	require.NoError(t, err)
	require.Equal(t, scheduling.ReserveOutcome_RESERVE_OUTCOME_LIMIT_EXCEEDED, exceeded.GetOutcome())
	_, err = client.Reserve(ctx, reserveRequest(other, sandboxID, uuid.NewString(), 1))
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	_, err = client.Get(ctx, scheduling.GetRequest_builder{ProjectId: project, SandboxId: sandboxID}.Build())
	require.NoError(t, err)

	after := outcomeCounts(t)
	for key := range after {
		after[key] -= before[key]
		if after[key] == 0 {
			delete(after, key)
		}
	}
	require.Equal(t, map[[2]string]int64{
		{"Reserve", scheduling.ReserveOutcome_RESERVE_OUTCOME_RESERVED.String()}:       1,
		{"Reserve", scheduling.ReserveOutcome_RESERVE_OUTCOME_LIMIT_EXCEEDED.String()}: 1,
	}, after)
}

// outcomeCounts returns the cumulative count of every series that has only a
// method and an outcome attribute.
func outcomeCounts(t *testing.T) map[[2]string]int64 {
	t.Helper()

	var collected metricdata.ResourceMetrics
	require.NoError(t, metricReader.Collect(t.Context(), &collected))
	counts := map[[2]string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, point := range sum.DataPoints {
				method, hasMethod := point.Attributes.Value("method")
				outcome, hasOutcome := point.Attributes.Value("outcome")
				if hasMethod && hasOutcome {
					require.Equal(t, 2, point.Attributes.Len(), "only method and outcome")
					counts[[2]string{method.AsString(), outcome.AsString()}] += point.Value
				}
			}
		}
	}

	return counts
}
