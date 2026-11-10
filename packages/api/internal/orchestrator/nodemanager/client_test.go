package nodemanager

import (
	"testing"

	"github.com/stretchr/testify/require"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/servicetest"
)

func TestNewClientDialsWithTheHopCredentials(t *testing.T) {
	t.Parallel()

	creds := servicetest.NewCredentials()
	client, err := NewClient(tracenoop.NewTracerProvider(), metricnoop.NewMeterProvider(), servicetest.GRPCServer(t), creds)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	client.Connection.Connect()
	servicetest.AwaitHandshake(t, creds)
}
