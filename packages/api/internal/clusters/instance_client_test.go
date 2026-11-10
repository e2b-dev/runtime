package clusters

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/connectivity"

	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/servicetest"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func TestLocalInstanceClientDialsWithTheHopCredentials(t *testing.T) {
	t.Parallel()

	creds := servicetest.NewCredentials()
	client, err := createClient(telemetry.NewNoopClient(), servicetest.GRPCServer(t), false, creds)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	client.Connection.Connect()
	servicetest.AwaitHandshake(t, creds)
}

// A remote cluster's TLS endpoint keeps its own credentials.
func TestRemoteTLSInstanceClientKeepsItsOwnCredentials(t *testing.T) {
	t.Parallel()

	creds := servicetest.NewCredentials()
	client, err := createClient(telemetry.NewNoopClient(), servicetest.GRPCServer(t), true, creds)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	// TLS against the plaintext server fails, which proves the dial happened.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	client.Connection.Connect()
	for state := client.Connection.GetState(); state != connectivity.TransientFailure; state = client.Connection.GetState() {
		require.True(t, client.Connection.WaitForStateChange(ctx, state), "still %s", state)
	}
	assert.Zero(t, creds.Handshakes())
}
