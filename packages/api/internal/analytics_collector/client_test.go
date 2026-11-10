package analyticscollector

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/servicetest"
)

func TestNewAnalyticsTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		host       string
		useTLS     bool
		wantTarget string
	}{
		{
			name:       "bare hostname defaults to the HTTPS port",
			host:       "collector.example.com",
			useTLS:     true,
			wantTarget: "collector.example.com:443",
		},
		{
			name:       "explicit port is kept",
			host:       "collector.example.com:8443",
			useTLS:     true,
			wantTarget: "collector.example.com:8443",
		},
		{
			// The local analytics collector: plaintext gRPC on loopback. Joining
			// :443 onto this used to produce "[localhost:5051]:443", which the
			// resolver could not resolve.
			name:       "plaintext local collector",
			host:       "localhost:5051",
			useTLS:     false,
			wantTarget: "localhost:5051",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			analytics, err := NewAnalytics(tt.host, "api-token", tt.useTLS, insecure.NewCredentials())
			require.NoError(t, err)

			t.Cleanup(func() { _ = analytics.Close() })

			require.NotNil(t, analytics.connection)
			assert.Equal(t, tt.wantTarget, analytics.connection.Target())
		})
	}
}

func TestNewAnalyticsWithoutHostIsNoop(t *testing.T) {
	t.Parallel()

	analytics, err := NewAnalytics("", "", true, insecure.NewCredentials())
	require.NoError(t, err)

	assert.Nil(t, analytics.client)
	assert.Nil(t, analytics.connection)

	res, err := analytics.InstanceStarted(t.Context(), &InstanceStartedEvent{})
	require.NoError(t, err)
	assert.Nil(t, res)

	assert.NoError(t, analytics.Close())
}

func TestNewAnalyticsPlaintextDialsWithTheHopCredentials(t *testing.T) {
	t.Parallel()

	creds := servicetest.NewCredentials()
	analytics, err := NewAnalytics(servicetest.GRPCServer(t), "api-token", false, creds)
	require.NoError(t, err)
	t.Cleanup(func() { _ = analytics.Close() })

	analytics.connection.Connect()
	servicetest.AwaitHandshake(t, creds)
}

// The TLS switch keeps its own credentials: the in-cluster hop's are not used.
func TestNewAnalyticsTLSKeepsItsOwnCredentials(t *testing.T) {
	t.Parallel()

	creds := servicetest.NewCredentials()
	analytics, err := NewAnalytics(servicetest.GRPCServer(t), "api-token", true, creds)
	require.NoError(t, err)
	t.Cleanup(func() { _ = analytics.Close() })

	// TLS against the plaintext server fails, which proves the dial happened.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	analytics.connection.Connect()
	for state := analytics.connection.GetState(); state != connectivity.TransientFailure; state = analytics.connection.GetState() {
		require.True(t, analytics.connection.WaitForStateChange(ctx, state), "still %s", state)
	}
	assert.Zero(t, creds.Handshakes())
}
