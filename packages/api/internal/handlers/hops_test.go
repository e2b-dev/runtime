package handlers

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/servicetest"
)

func TestSecretsManagementClientDialsWithTheHopCredentials(t *testing.T) {
	t.Parallel()

	creds := servicetest.NewCredentials()
	conn, err := newSecretsManagementClient(servicetest.GRPCServer(t), creds)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	conn.Connect()
	servicetest.AwaitHandshake(t, creds)
}

func TestWebhooksClientDialsWithTheHopCredentials(t *testing.T) {
	t.Parallel()

	creds := servicetest.NewCredentials()
	conn, err := newWebhooksClient(servicetest.GRPCServer(t), creds)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	conn.Connect()
	servicetest.AwaitHandshake(t, creds)
}
