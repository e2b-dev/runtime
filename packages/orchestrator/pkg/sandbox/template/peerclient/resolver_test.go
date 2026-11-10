package peerclient

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/servicetest"
)

func TestPeersAreDialledWithTheHopCredentials(t *testing.T) {
	t.Parallel()

	creds := servicetest.NewCredentials()
	resolver, ok := NewResolver(NopRegistry(), "self:5008", creds).(*peerResolver)
	require.True(t, ok)
	t.Cleanup(resolver.Close)

	conn, err := resolver.getOrDialPeer(t.Context(), servicetest.GRPCServer(t))
	require.NoError(t, err)
	conn.Connect()
	servicetest.AwaitHandshake(t, creds)
}
