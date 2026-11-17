package setup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

func GetOrchestratorClient(tb testing.TB, ctx context.Context) orchestrator.SandboxServiceClient {
	tb.Helper()

	conn, err := grpc.NewClient(OrchestratorHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(tb, err, "failed to establish GRPC connection")

	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	return orchestrator.NewSandboxServiceClient(conn)
}
