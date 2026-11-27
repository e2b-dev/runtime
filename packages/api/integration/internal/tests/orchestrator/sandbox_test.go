package orchestrator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
)

func TestList(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	client := setup.GetOrchestratorClient(t, ctx)

	_, err := client.List(ctx, &emptypb.Empty{})
	require.NoError(t, err)
}
