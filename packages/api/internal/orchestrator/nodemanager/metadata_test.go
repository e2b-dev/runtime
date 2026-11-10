package nodemanager

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	grpcshared "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// The create RPC carries only the resume marker and the team the server
// labels its RPC metrics with: routing is the node's own and no catalog event
// rides on the metadata, cluster node or not.
func TestGetSandboxCreateCtx_CarriesOnlyTheMetricMarkers(t *testing.T) {
	t.Parallel()

	for name, clusterID := range map[string]uuid.UUID{"cluster node": uuid.New(), "local node": consts.LocalClusterID} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			node := NewTestNode("node-1", api.NodeStatusReady, 0, 8)
			node.ClusterID = clusterID

			_, ctx := node.GetSandboxCreateCtx(t.Context(), &orchestrator.SandboxCreateRequest{
				Sandbox: &orchestrator.SandboxConfig{SandboxId: "sbx-1", ExecutionId: "exec-1", Snapshot: true, TeamId: "team-1"},
			})
			md, ok := metadata.FromOutgoingContext(ctx)
			require.True(t, ok)

			assert.Equal(t, []string{"true"}, md.Get(grpcshared.IsResumeMetadataKey))
			assert.Equal(t, []string{"team-1"}, md.Get(grpcshared.TeamIDMetadataKey))
			assert.Len(t, md, 2)
		})
	}
}

// The other sandbox RPCs carry the team too, so every orchestrator RPC
// metric can be split by it.
func TestGetSandboxClient_CarriesTheTeamID(t *testing.T) {
	t.Parallel()

	node := NewTestNode("node-1", api.NodeStatusReady, 0, 8)

	_, ctx := node.GetSandboxClient(t.Context(), "team-1")
	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)

	assert.Equal(t, []string{"team-1"}, md.Get(grpcshared.TeamIDMetadataKey))
}
