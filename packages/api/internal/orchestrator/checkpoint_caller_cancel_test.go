package orchestrator

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// gatedCheckpointClient answers Checkpoint with checkpoint and counts the
// Checkpoints and Deletes the node receives.
type gatedCheckpointClient struct {
	orchestrator.SandboxServiceClient

	checkpoint  func(context.Context) error
	checkpoints atomic.Int32
	deletes     atomic.Int32
}

func (c *gatedCheckpointClient) Checkpoint(ctx context.Context, _ *orchestrator.SandboxCheckpointRequest, _ ...grpc.CallOption) (*orchestrator.SandboxCheckpointResponse, error) {
	c.checkpoints.Add(1)
	if err := c.checkpoint(ctx); err != nil {
		return nil, err
	}

	return &orchestrator.SandboxCheckpointResponse{}, nil
}

func (c *gatedCheckpointClient) Delete(context.Context, *orchestrator.SandboxDeleteRequest, ...grpc.CallOption) (*emptypb.Empty, error) {
	c.deletes.Add(1)

	return &emptypb.Empty{}, nil
}

type checkpointCaller struct {
	call func(ctx context.Context, o *Orchestrator, teamID uuid.UUID, sandboxID string) error
}

var checkpointCallers = map[string]checkpointCaller{
	"snapshot template": {
		call: func(ctx context.Context, o *Orchestrator, teamID uuid.UUID, sandboxID string) error {
			_, err := o.CreateSnapshotTemplate(ctx, teamID, sandboxID, SnapshotTemplateOpts{Tag: "default"})

			return err
		},
	},
	"checkpoint": {
		call: func(ctx context.Context, o *Orchestrator, teamID uuid.UUID, sandboxID string) error {
			return o.CheckpointSandbox(ctx, teamID, sandboxID)
		},
	},
}

func checkpointFixture(t *testing.T, checkpoint func(context.Context) error) (refusalFixture, *gatedCheckpointClient) {
	t.Helper()

	f := newRefusalFixture(t, true, consts.LocalClusterID, nil)
	client := &gatedCheckpointClient{checkpoint: checkpoint}
	node := f.o.GetNode(f.sbx.ClusterID, f.sbx.NodeID)
	require.NotNil(t, node)
	node.SetSandboxClient(client)

	return f, client
}

// A checkpoint's work can outlive the request, so a draining replica refuses
// one before touching the sandbox, the same as a pause.
func TestCheckpoint_DrainingRefusesBeforeTheTransition(t *testing.T) {
	t.Parallel()

	for name, c := range checkpointCallers {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			f, client := checkpointFixture(t, func(context.Context) error { return nil })
			require.NoError(t, f.o.Drain(t.Context()))

			err := c.call(t.Context(), f.o, f.sbx.TeamID, f.sbx.SandboxID)
			require.ErrorIs(t, err, ErrDraining)

			assert.Zero(t, client.checkpoints.Load())
			stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
			require.NoError(t, err)
			assert.Equal(t, sandbox.StateRunning, stored.State)
		})
	}
}
