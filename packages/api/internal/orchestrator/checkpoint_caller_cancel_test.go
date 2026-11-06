package orchestrator

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// gatedCheckpointClient answers Checkpoint with checkpoint, and with
// response once it succeeds, accepts every Update, and counts the
// Checkpoints and Deletes the node receives.
type gatedCheckpointClient struct {
	orchestrator.SandboxServiceClient

	checkpoint  func(context.Context) error
	response    *orchestrator.SandboxCheckpointResponse
	checkpoints atomic.Int32
	deletes     atomic.Int32
}

func (c *gatedCheckpointClient) Checkpoint(ctx context.Context, _ *orchestrator.SandboxCheckpointRequest, _ ...grpc.CallOption) (*orchestrator.SandboxCheckpointResponse, error) {
	c.checkpoints.Add(1)
	if err := c.checkpoint(ctx); err != nil {
		return nil, err
	}
	if c.response != nil {
		return c.response, nil
	}

	return &orchestrator.SandboxCheckpointResponse{}, nil
}

func (c *gatedCheckpointClient) Update(context.Context, *orchestrator.SandboxUpdateRequest, ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (c *gatedCheckpointClient) Delete(context.Context, *orchestrator.SandboxDeleteRequest, ...grpc.CallOption) (*emptypb.Empty, error) {
	c.deletes.Add(1)

	return &emptypb.Empty{}, nil
}

// checkpointRestoreHook acts on the next record restore once armed: before
// runs first, with the record key the restore is about to write, and fail
// then fails the restore without sending it.
type checkpointRestoreHook struct {
	before func(ctx context.Context, recordKey string) error
	fail   bool

	armed atomic.Bool
}

func (h *checkpointRestoreHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *checkpointRestoreHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *checkpointRestoreHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if isRestoreRunningScript(cmd) && h.armed.CompareAndSwap(true, false) {
			if h.before != nil {
				if err := h.before(ctx, cmd.Args()[3].(string)); err != nil {
					return err
				}
			}
			if h.fail {
				return errors.New("the record store is unavailable")
			}
		}

		return next(ctx, cmd)
	}
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

func checkpointFixture(t *testing.T, checkpoint func(context.Context) error, hooks ...redis.Hook) (refusalFixture, *gatedCheckpointClient) {
	t.Helper()

	f := newRefusalFixture(t, true, consts.LocalClusterID, nil, hooks...)
	client := &gatedCheckpointClient{checkpoint: checkpoint}
	node := f.o.GetNode(f.sbx.ClusterID, f.sbx.NodeID)
	require.NotNil(t, node)
	node.SetSandboxClient(client)

	return f, client
}

// A checkpoint the node refuses because another is in flight comes back as
// its own retryable refusal, with the sandbox restored to Running and its VM
// untouched, for the fork and the snapshot-template caller alike.
func TestCheckpoint_InProgressRefusalRestores(t *testing.T) {
	t.Parallel()

	for name, c := range checkpointCallers {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			refusal, err := status.New(codes.FailedPrecondition, "a checkpoint is already in progress for sandbox 'x'").WithDetails(
				&orchestrator.UserError{Code: orchestrator.UserErrorCode_CHECKPOINT_IN_PROGRESS, HttpStatus: 503},
			)
			require.NoError(t, err)
			f, client := checkpointFixture(t, func(context.Context) error { return refusal.Err() })

			err = c.call(t.Context(), f.o, f.sbx.TeamID, f.sbx.SandboxID)
			require.ErrorIs(t, err, PauseRefusedCheckpointInProgressError{})
			require.ErrorIs(t, err, PauseQueueExhaustedError{})

			assert.Equal(t, int32(1), client.checkpoints.Load())
			assert.Zero(t, client.deletes.Load(), "the sandbox stays on its node")
			stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
			require.NoError(t, err)
			assert.Equal(t, sandbox.StateRunning, stored.State)
		})
	}
}

// A checkpoint the node refuses because the guest agent did not answer its
// health probe comes back as its own retryable refusal, apart from the
// in-progress one, with the sandbox restored to Running and its VM untouched,
// for the fork and the snapshot-template caller alike.
func TestCheckpoint_EnvdUnresponsiveRefusalRestores(t *testing.T) {
	t.Parallel()

	for name, c := range checkpointCallers {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			refusal, err := status.New(codes.ResourceExhausted, "sandbox 'x' guest agent is not responding, please retry").WithDetails(
				&orchestrator.UserError{Code: orchestrator.UserErrorCode_ENVD_UNRESPONSIVE, HttpStatus: 503},
			)
			require.NoError(t, err)
			f, client := checkpointFixture(t, func(context.Context) error { return refusal.Err() })

			err = c.call(t.Context(), f.o, f.sbx.TeamID, f.sbx.SandboxID)
			require.ErrorIs(t, err, PauseRefusedEnvdUnresponsiveError{})
			require.ErrorIs(t, err, PauseQueueExhaustedError{})
			require.NotErrorIs(t, err, PauseRefusedCheckpointInProgressError{})

			assert.Equal(t, int32(1), client.checkpoints.Load())
			assert.Zero(t, client.deletes.Load(), "the sandbox stays on its node")
			stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
			require.NoError(t, err)
			assert.Equal(t, sandbox.StateRunning, stored.State)
		})
	}
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
