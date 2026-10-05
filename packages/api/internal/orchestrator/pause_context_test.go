package orchestrator

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

type pauseContextKey struct{}

// transitionCommitHook runs afterCommit once the pause transition is written,
// before the snapshot upsert: the earliest point a caller can cancel after
// the removal is committed.
type transitionCommitHook struct {
	afterCommit func()

	fired atomic.Bool
}

func (h *transitionCommitHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *transitionCommitHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *transitionCommitHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if err == nil && ctx.Value(pauseContextKey{}) != nil && isStartTransitionScript(cmd) {
			h.fired.Store(true)
			h.afterCommit()
		}

		return err
	}
}

// startTransitionScript runs with three keys: the record, the transition and
// its result. The finish script carries two.
func isStartTransitionScript(cmd redis.Cmder) bool {
	if cmd.Name() != "evalsha" && cmd.Name() != "eval" {
		return false
	}
	args := cmd.Args()
	if len(args) < 5 {
		return false
	}
	numKeys, _ := args[2].(int)
	key, _ := args[4].(string)

	return numKeys == 3 && strings.Contains(key, ":transition:")
}

type pauseContextClient struct {
	orchestrator.SandboxServiceClient

	pause func(context.Context, *orchestrator.SandboxPauseRequest) error
}

func (c *pauseContextClient) Pause(ctx context.Context, in *orchestrator.SandboxPauseRequest, _ ...grpc.CallOption) (*orchestrator.SandboxPauseResponse, error) {
	if err := c.pause(ctx, in); err != nil {
		return nil, err
	}

	return &orchestrator.SandboxPauseResponse{}, nil
}

func TestRemoveSandbox_PausePersistsSnapshotAfterCallerCancellation(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name          string
		beforeUpsert  bool
		callerTimeout time.Duration
	}{
		{name: "before_snapshot_upsert", beforeUpsert: true, callerTimeout: 5 * time.Second},
		{name: "during_rpc", callerTimeout: 5 * time.Second},
		{name: "long_caller_deadline", callerTimeout: 10 * time.Minute},
		{name: "no_caller_deadline"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var f refusalFixture
			var cancel context.CancelFunc
			hook := &transitionCommitHook{afterCommit: func() {
				stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
				require.NoError(t, err)
				require.Equal(t, sandbox.StatePausing, stored.State)
				if tt.beforeUpsert {
					cancel()
				}
			}}
			f = newRefusalFixture(t, true, consts.LocalClusterID, nil, hook)
			ctx := context.WithValue(t.Context(), pauseContextKey{}, "request-value")
			if tt.callerTimeout > 0 {
				ctx, cancel = context.WithTimeout(ctx, tt.callerTimeout)
			} else {
				ctx, cancel = context.WithCancel(ctx)
			}
			defer cancel()

			var snapshotBuildID uuid.UUID
			node := f.o.GetNode(f.sbx.ClusterID, f.sbx.NodeID)
			require.NotNil(t, node)
			node.SetSandboxClient(&pauseContextClient{pause: func(pauseCtx context.Context, in *orchestrator.SandboxPauseRequest) error {
				cancel()
				assert.Equal(t, "request-value", pauseCtx.Value(pauseContextKey{}))
				deadline, ok := pauseCtx.Deadline()
				require.True(t, ok)
				assert.WithinDuration(t, time.Now().Add(pauseTimeout), deadline, 2*time.Second, "the RPC runs on the pause budget, not the caller's")

				var err error
				snapshotBuildID, err = uuid.Parse(in.GetBuildId())
				require.NoError(t, err)

				return pauseCtx.Err()
			}})

			require.NoError(t, f.o.RemoveSandbox(ctx, f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionPause}))
			require.True(t, hook.fired.Load())
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			require.NotEqual(t, uuid.Nil, snapshotBuildID)

			snapshot, err := f.o.sqlcDB.GetLastSnapshot(t.Context(), f.sbx.SandboxID)
			require.NoError(t, err, "the completed snapshot must remain available for resume")
			assert.Equal(t, snapshotBuildID, snapshot.EnvBuild.ID)
			assert.Equal(t, types.BuildStatusSuccess, snapshot.EnvBuild.Status)
			assert.NotNil(t, snapshot.EnvBuild.FinishedAt)
			assert.Equal(t, f.sbx.TeamID, snapshot.Snapshot.TeamID)

			_, err = f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
			require.ErrorIs(t, err, sandbox.ErrNotFound)
		})
	}
}

func TestRemoveSandbox_CanceledPauseDoesNotAcquireTransition(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, nil)

	err := f.o.RemoveSandbox(cancelledContext(t), f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionPause})
	require.Error(t, err)

	stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, sandbox.StateRunning, stored.State)
}
