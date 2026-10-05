package orchestrator

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

type supersededWaiterContextKey struct{}

type supersededWaiterHook struct {
	ready chan struct{}
	once  sync.Once
}

func (h *supersededWaiterHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *supersededWaiterHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *supersededWaiterHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if err == nil && ctx.Value(supersededWaiterContextKey{}) == true && cmd.Name() == "get" && strings.Contains(cmd.Args()[1].(string), ":transition:") {
			h.once.Do(func() { close(h.ready) })
		}

		return err
	}
}

func TestRemoveSandbox_SupersededRestoreInformsWaiters(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		action *sandbox.StateAction
	}{
		{name: "state waiter"},
		{name: "successor kill", action: &sandbox.StateActionKill},
		{name: "successor pause", action: &sandbox.StateActionPause},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ready := make(chan struct{})
			f := newRefusalFixture(t, true, consts.LocalClusterID, refusedPauseErr(), &supersededWaiterHook{ready: ready})
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			waitCtx := context.WithValue(ctx, supersededWaiterContextKey{}, true)
			resumed := f.sbx
			resumed.ExecutionID = uuid.NewString()
			type result struct {
				transition  sandbox.StateTransition
				alreadyDone bool
				finish      func(context.Context, error)
				err         error
			}
			waiter := make(chan result, 1)
			node, ok := f.o.nodes.Get(f.o.scopedNodeID(f.sbx.ClusterID, f.sbx.NodeID))
			require.True(t, ok)
			stub := &pauseStubClient{err: refusedPauseErr(), onPause: func() {
				require.NoError(t, f.o.sandboxStore.Add(ctx, resumed, nil))
				go func() {
					if tc.action == nil {
						waiter <- result{err: f.o.sandboxStore.WaitForStateChange(waitCtx, resumed.TeamID, resumed.SandboxID)}

						return
					}
					transition, alreadyDone, finish, err := f.o.sandboxStore.StartRemoving(waitCtx, resumed.TeamID, resumed.SandboxID, sandbox.RemoveOpts{
						Action: *tc.action, ExpectExecutionID: resumed.ExecutionID,
					})
					waiter <- result{transition: transition, alreadyDone: alreadyDone, finish: finish, err: err}
				}()
				// Complete the old pause only after the waiter has read its transition token.
				select {
				case <-ready:
				case <-ctx.Done():
					t.Fatal("waiter did not read the transition token")
				}
			}}
			node.SetSandboxClient(stub)

			err := f.o.RemoveSandbox(ctx, f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionPause})
			require.ErrorIs(t, err, ErrSandboxNotFound)
			require.ErrorIs(t, err, sandbox.ErrExecutionMismatch)
			select {
			case got := <-waiter:
				if tc.action == nil {
					require.ErrorIs(t, got.err, sandbox.ErrTransitionRestored)
				} else {
					require.NoError(t, got.err)
					require.False(t, got.alreadyDone)
					require.NotNil(t, got.finish)
					defer got.finish(context.WithoutCancel(ctx), nil)
					assert.Equal(t, resumed.ExecutionID, got.transition.Sandbox.ExecutionID)
					assert.Equal(t, tc.action.TargetState, got.transition.Sandbox.State)
				}
			case <-ctx.Done():
				t.Fatal("waiter did not finish")
			}
			stored, err := f.o.sandboxStore.Get(ctx, resumed.TeamID, resumed.SandboxID)
			require.NoError(t, err)
			assert.Equal(t, resumed.ExecutionID, stored.ExecutionID)
			assert.Zero(t, stub.deleteCount())
		})
	}
}
