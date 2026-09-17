package orchestrator

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	e2bcatalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
)

type interposingRestoreCatalog struct {
	restorableCatalog

	beforeRestore func()
	afterRestore  func()
}

func (c interposingRestoreCatalog) RestoreSandbox(ctx context.Context, id string, info *e2bcatalog.SandboxInfo, ttl time.Duration) error {
	if c.beforeRestore != nil {
		c.beforeRestore()
	}
	err := c.restorableCatalog.RestoreSandbox(ctx, id, info, ttl)
	if c.afterRestore != nil {
		c.afterRestore()
	}

	return err
}

func TestRemoveSandbox_RefusalRouteRestorePreservesSuccessor(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		publishRoute bool
		afterRestore bool
		removeOnly   bool
	}{
		{name: "successor route published before restore", publishRoute: true},
		{name: "successor route not published yet"},
		{name: "successor route published after restore", publishRoute: true, afterRestore: true},
		{name: "record removed before route restore", removeOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newRefusalFixture(t, true, consts.LocalClusterID, refusedPauseErr())
			node, ok := f.o.nodes.Get(f.o.scopedNodeID(f.sbx.ClusterID, f.sbx.NodeID))
			require.True(t, ok)
			stub := &pauseStubClient{err: refusedPauseErr()}
			node.SetSandboxClient(stub)

			resumed := f.sbx
			resumed.ExecutionID = uuid.NewString()
			resumed.NodeID = "node-2"
			newRoute := &e2bcatalog.SandboxInfo{
				ExecutionID: resumed.ExecutionID, OrchestratorID: "successor-service", OrchestratorIP: "10.0.0.2",
				StartedAt: resumed.StartTime, MaxLengthInHours: 1,
			}
			catalog := f.o.routingCatalog
			replace := func() {
				stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
				require.NoError(t, err)
				require.Equal(t, sandbox.StateRunning, stored.State)
				require.Equal(t, f.sbx.ExecutionID, stored.ExecutionID)

				if tc.removeOnly {
					f.o.sandboxStore.Remove(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, f.sbx.ExecutionID)

					return
				}
				require.NoError(t, f.o.sandboxStore.Add(t.Context(), resumed, nil))
				if tc.publishRoute {
					require.NoError(t, catalog.StoreSandbox(t.Context(), resumed.SandboxID, newRoute, time.Hour))
				}
			}
			interposed := interposingRestoreCatalog{restorableCatalog: catalog}
			if tc.afterRestore {
				interposed.afterRestore = replace
			} else {
				interposed.beforeRestore = replace
			}
			f.o.routingCatalog = interposed

			err := f.removePause(t)
			require.ErrorIs(t, err, ErrSandboxNotFound)
			require.ErrorIs(t, err, sandbox.ErrExecutionMismatch)
			assert.Zero(t, stub.deleteCount())

			stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
			if tc.removeOnly {
				require.ErrorIs(t, err, sandbox.ErrNotFound)
			} else {
				require.NoError(t, err)
				assert.Equal(t, resumed.ExecutionID, stored.ExecutionID)
				assert.Equal(t, resumed.NodeID, stored.NodeID)
				assert.Equal(t, sandbox.StateRunning, stored.State)
				assert.True(t, stored.RefusedUntil.IsZero())
			}

			route, err := catalog.GetSandbox(t.Context(), f.sbx.SandboxID)
			if tc.publishRoute {
				require.NoError(t, err)
				assert.Equal(t, newRoute.ExecutionID, route.ExecutionID)
				assert.Equal(t, newRoute.OrchestratorID, route.OrchestratorID)
				assert.Equal(t, newRoute.OrchestratorIP, route.OrchestratorIP)
			} else {
				require.ErrorIs(t, err, e2bcatalog.ErrSandboxNotFound)
			}
			assert.Never(t, func() bool { return f.recorder.stoppedCount() != 0 }, 200*time.Millisecond, 10*time.Millisecond)
			assert.Equal(t, map[string]int64{"superseded/request": 1}, f.restoreOutcomes(t))
		})
	}
}

func TestRemoveSandbox_RefusalRouteRestorePreservesCheckpoint(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, refusedPauseErr())
	f.o.routingCatalog = interposingRestoreCatalog{
		restorableCatalog: f.o.routingCatalog,
		beforeRestore: func() {
			_, err := f.o.sandboxStore.Update(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, func(sbx sandbox.Sandbox) (sandbox.Sandbox, error) {
				sbx.State = sandbox.StateSnapshotting

				return sbx, nil
			})
			require.NoError(t, err)
		},
	}

	require.ErrorIs(t, f.removePause(t), PauseQueueExhaustedError{})
	stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, sandbox.StateSnapshotting, stored.State)
	route, err := f.o.routingCatalog.GetSandbox(t.Context(), f.sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, f.sbx.ExecutionID, route.ExecutionID)
}

type verificationReadFailureHook struct {
	armed atomic.Bool
	fired atomic.Bool
}

func (h *verificationReadFailureHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *verificationReadFailureHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *verificationReadFailureHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "get" && strings.Contains(cmd.Args()[1].(string), ":sandboxes:") && h.armed.CompareAndSwap(true, false) {
			h.fired.Store(true)

			return errors.New("verification read unavailable")
		}

		return next(ctx, cmd)
	}
}

func TestRemoveSandbox_VerificationReadFailurePreservesSandbox(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"restored execution", "successor execution"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			hook := &verificationReadFailureHook{}
			f := newRefusalFixture(t, true, consts.LocalClusterID, refusedPauseErr(), hook)
			node, ok := f.o.nodes.Get(f.o.scopedNodeID(f.sbx.ClusterID, f.sbx.NodeID))
			require.True(t, ok)
			stub := &pauseStubClient{err: refusedPauseErr()}
			node.SetSandboxClient(stub)

			var expected sandbox.Sandbox
			var expectedRoute *e2bcatalog.SandboxInfo
			catalog := f.o.routingCatalog
			f.o.routingCatalog = interposingRestoreCatalog{
				restorableCatalog: catalog,
				afterRestore: func() {
					stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
					require.NoError(t, err)
					require.Equal(t, sandbox.StateRunning, stored.State)
					route, err := catalog.GetSandbox(t.Context(), f.sbx.SandboxID)
					require.NoError(t, err)
					require.Equal(t, f.sbx.ExecutionID, route.ExecutionID)

					if name == "successor execution" {
						resumed := f.sbx
						resumed.ExecutionID = uuid.NewString()
						resumed.NodeID = "node-2"
						require.NoError(t, f.o.sandboxStore.Add(t.Context(), resumed, nil))
						route.ExecutionID = resumed.ExecutionID
						route.OrchestratorID = "successor-service"
						route.OrchestratorIP = "10.0.0.2"
						require.NoError(t, catalog.StoreSandbox(t.Context(), resumed.SandboxID, route, time.Hour))
					}
					expected, err = f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
					require.NoError(t, err)
					expectedRoute = route
					hook.armed.Store(true)
				},
			}

			err := f.removePause(t)
			require.True(t, hook.fired.Load())
			assert.Zero(t, stub.deleteCount())
			require.ErrorIs(t, err, PauseQueueExhaustedError{})
			stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
			require.NoError(t, err)
			assert.Equal(t, expected, stored)
			route, err := catalog.GetSandbox(t.Context(), f.sbx.SandboxID)
			require.NoError(t, err)
			assert.Equal(t, expectedRoute, route)
			assert.Never(t, func() bool { return f.recorder.stoppedCount() != 0 }, 200*time.Millisecond, 10*time.Millisecond)
			assert.Equal(t, map[string]int64{"restored/request": 1}, f.restoreOutcomes(t))
		})
	}
}
