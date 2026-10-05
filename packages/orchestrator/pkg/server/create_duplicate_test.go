//go:build linux

package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// A create for a sandbox ID this node already runs is refused before any
// resource is touched: the template cache is nil here, so reaching it would
// panic. AlreadyExists tells the caller the VM exists — retrying elsewhere
// would only make a second one.
func TestCreate_RefusesASandboxAlreadyLiveOnTheNode(t *testing.T) {
	t.Parallel()

	s := duplicateCreateTestServer()

	live := drainTestSandbox(t, "lifecycle-live")
	live.Runtime.ExecutionID = "exec-live"
	require.NoError(t, s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), live))

	_, err := s.Create(t.Context(), &orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{
			SandboxId:   live.Runtime.SandboxID,
			ExecutionId: "exec-new",
			Snapshot:    true,
		},
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.AlreadyExists, st.Code())

	got, ok := s.sandboxFactory.Sandboxes.Get(live.Runtime.SandboxID)
	require.True(t, ok)
	assert.Same(t, live, got, "the running sandbox is untouched")
	assert.Equal(t, 1, s.sandboxFactory.Sandboxes.Count())
}

// A create for a sandbox ID whose create is still in flight is refused just
// as early: the first create holds the ID from its entry, so the second
// never builds a VM it would only have to tear down.
func TestCreate_RefusesASandboxWhoseCreateIsInFlight(t *testing.T) {
	t.Parallel()

	s := duplicateCreateTestServer()

	inFlight, err := s.sandboxFactory.Sandboxes.Reserve("sandbox-1")
	require.NoError(t, err)

	_, err = s.Create(t.Context(), &orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{SandboxId: "sandbox-1", ExecutionId: "exec-second", Snapshot: true},
	})
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.AlreadyExists, st.Code())
	assert.Zero(t, s.sandboxFactory.Sandboxes.Count(), "nothing was registered")

	// The refusal must not have released the first create's hold.
	_, err = s.sandboxFactory.Sandboxes.Reserve("sandbox-1")
	require.ErrorIs(t, err, sandbox.ErrSandboxAlreadyRunning)
	inFlight.Release()
}

// A create that fails releases its hold on the ID, so a later create for the
// same sandbox is admitted.
func TestCreate_ReleasesTheIDWhenItFails(t *testing.T) {
	t.Parallel()

	s := duplicateCreateTestServer()
	s.info.MaxSandboxes.Store(0) // every create fails right after reserving

	_, err := s.Create(t.Context(), &orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{SandboxId: "sandbox-1", Snapshot: true},
	})
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.ResourceExhausted, st.Code())

	r, err := s.sandboxFactory.Sandboxes.Reserve("sandbox-1")
	require.NoError(t, err, "a failed create must not keep the ID")
	r.Release()
}

func TestFailedStartReturnsBeforeCleanupAndRetainsReservation(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s := duplicateCreateTestServer()
		r, err := s.sandboxFactory.Sandboxes.Reserve("sandbox-1")
		require.NoError(t, err)
		rollback := sandbox.NewCleanup()
		processExit, resourcesReleased := make(chan struct{}), make(chan struct{})
		cleanupErr := errors.New("cleanup reported an error")
		stops, releases := 0, 0
		rollback.AddPriority(t.Context(), func(ctx context.Context) error {
			assert.NoError(t, ctx.Err())
			stops++
			<-processExit

			return nil
		})
		rollback.Add(t.Context(), func(ctx context.Context) error {
			assert.NoError(t, ctx.Err())
			releases++
			<-resourcesReleased

			return cleanupErr
		})
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		s.finishSandboxStart(ctx, r, rollback, context.Canceled)
		require.Equal(t, int64(1), s.info.OutstandingWork())
		synctest.Wait()
		require.Equal(t, 1, stops)
		require.Zero(t, releases)
		_, err = s.Create(t.Context(), &orchestrator.SandboxCreateRequest{
			Sandbox: &orchestrator.SandboxConfig{SandboxId: "sandbox-1"},
		})
		require.Equal(t, codes.AlreadyExists, status.Code(err))

		close(processExit)
		synctest.Wait()
		require.Equal(t, 1, releases)
		require.Equal(t, int64(1), s.info.OutstandingWork(), "process exit alone does not finish cleanup")
		_, err = s.sandboxFactory.Sandboxes.Reserve("sandbox-1")
		require.ErrorIs(t, err, sandbox.ErrSandboxAlreadyRunning)

		close(resourcesReleased)
		synctest.Wait()
		require.Zero(t, s.info.OutstandingWork())
		r, err = s.sandboxFactory.Sandboxes.Reserve("sandbox-1")
		require.NoError(t, err)
		r.Release()
	})
}

func TestSuccessfulStartReleasesReservationWithoutRollback(t *testing.T) {
	t.Parallel()

	s := duplicateCreateTestServer()
	r, err := s.sandboxFactory.Sandboxes.Reserve("sandbox-1")
	require.NoError(t, err)
	sbx := drainTestSandbox(t, "successful")
	require.NoError(t, r.MarkRunning(t.Context(), sbx))
	rollback := sandbox.NewCleanup()
	rollback.Add(t.Context(), func(context.Context) error {
		t.Error("successful start ran rollback")

		return nil
	})
	s.finishSandboxStart(t.Context(), r, rollback, nil)
	got, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
	require.True(t, live)
	require.Same(t, sbx, got)
	_, err = s.sandboxFactory.Sandboxes.Reserve(sbx.Runtime.SandboxID)
	require.ErrorIs(t, err, sandbox.ErrSandboxAlreadyRunning)
}

func TestFailedCheckpointRetainsReservationUntilBothCleanupsFinish(t *testing.T) {
	t.Parallel()

	for _, blocked := range []string{"predecessor", "successor"} {
		t.Run(blocked, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				s := duplicateCreateTestServer()
				old := drainTestSandbox(t, "old")
				require.NoError(t, s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), old))
				r, err := s.sandboxFactory.Sandboxes.MarkStoppingReserved(t.Context(), old.Runtime.SandboxID, old.LifecycleID)
				require.NoError(t, err)
				rollback := sandbox.NewCleanup()
				finish := make(chan struct{})
				closed := 0
				for _, participant := range []string{"predecessor", "successor"} {
					rollback.Add(t.Context(), func(ctx context.Context) error {
						assert.NoError(t, ctx.Err())
						if participant == blocked {
							<-finish
						}
						closed++

						return nil
					})
				}
				s.finishSandboxStart(t.Context(), r, rollback, errors.New("checkpoint failed"))
				synctest.Wait()
				_, err = s.sandboxFactory.Sandboxes.Reserve(old.Runtime.SandboxID)
				require.ErrorIs(t, err, sandbox.ErrSandboxAlreadyRunning)
				require.Equal(t, int64(1), s.info.OutstandingWork())

				close(finish)
				synctest.Wait()
				require.Equal(t, 2, closed)
				require.Zero(t, s.info.OutstandingWork())
				r, err = s.sandboxFactory.Sandboxes.Reserve(old.Runtime.SandboxID)
				require.NoError(t, err)
				r.Release()
			})
		})
	}
}

func TestMarkSandboxLiveRejectsForeignReservationBeforeHealthChecks(t *testing.T) {
	t.Parallel()

	s := duplicateCreateTestServer()
	owner, err := s.sandboxFactory.Sandboxes.Reserve("sandbox-1")
	require.NoError(t, err)
	t.Cleanup(owner.Release)
	foreign, err := s.sandboxFactory.Sandboxes.Reserve("other-sandbox")
	require.NoError(t, err)
	t.Cleanup(foreign.Release)
	sbx := drainTestSandbox(t, "candidate")
	require.Error(t, s.markSandboxLive(t.Context(), sbx, foreign))
	require.Equal(t, sandbox.StopReasonRegistrationFailed, sbx.GetStopReason())
	require.Empty(t, s.sandboxFactory.Sandboxes.Items())
	require.NoError(t, owner.MarkRunning(t.Context(), sbx))
}

func TestCheckpointRefusesBusyReservationWithoutStoppingSandbox(t *testing.T) {
	t.Parallel()

	s := duplicateCreateTestServer()
	sbx := drainTestSandbox(t, "published-successor")
	owner, err := s.sandboxFactory.Sandboxes.Reserve(sbx.Runtime.SandboxID)
	require.NoError(t, err)
	t.Cleanup(owner.Release)
	require.NoError(t, owner.MarkRunning(t.Context(), sbx))

	resp, err := s.checkpointResumeFresh(t.Context(), sbx, &orchestrator.SandboxCheckpointRequest{
		SandboxId: sbx.Runtime.SandboxID,
	})
	require.Nil(t, resp)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "the API must preserve the healthy sandbox")
	live, ok := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
	require.True(t, ok)
	require.Same(t, sbx, live)
	require.Equal(t, sandbox.StopReasonCrashed, sbx.GetStopReason(), "no stop reason was assigned")
	require.Zero(t, s.info.OutstandingWork(), "refusal must not start asynchronous teardown")
	require.Len(t, s.sandboxFactory.Sandboxes.LifecycleItems(), 1)
	require.NoError(t, owner.MarkRunning(t.Context(), sbx), "the first operation keeps ownership")
}

type teardownSandboxStub struct {
	stop  func(context.Context) error
	wait  func(context.Context) error
	close func(context.Context) error
}

func (s teardownSandboxStub) Stop(ctx context.Context) error  { return s.stop(ctx) }
func (s teardownSandboxStub) Wait(ctx context.Context) error  { return s.wait(ctx) }
func (s teardownSandboxStub) Close(ctx context.Context) error { return s.close(ctx) }

func TestPublishedRollbackWaitsForMemoryExitBeforeClosing(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s := duplicateCreateTestServer()
		sbx := drainTestSandbox(t, "published")
		r, err := s.sandboxFactory.Sandboxes.Reserve(sbx.Runtime.SandboxID)
		require.NoError(t, err)
		require.NoError(t, r.MarkRunning(t.Context(), sbx))
		require.True(t, s.sandboxFactory.Sandboxes.MarkStopping(t.Context(), sbx.Runtime.SandboxID, sbx.LifecycleID))
		memoryExit, resourcesReleased := make(chan struct{}), make(chan struct{})
		releaseMemory := sync.OnceFunc(func() { close(memoryExit) })
		releaseResources := sync.OnceFunc(func() { close(resourcesReleased) })
		defer releaseMemory()
		defer releaseResources()
		var calls []string
		candidate := teardownSandboxStub{
			stop: func(ctx context.Context) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				calls = append(calls, "stop")

				return nil
			},
			wait: func(ctx context.Context) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				calls = append(calls, "wait")
				<-memoryExit

				return nil
			},
			close: func(ctx context.Context) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				calls = append(calls, "close")
				<-resourcesReleased
				s.sandboxFactory.Sandboxes.MarkStopped(ctx, sbx)

				return nil
			},
		}
		rollback := sandbox.NewCleanup()
		rollback.Add(t.Context(), func(ctx context.Context) error { return stopAndCloseSandbox(ctx, candidate) })
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		s.finishSandboxStart(ctx, r, rollback, errors.New("checkpoint upload failed"))
		drained := make(chan error, 1)
		go func() { drained <- s.DrainSandboxes(t.Context()) }()
		synctest.Wait()
		require.Equal(t, []string{"stop", "wait"}, calls)
		require.Empty(t, drained)
		require.Len(t, s.sandboxFactory.Sandboxes.LifecycleItems(), 1)
		_, err = s.sandboxFactory.Sandboxes.Reserve(sbx.Runtime.SandboxID)
		require.ErrorIs(t, err, sandbox.ErrSandboxAlreadyRunning)

		releaseMemory()
		synctest.Wait()
		require.Equal(t, []string{"stop", "wait", "close"}, calls)
		require.Empty(t, drained, "resource cleanup must complete after memory exit")
		require.Equal(t, int64(1), s.info.OutstandingWork())
		releaseResources()
		synctest.Wait()
		require.NoError(t, <-drained)
		require.Zero(t, s.info.OutstandingWork())
		r, err = s.sandboxFactory.Sandboxes.Reserve(sbx.Runtime.SandboxID)
		require.NoError(t, err)
		r.Release()
	})
}

func TestStopAndCloseSandboxAttemptsEveryStageOnErrors(t *testing.T) {
	t.Parallel()

	stopErr, waitErr, closeErr := errors.New("stop failed"), errors.New("wait failed"), errors.New("close failed")
	var calls []string
	candidate := teardownSandboxStub{
		stop: func(context.Context) error {
			calls = append(calls, "stop")

			return stopErr
		},
		wait: func(context.Context) error {
			calls = append(calls, "wait")

			return waitErr
		},
		close: func(context.Context) error {
			calls = append(calls, "close")

			return closeErr
		},
	}
	err := stopAndCloseSandbox(t.Context(), candidate)
	require.Equal(t, []string{"stop", "wait", "close"}, calls)
	require.ErrorIs(t, err, stopErr)
	require.ErrorIs(t, err, waitErr)
	require.ErrorIs(t, err, closeErr)
}

func duplicateCreateTestServer() *Server {
	meter := noop.NewMeterProvider().Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/server")
	s := &Server{
		info:                  &service.ServiceInfo{},
		sandboxFactory:        &sandbox.Factory{Sandboxes: sandbox.NewSandboxesMap()},
		startingSandboxes:     utils.Must(utils.NewAdjustableSemaphore(1)),
		sandboxCreateDuration: utils.Must(telemetry.GetHistogram(meter, telemetry.OrchestratorSandboxCreateDurationName)),
	}
	s.info.MaxSandboxes.Store(10)

	return s
}
