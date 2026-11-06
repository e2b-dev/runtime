//go:build linux

package server

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	internalevents "github.com/e2b-dev/infra/packages/orchestrator/pkg/events"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/shared/pkg/events"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

var (
	errResumeStub     = errors.New("resume stub finished")
	errCallerDeadline = errors.New("test caller gave up")
)

// The messages the detached checkpoint reports, spelled out so the tests
// read the same without the fix's sentinels.
const (
	snapshotBudgetMessage = "checkpoint snapshot exceeded its budget"
	resumeBudgetMessage   = "resume after checkpoint timed out"
	serverShutdownMessage = "server shutting down during checkpoint"
	deletedMessage        = "sandbox deleted during checkpoint"
)

// testWait bounds every wait, so a missing fix fails on an assertion instead
// of a synctest deadlock. Fake time makes it instant.
const testWait = time.Hour

// slowResumeSteps stands in for the VM: the snapshot and the resume block,
// as they do while a large memory snapshot is still being processed, until
// their release is closed, their delay passes, or their context ends.
type slowResumeSteps struct {
	snapshotStarted chan struct{}
	snapshotRelease chan struct{}
	resumeStarted   chan struct{}
	release         chan struct{}
	stopRelease     chan struct{}
	snapshotOnce    sync.Once
	resumeOnce      sync.Once
	stopOnce        sync.Once

	// blockSnapshot makes the snapshot wait for its release too.
	blockSnapshot bool
	// blockStop makes stopAndClose, the rollback's teardown, wait for its
	// release.
	blockStop bool
	// snapshotDelay and resumeDelay make a step take that long instead of
	// waiting for its release.
	snapshotDelay time.Duration
	resumeDelay   time.Duration
	// ignoreCancel makes the resume wait for release only, as a VM step that
	// does not observe its context.
	ignoreCancel bool
	// ownErrorOnCancel makes the resume fail for its own reason once its
	// context ends, as a step whose failure is unrelated to the cancellation.
	ownErrorOnCancel bool
	// resumed, when set, is what the resume returns once released.
	resumed *sandbox.Sandbox
	// panicInResume makes the resume panic, as a VM step with a bug would.
	panicInResume bool
	// onStopAsync runs inside the old sandbox's deferred stop, after the
	// steps returned and before the checkpoint's outcome is decided.
	onStopAsync func()

	snapshotCtxErr   error
	resumeCtxErr     error
	abandonedWith    error
	slotReleases     atomic.Int32
	stopAndCloses    atomic.Int32
	asyncStops       atomic.Int32
	templateReleases atomic.Int32
}

func newSlowResumeSteps() *slowResumeSteps {
	return &slowResumeSteps{
		snapshotStarted: make(chan struct{}),
		snapshotRelease: make(chan struct{}),
		resumeStarted:   make(chan struct{}),
		release:         make(chan struct{}),
		stopRelease:     make(chan struct{}),
	}
}

func (f *slowResumeSteps) releaseSnapshot() { f.snapshotOnce.Do(func() { close(f.snapshotRelease) }) }

func (f *slowResumeSteps) releaseResume() { f.resumeOnce.Do(func() { close(f.release) }) }

func (f *slowResumeSteps) releaseStop() { f.stopOnce.Do(func() { close(f.stopRelease) }) }

// releaseAll unblocks every step; deferred by each test so a failed
// assertion ends the checkpoint instead of leaving the bubble deadlocked.
func (f *slowResumeSteps) releaseAll() {
	f.releaseSnapshot()
	f.releaseResume()
	f.releaseStop()
}

func waitStep(ctx context.Context, release <-chan struct{}, delay time.Duration) {
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
		}

		return
	}
	select {
	case <-release:
	case <-ctx.Done():
	}
}

func (f *slowResumeSteps) snapshot(ctx context.Context, _ *sandbox.Sandbox, _ *orchestrator.SandboxCheckpointRequest) (*snapshotResult, error) {
	close(f.snapshotStarted)
	if f.blockSnapshot || f.snapshotDelay > 0 {
		waitStep(ctx, f.snapshotRelease, f.snapshotDelay)
	}
	f.snapshotCtxErr = ctx.Err()
	if f.snapshotCtxErr != nil {
		return nil, f.snapshotCtxErr
	}

	return &snapshotResult{
		abandonUpload: func(_ context.Context, err error) { f.abandonedWith = err },
	}, nil
}

func (f *slowResumeSteps) template(context.Context, *sandbox.Sandbox, string) (sbxtemplate.Template, func(), error) {
	return nil, func() { f.templateReleases.Add(1) }, nil
}

func (f *slowResumeSteps) resume(ctx context.Context, _ sbxtemplate.Template, _ *sandbox.Sandbox) (*sandbox.Sandbox, error) {
	close(f.resumeStarted)
	if f.panicInResume {
		panic("resume stub panicked")
	}
	if f.ignoreCancel {
		<-f.release
	} else {
		waitStep(ctx, f.release, f.resumeDelay)
	}
	f.resumeCtxErr = ctx.Err()
	if f.resumeCtxErr != nil && !f.ownErrorOnCancel && f.resumed == nil {
		return nil, f.resumeCtxErr
	}
	if f.resumed != nil {
		return f.resumed, nil
	}

	// Returning a live sandbox would need a real VM for the steps after the
	// resume; ending here is enough to show what decided the outcome.
	return nil, errResumeStub
}

func (f *slowResumeSteps) stopAndClose(context.Context, *sandbox.Sandbox) error {
	if f.blockStop {
		<-f.stopRelease
	}
	f.stopAndCloses.Add(1)

	return nil
}

func (f *slowResumeSteps) stopAsync(context.Context, *sandbox.Sandbox) {
	if f.onStopAsync != nil {
		f.onStopAsync()
	}
	f.asyncStops.Add(1)
}

// killRecorder collects the kill reasons of the killed events the server
// publishes.
type killRecorder struct {
	mu      sync.Mutex
	reasons []string
}

func (r *killRecorder) deliver(_ context.Context, _ string, ev events.SandboxEvent) error {
	if ev.Type != events.SandboxKilledEvent {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	reason, _ := ev.EventData["kill_reason"].(string)
	r.reasons = append(r.reasons, reason)

	return nil
}

func (r *killRecorder) killed() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.reasons...)
}

func slowResumeServer(t *testing.T) (*Server, *sandbox.Sandbox, *slowResumeSteps, *killRecorder) {
	t.Helper()

	s := duplicateCreateTestServer()
	kills := &killRecorder{}
	s.sbxEventsService = internalevents.NewEventsService([]events.Delivery[events.SandboxEvent]{sandboxEventDeliveryFunc(kills.deliver)})
	steps := newSlowResumeSteps()

	sbx := drainTestSandbox(t, "checkpointed")
	sbx.Runtime.TeamID = uuid.NewString()
	sbx.Runtime.ExecutionID = "execution-1"
	require.NoError(t, s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx))

	return s, sbx, steps, kills
}

func startSlowCheckpoint(ctx context.Context, s *Server, steps *slowResumeSteps, sbx *sandbox.Sandbox) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := s.checkpointResumeFreshWith(ctx, steps, sbx, &orchestrator.SandboxCheckpointRequest{
			SandboxId: sbx.Runtime.SandboxID,
			BuildId:   "checkpoint-build",
		}, func() { steps.slotReleases.Add(1) })
		done <- err
	}()

	return done
}

// checkpointOutcome is what a checkpoint run under recover ended with.
type checkpointOutcome struct {
	err      error
	panicked any
}

func startCheckpointRecovering(ctx context.Context, s *Server, steps *slowResumeSteps, sbx *sandbox.Sandbox) <-chan checkpointOutcome {
	done := make(chan checkpointOutcome, 1)
	go func() {
		var out checkpointOutcome
		defer func() {
			out.panicked = recover()
			done <- out
		}()
		_, out.err = s.checkpointResumeFreshWith(ctx, steps, sbx, &orchestrator.SandboxCheckpointRequest{
			SandboxId: sbx.Runtime.SandboxID,
			BuildId:   "checkpoint-build",
		}, func() { steps.slotReleases.Add(1) })
	}()

	return done
}

func awaitOutcome(t *testing.T, done <-chan checkpointOutcome) checkpointOutcome {
	t.Helper()

	select {
	case out := <-done:
		return out
	case <-time.After(testWait):
		t.Fatal("the checkpoint did not return")

		return checkpointOutcome{}
	}
}

func awaitCheckpoint(t *testing.T, done <-chan error) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(testWait):
		t.Fatal("the checkpoint did not return")

		return nil
	}
}

func awaitStep(t *testing.T, started <-chan struct{}, step string) {
	t.Helper()

	select {
	case <-started:
	case <-time.After(testWait):
		t.Fatalf("the %s step did not start", step)
	}
}

func deleteRequest(sbx *sandbox.Sandbox, reason string) *orchestrator.SandboxDeleteRequest {
	req := &orchestrator.SandboxDeleteRequest{SandboxId: sbx.Runtime.SandboxID}
	if reason != "" {
		req.KillReason = &reason
	}

	return req
}

// Once the sandbox is reserved for the checkpoint it is gone on any error,
// so the snapshot is already past the point where the caller's deadline may
// decide. A caller giving up during a long snapshot must not cancel it, or
// the API restores its record to Running over a dead VM.
func TestCheckpointResumeFresh_CallerCancelDoesNotCancelTheSnapshot(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, _ := slowResumeServer(t)
		defer steps.releaseAll()
		steps.blockSnapshot = true
		steps.releaseResume()

		callerCtx, cancelCaller := context.WithCancel(t.Context())
		done := startSlowCheckpoint(callerCtx, s, steps, sbx)

		awaitStep(t, steps.snapshotStarted, "snapshot")
		cancelCaller()
		synctest.Wait()

		select {
		case err := <-done:
			t.Fatalf("the caller's cancellation ended the snapshot: %v", err)
		default:
		}

		steps.releaseSnapshot()
		err := awaitCheckpoint(t, done)
		synctest.Wait()

		require.NoError(t, steps.snapshotCtxErr, "the snapshot ran on a context the caller could cancel")
		require.NoError(t, steps.resumeCtxErr)
		assert.Equal(t, codes.Internal, status.Code(err))
		require.ErrorContains(t, err, errResumeStub.Error(), "the resume's own result decides the checkpoint")
		assert.Zero(t, s.info.OutstandingWork())
	})
}

// Once the VM is paused and snapshotted, the old sandbox is gone either way:
// the only way the caller keeps its sandbox is the resume. A caller that
// gives up while the resume is still waiting on the memory snapshot (the API
// request deadline passing during a large checkpoint) must not cancel it, or
// a good snapshot and a healthy sandbox are both thrown away.
func TestCheckpointResumeFresh_CallerCancelDoesNotCancelTheResume(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, _ := slowResumeServer(t)
		defer steps.releaseAll()

		callerCtx, cancelCaller := context.WithCancel(t.Context())
		done := startSlowCheckpoint(callerCtx, s, steps, sbx)

		awaitStep(t, steps.resumeStarted, "resume")
		cancelCaller()
		synctest.Wait()

		select {
		case err := <-done:
			t.Fatalf("the caller's cancellation ended the resume: %v", err)
		default:
		}

		steps.releaseResume()
		err := awaitCheckpoint(t, done)
		synctest.Wait()

		require.NoError(t, steps.resumeCtxErr, "the resume ran on a context the caller could cancel")
		assert.Equal(t, codes.Internal, status.Code(err))
		require.ErrorContains(t, err, errResumeStub.Error(), "the resume's own result decides the checkpoint")
		require.ErrorContains(t, steps.abandonedWith, errResumeStub.Error())
		assert.Equal(t, int32(1), steps.asyncStops.Load(), "the old sandbox is stopped once")
		assert.Zero(t, s.info.OutstandingWork())
	})
}

// A resume that outlives its own budget fails, and says it was that budget,
// so it cannot be mistaken for the caller's deadline. The caller's deadline
// is later than the budget so that, without the budget, the test ends on
// the caller's cause and fails here rather than hanging.
func TestCheckpointResumeFresh_ResumeBudgetNamesItsCause(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, _ := slowResumeServer(t)
		defer steps.releaseAll()

		callerCtx, cancelCaller := context.WithTimeoutCause(t.Context(), testWait, errCallerDeadline)
		defer cancelCaller()

		err := awaitCheckpoint(t, startSlowCheckpoint(callerCtx, s, steps, sbx))
		synctest.Wait()

		require.ErrorIs(t, steps.resumeCtxErr, context.DeadlineExceeded)
		assert.Equal(t, codes.Internal, status.Code(err))
		require.ErrorContains(t, err, resumeBudgetMessage)
		assert.NotContains(t, err.Error(), errCallerDeadline.Error())
		assert.Equal(t, int32(1), steps.stopAndCloses.Load(), "the old sandbox is torn down")
	})
}

// The snapshot phase has a budget of its own, named when it runs out.
func TestCheckpointResumeFresh_SnapshotBudgetNamesItsCause(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, _ := slowResumeServer(t)
		defer steps.releaseAll()
		steps.blockSnapshot = true

		err := awaitCheckpoint(t, startSlowCheckpoint(t.Context(), s, steps, sbx))
		synctest.Wait()

		require.ErrorIs(t, steps.snapshotCtxErr, context.DeadlineExceeded)
		assert.Equal(t, codes.Internal, status.Code(err))
		require.ErrorContains(t, err, snapshotBudgetMessage)
		assert.NotContains(t, err.Error(), resumeBudgetMessage)
		assert.Equal(t, int32(1), steps.stopAndCloses.Load(), "the old sandbox is torn down")
	})
}

// A long snapshot does not eat into the resume's budget: six minutes of
// snapshot followed by five of resume, each within its own ten, succeed.
func TestCheckpointResumeFresh_PhasesHaveSeparateBudgets(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, _ := slowResumeServer(t)
		defer steps.releaseAll()
		steps.snapshotDelay = 6 * time.Minute
		steps.resumeDelay = 5 * time.Minute

		err := awaitCheckpoint(t, startSlowCheckpoint(t.Context(), s, steps, sbx))
		synctest.Wait()

		require.NoError(t, steps.snapshotCtxErr)
		require.NoError(t, steps.resumeCtxErr, "the snapshot's time was charged to the resume")
		require.ErrorContains(t, err, errResumeStub.Error())
	})
}

// A step that fails for its own reason after the budget expired reports
// that reason, not the budget.
func TestCheckpointResumeFresh_OwnFailureAfterBudgetIsNotTheBudget(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, _ := slowResumeServer(t)
		defer steps.releaseAll()
		steps.ownErrorOnCancel = true

		err := awaitCheckpoint(t, startSlowCheckpoint(t.Context(), s, steps, sbx))
		synctest.Wait()

		require.ErrorIs(t, steps.resumeCtxErr, context.DeadlineExceeded, "the budget did expire")
		assert.Equal(t, codes.Internal, status.Code(err))
		require.ErrorContains(t, err, errResumeStub.Error())
		assert.NotContains(t, err.Error(), resumeBudgetMessage)
	})
}

// A resume that fails on its own, with the caller still waiting, still rolls
// the checkpoint back: the old sandbox is torn down, the template pin and the
// upload are released, and the API is told the sandbox is gone.
func TestCheckpointResumeFresh_FailedResumeRollsBack(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, _ := slowResumeServer(t)
		defer steps.releaseAll()
		steps.releaseResume()

		err := awaitCheckpoint(t, startSlowCheckpoint(t.Context(), s, steps, sbx))
		synctest.Wait()

		assert.Equal(t, codes.Internal, status.Code(err))
		require.ErrorContains(t, err, errResumeStub.Error())
		require.ErrorContains(t, steps.abandonedWith, errResumeStub.Error())
		assert.Equal(t, int32(1), steps.stopAndCloses.Load(), "the old sandbox is torn down")
		assert.Equal(t, int32(1), steps.asyncStops.Load())
		assert.Equal(t, int32(1), steps.templateReleases.Load(), "the template pin is returned")
		assert.Zero(t, steps.slotReleases.Load(), "no VM came up, the caller returns the slot")
		assert.Zero(t, s.info.OutstandingWork())

		_, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
		assert.False(t, live)
		r, err := s.sandboxFactory.Sandboxes.Reserve(sbx.Runtime.SandboxID)
		require.NoError(t, err, "the rollback releases the sandbox ID")
		r.Release()
	})
}

// The VM dies with the process, so a server shutting down ends the detached
// checkpoint instead of holding the gRPC server open for the whole budget.
func TestCheckpointResumeFresh_ServerShutdownEndsTheCheckpoint(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, _ := slowResumeServer(t)
		defer steps.releaseAll()
		s.shutdownCtx, s.shutdown = context.WithCancel(t.Context())

		done := startSlowCheckpoint(t.Context(), s, steps, sbx)
		awaitStep(t, steps.resumeStarted, "resume")

		s.BeginShutdown()
		err := awaitCheckpoint(t, done)
		synctest.Wait()

		require.ErrorIs(t, steps.resumeCtxErr, context.Canceled)
		assert.Equal(t, codes.Internal, status.Code(err))
		require.ErrorContains(t, err, serverShutdownMessage)
		assert.NotContains(t, err.Error(), resumeBudgetMessage)
		assert.Equal(t, int32(1), steps.stopAndCloses.Load(), "the old sandbox is torn down")
	})
}

// While the sandbox is reserved out of the live map a Delete must not be
// answered NotFound and forgotten: the API removes its record on NotFound
// and the resumed VM would go live minutes later, untracked. Delete ends the
// checkpoint, waits until its rollback has run and the ID is free, publishes
// the kill, and answers success.
func TestCheckpointResumeFresh_DeleteEndsTheCheckpointAndWaits(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, kills := slowResumeServer(t)
		defer steps.releaseAll()
		steps.blockStop = true

		done := startSlowCheckpoint(t.Context(), s, steps, sbx)
		awaitStep(t, steps.resumeStarted, "resume")

		delDone := make(chan error, 1)
		go func() {
			_, err := s.Delete(t.Context(), deleteRequest(sbx, "test-kill"))
			delDone <- err
		}()
		synctest.Wait()

		select {
		case err := <-done:
			require.ErrorIs(t, steps.resumeCtxErr, context.Canceled)
			require.ErrorContains(t, err, deletedMessage)
		default:
			t.Fatal("Delete did not end the checkpoint")
		}
		select {
		case err := <-delDone:
			t.Fatalf("Delete answered before the rollback ran: %v", err)
		default:
		}
		_, err := s.sandboxFactory.Sandboxes.Reserve(sbx.Runtime.SandboxID)
		require.Error(t, err, "the ID is still held while the rollback runs")

		steps.releaseStop()
		require.NoError(t, awaitCheckpoint(t, delDone), "the checkpoint's rollback is the delete")
		assert.Equal(t, int32(1), steps.stopAndCloses.Load(), "the rollback ran before Delete answered")
		r, err := s.sandboxFactory.Sandboxes.Reserve(sbx.Runtime.SandboxID)
		require.NoError(t, err, "the ID is free when Delete answers")
		r.Release()
		synctest.Wait()

		_, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
		assert.False(t, live, "nothing goes live after the delete")
		assert.Nil(t, s.detachedCheckpointFor(sbx.Runtime.SandboxID))
		assert.Equal(t, []string{"test-kill"}, kills.killed(), "the checkpoint publishes the kill with the request's reason")
		assert.Equal(t, sandbox.StopReasonKilled, sbx.GetStopReason(), "the execution ended by the delete, not the checkpoint")
		assert.Zero(t, s.info.OutstandingWork())

		_, delErr := s.Delete(t.Context(), deleteRequest(sbx, ""))
		assert.Equal(t, codes.NotFound, status.Code(delErr))
	})
}

// A VM step that does not observe its cancellation keeps the checkpoint in
// flight; Delete's wait for it is bounded by the request, and the Delete
// is still success: the kill is delivered and the checkpoint publishes it
// as it ends. The API takes only NotFound besides success, so an error here
// would fail the user's kill of a sandbox that is being torn down.
func TestCheckpointResumeFresh_DeleteWaitIsBoundedAndSucceeds(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, kills := slowResumeServer(t)
		defer steps.releaseAll()
		steps.ignoreCancel = true
		steps.ownErrorOnCancel = true

		done := startSlowCheckpoint(t.Context(), s, steps, sbx)
		awaitStep(t, steps.resumeStarted, "resume")

		deleteStart := time.Now()
		_, delErr := s.Delete(t.Context(), deleteRequest(sbx, "test-kill"))
		require.NoError(t, delErr, "the kill is delivered; the Delete does not fail on its own deadline")
		assert.Equal(t, requestTimeout, time.Since(deleteStart), "the Delete waited out its request budget")
		assert.NotNil(t, s.detachedCheckpointFor(sbx.Runtime.SandboxID), "the checkpoint is still in flight when the Delete answers")
		assert.Empty(t, kills.killed(), "no kill is published for a sandbox not yet torn down")

		steps.releaseResume()
		err := awaitCheckpoint(t, done)
		synctest.Wait()

		require.ErrorIs(t, steps.resumeCtxErr, context.Canceled, "the delete did cancel the checkpoint")
		require.ErrorContains(t, err, errResumeStub.Error())
		assert.NotContains(t, err.Error(), deletedMessage, "the step failed for its own reason")
		_, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
		assert.False(t, live)
		assert.Equal(t, []string{"test-kill"}, kills.killed(), "the kill the delete could not wait for is still published")
		assert.Equal(t, sandbox.StopReasonKilled, sbx.GetStopReason())
	})
}

// A resume that ignores its cancellation and then hands back a sandbox must
// not register it live after Delete answered: the checkpoint notices the
// cancellation between steps and tears the resumed sandbox down as killed.
func TestCheckpointResumeFresh_UnobservedCancelDoesNotGoLive(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, kills := slowResumeServer(t)
		defer steps.releaseAll()
		steps.ignoreCancel = true
		steps.resumed = drainTestSandbox(t, "resumed")

		done := startSlowCheckpoint(t.Context(), s, steps, sbx)
		awaitStep(t, steps.resumeStarted, "resume")

		delDone := make(chan error, 1)
		go func() {
			_, err := s.Delete(t.Context(), deleteRequest(sbx, "test-kill"))
			delDone <- err
		}()
		synctest.Wait()
		steps.releaseResume()

		err := awaitCheckpoint(t, done)
		require.NoError(t, awaitCheckpoint(t, delDone))
		synctest.Wait()

		require.ErrorIs(t, steps.resumeCtxErr, context.Canceled)
		assert.Equal(t, codes.Internal, status.Code(err))
		require.ErrorContains(t, err, deletedMessage)
		_, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
		assert.False(t, live, "the resumed sandbox went live after the delete")
		assert.Equal(t, sandbox.StopReasonKilled, steps.resumed.GetStopReason())
		assert.Equal(t, int32(2), steps.stopAndCloses.Load(), "both the old and the resumed sandbox are torn down")
		assert.Equal(t, []string{"test-kill"}, kills.killed())
	})
}

// A second Checkpoint during the detached window is refused as an operation
// in progress, as it is once the resumed sandbox is live, never NotFound.
func TestCheckpointResumeFresh_CheckpointDuringTheWindowIsInProgress(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, _ := slowResumeServer(t)
		defer steps.releaseAll()

		done := startSlowCheckpoint(t.Context(), s, steps, sbx)
		awaitStep(t, steps.resumeStarted, "resume")

		req := &orchestrator.SandboxCheckpointRequest{SandboxId: sbx.Runtime.SandboxID, BuildId: "second-build"}
		_, err := s.Checkpoint(t.Context(), req)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.ErrorContains(t, err, "already in progress")
		assert.NotNil(t, checkpointInProgressDetail(t, err), "the refusal carries its typed detail for the API")

		steps.releaseResume()
		awaitCheckpoint(t, done)
		synctest.Wait()

		_, err = s.Checkpoint(t.Context(), req)
		assert.Equal(t, codes.NotFound, status.Code(err), "the sandbox is gone once the checkpoint returned")
	})
}

// A read that missed the live map is answered retryably while a checkpoint
// holds the sandbox and once its claim has ended with the resumed sandbox
// live, the state a caller sees when the claim ended between its read and
// the registry lookup; only a sandbox that is gone reads as such.
func TestHeldByCheckpoint(t *testing.T) {
	t.Parallel()

	s, sbx, steps, _ := slowResumeServer(t)
	defer steps.releaseAll()
	id := sbx.Runtime.SandboxID

	assert.True(t, s.heldByCheckpoint(id), "live with no claim: the claim ended after the resumed sandbox went live")

	_, op, ok := s.registerDetachedCheckpoint(t.Context(), sbx)
	require.True(t, ok)
	reservation, err := op.reserve(t.Context(), s.sandboxFactory.Sandboxes, sbx)
	require.NoError(t, err)
	assert.True(t, s.heldByCheckpoint(id), "out of the live map under the claim")

	op.release()
	assert.False(t, s.heldByCheckpoint(id), "claim ended and nothing went live")
	reservation.Release()
	assert.False(t, s.heldByCheckpoint(id))
}

// Pause during the detached window is refused retryably, never NotFound:
// the API's pause path restores the record and retries on
// ResourceExhausted only, and on any other failure removes the record, so
// the sandbox the checkpoint resumes would be killed as an orphan.
func TestCheckpointResumeFresh_PauseDuringTheWindowIsRetryable(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, _ := slowResumeServer(t)
		defer steps.releaseAll()

		done := startSlowCheckpoint(t.Context(), s, steps, sbx)
		awaitStep(t, steps.resumeStarted, "resume")

		_, err := s.Pause(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: sbx.Runtime.SandboxID})
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
		require.ErrorContains(t, err, "please retry")
		assert.NotNil(t, checkpointInProgressDetail(t, err), "the API restores on this detail; without it the flag-off pause kills")

		steps.releaseResume()
		awaitCheckpoint(t, done)
		synctest.Wait()

		_, err = s.Pause(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: sbx.Runtime.SandboxID})
		assert.Equal(t, codes.NotFound, status.Code(err), "the sandbox is gone once the checkpoint returned")
	})
}

// A filesystem-only checkpoint runs in place and leaves the sandbox live,
// so a memory checkpoint routed resume-fresh finds it in the live map
// meanwhile, and the other way round between the claim and the reservation.
// The two routes share the sandbox's checkpoint marker: the resume-fresh
// claim is refused while an in-place checkpoint holds it, and holds it
// itself until the claim ends.
func TestCheckpointResumeFresh_ExcludesAnInPlaceCheckpoint(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, _ := slowResumeServer(t)
		defer steps.releaseAll()

		require.True(t, sbx.BeginCheckpoint(), "an in-place checkpoint is in flight")
		err := awaitCheckpoint(t, startSlowCheckpoint(t.Context(), s, steps, sbx))
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.ErrorContains(t, err, "already in progress")
		_, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
		assert.True(t, live, "the sandbox stays live and untouched")
		assert.Nil(t, s.detachedCheckpointFor(sbx.Runtime.SandboxID), "no claim is left behind")
		select {
		case <-steps.snapshotStarted:
			t.Fatal("the snapshot ran on the VM the in-place checkpoint holds")
		default:
		}
		sbx.EndCheckpoint()

		done := startSlowCheckpoint(t.Context(), s, steps, sbx)
		awaitStep(t, steps.resumeStarted, "resume")
		assert.False(t, sbx.BeginCheckpoint(), "the resume-fresh checkpoint holds the marker while in flight")

		steps.releaseResume()
		awaitCheckpoint(t, done)
		synctest.Wait()

		assert.True(t, sbx.BeginCheckpoint(), "the marker ends with the claim")
		sbx.EndCheckpoint()
	})
}

// stoppingProbe runs probe from inside the sandbox map's stopping
// notification, which fires after the live entry is removed and before the
// checkpoint runs on: the narrowest window between live and in flight.
type stoppingProbe struct {
	probe func()
}

func (stoppingProbe) OnInsert(context.Context, *sandbox.Sandbox) {}

func (p stoppingProbe) OnStopping(context.Context, *sandbox.Sandbox) { p.probe() }

func (stoppingProbe) OnNetworkRelease(context.Context, *sandbox.Sandbox) error { return nil }

// The in-flight claim is taken before the reservation removes the sandbox
// from the live map, so there is no interleaving in which a Checkpoint
// finds the sandbox neither live nor in flight.
func TestCheckpointResumeFresh_ClaimPrecedesTheReservation(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, _ := slowResumeServer(t)
		defer steps.releaseAll()
		steps.releaseResume()

		var probed atomic.Int32
		var code codes.Code
		s.sandboxFactory.Sandboxes.Subscribe(stoppingProbe{probe: func() {
			if probed.Add(1) != 1 {
				return
			}
			_, err := s.Checkpoint(t.Context(), &orchestrator.SandboxCheckpointRequest{SandboxId: sbx.Runtime.SandboxID, BuildId: "second-build"})
			code = status.Code(err)
		}})

		awaitCheckpoint(t, startSlowCheckpoint(t.Context(), s, steps, sbx))
		synctest.Wait()

		require.Equal(t, int32(1), probed.Load())
		assert.Equal(t, codes.FailedPrecondition, code, "a Checkpoint between the reservation and the claim")
	})
}

// A panic inside the reservation (an OnStopping subscriber) must not leak
// the claim, the reservation or the sandbox: the map puts the sandbox back
// live and drops the hold, the claim is released, and the next checkpoint
// reserves it as if nothing had happened.
func TestCheckpointResumeFresh_PanicInReservationReleasesTheClaim(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, _ := slowResumeServer(t)
		defer steps.releaseAll()

		var probed atomic.Int32
		s.sandboxFactory.Sandboxes.Subscribe(stoppingProbe{probe: func() {
			if probed.Add(1) == 1 {
				panic("subscriber panicked")
			}
		}})

		out := awaitOutcome(t, startCheckpointRecovering(t.Context(), s, steps, sbx))
		synctest.Wait()

		require.NotNil(t, out.panicked, "the panic is raised again")
		assert.Nil(t, s.detachedCheckpointFor(sbx.Runtime.SandboxID), "the claim is released")
		live, ok := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
		require.True(t, ok, "the sandbox is live again")
		assert.Same(t, sbx, live)
		assert.Zero(t, steps.stopAndCloses.Load(), "a live sandbox is not torn down")
		assert.Zero(t, steps.asyncStops.Load())
		assert.Equal(t, sandbox.StopReasonCrashed, sbx.GetStopReason(), "no stop reason was recorded")

		steps.releaseAll()
		err := awaitCheckpoint(t, startSlowCheckpoint(t.Context(), s, steps, sbx))
		synctest.Wait()
		require.ErrorContains(t, err, errResumeStub.Error(), "the next checkpoint takes the reservation")
		assert.Equal(t, int32(2), probed.Load())
	})
}

// A panicking step is rolled back like a failing one: the old sandbox is
// torn down, the pin and the upload are released, the claim and the
// reservation are freed, and the panic is raised again.
func TestCheckpointResumeFresh_PanickingStepRollsBack(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, _ := slowResumeServer(t)
		defer steps.releaseAll()
		steps.panicInResume = true

		out := awaitOutcome(t, startCheckpointRecovering(t.Context(), s, steps, sbx))
		synctest.Wait()

		require.NotNil(t, out.panicked, "the panic is raised again")
		assert.Equal(t, int32(1), steps.stopAndCloses.Load(), "the old sandbox is torn down")
		assert.Equal(t, int32(1), steps.asyncStops.Load())
		assert.Equal(t, int32(1), steps.templateReleases.Load(), "the template pin is returned")
		require.ErrorContains(t, steps.abandonedWith, "panicked")
		assert.Nil(t, s.detachedCheckpointFor(sbx.Runtime.SandboxID))
		r, err := s.sandboxFactory.Sandboxes.Reserve(sbx.Runtime.SandboxID)
		require.NoError(t, err, "the reservation is released")
		r.Release()
		assert.Zero(t, s.info.OutstandingWork())
	})
}

// A checkpoint that failed on its own is not this Delete's kill: the Delete
// waits out its rollback, publishes nothing and answers NotFound, as it
// would have found nothing in that window before.
func TestCheckpointResumeFresh_DeleteAfterOwnFailurePublishesNoKill(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, kills := slowResumeServer(t)
		defer steps.releaseAll()
		steps.blockStop = true
		steps.releaseResume()

		done := startSlowCheckpoint(t.Context(), s, steps, sbx)
		require.ErrorContains(t, awaitCheckpoint(t, done), errResumeStub.Error())

		delDone := make(chan error, 1)
		go func() {
			_, err := s.Delete(t.Context(), deleteRequest(sbx, "test-kill"))
			delDone <- err
		}()
		synctest.Wait()
		steps.releaseStop()
		assert.Equal(t, codes.NotFound, status.Code(awaitCheckpoint(t, delDone)), "the delete did not end the checkpoint")
		synctest.Wait()

		assert.Empty(t, kills.killed(), "the delete did not end the checkpoint")
		assert.Equal(t, sandbox.StopReasonCheckpointing, sbx.GetStopReason())
	})
}

// A resumed sandbox whose upload fails is live, so the API's follow-up
// Delete finds nothing to kill: the checkpoint publishes the kill itself,
// unless a Delete already took the sandbox out and published it.
func TestKillResumedOnUploadFailure(t *testing.T) {
	t.Parallel()

	t.Run("live sandbox", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			s, sbx, _, kills := slowResumeServer(t)

			s.killResumedOnUploadFailure(t.Context(), sbx)
			synctest.Wait()

			_, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
			assert.False(t, live)
			assert.Equal(t, sandbox.StopReasonKilled, sbx.GetStopReason())
			assert.Equal(t, []string{"upload_failed"}, kills.killed())
		})
	})

	t.Run("sandbox a delete took out first", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			s, sbx, _, kills := slowResumeServer(t)
			require.True(t, s.sandboxFactory.Sandboxes.MarkStopping(t.Context(), sbx.Runtime.SandboxID, sbx.LifecycleID))

			s.killResumedOnUploadFailure(t.Context(), sbx)
			synctest.Wait()

			assert.Empty(t, kills.killed(), "the delete published the kill")
		})
	})
}

// The reservation runs on the detached context: a caller gone at that
// instant must not abort the stopping notification's work.
func TestCheckpointResumeFresh_ReservationRunsDetached(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, _ := slowResumeServer(t)
		defer steps.releaseAll()
		steps.releaseResume()

		var seen error
		s.sandboxFactory.Sandboxes.Subscribe(stoppingCtxProbe{probe: func(ctx context.Context) { seen = ctx.Err() }})

		callerCtx, cancelCaller := context.WithCancel(t.Context())
		cancelCaller()
		awaitCheckpoint(t, startSlowCheckpoint(callerCtx, s, steps, sbx))
		synctest.Wait()

		require.NoError(t, seen, "the stopping notification ran on the caller's dead context")
	})
}

// The starting-sandboxes slot is held until the resumed sandbox is live,
// as Create defines a start: a resume that hands back a VM but never
// reaches registration does not return it; the handler does.
func TestCheckpointResumeFresh_SlotHeldThroughRegistration(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, _ := slowResumeServer(t)
		defer steps.releaseAll()
		steps.ignoreCancel = true
		steps.resumed = drainTestSandbox(t, "resumed")

		done := startSlowCheckpoint(t.Context(), s, steps, sbx)
		awaitStep(t, steps.resumeStarted, "resume")

		delDone := make(chan error, 1)
		go func() {
			_, err := s.Delete(t.Context(), deleteRequest(sbx, "test-kill"))
			delDone <- err
		}()
		synctest.Wait()
		steps.releaseResume()

		require.NoError(t, awaitCheckpoint(t, delDone))
		awaitCheckpoint(t, done)
		synctest.Wait()
		assert.Zero(t, steps.slotReleases.Load(), "the VM came up but never went live; the handler returns the slot")
	})
}

// A Delete whose cancellation lands after the checkpoint's last check, as
// the resumed sandbox goes live, finds that sandbox once the claim ends and
// kills it like any other instead of answering over a live, unkilled VM.
func TestCheckpointResumeFresh_DeleteFindsTheSandboxThatWentLive(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, _, _ := slowResumeServer(t)

		ctx, op, ok := s.registerDetachedCheckpoint(t.Context(), sbx)
		require.True(t, ok)
		defer op.cancel(nil)
		r, err := op.reserve(ctx, s.sandboxFactory.Sandboxes, sbx)
		require.NoError(t, err)

		type ending struct {
			live  *sandbox.Sandbox
			ended bool
		}
		endDone := make(chan ending, 1)
		go func() {
			live, ended := s.endCheckpointForDelete(t.Context(), sbx.Runtime.SandboxID, "test-kill")
			endDone <- ending{live, ended}
		}()
		synctest.Wait()
		require.ErrorIs(t, context.Cause(ctx), errCheckpointDeleted, "the delete reached the checkpoint")

		resumed := drainTestSandbox(t, "resumed")
		require.NoError(t, r.MarkRunning(ctx, resumed), "the checkpoint registers the resumed sandbox live")
		op.setRegistered(resumed.LifecycleID)
		op.release()

		select {
		case got := <-endDone:
			assert.True(t, got.ended)
			assert.Same(t, resumed, got.live, "Delete is handed the sandbox that went live")
		case <-time.After(testWait):
			t.Fatal("Delete kept waiting after the claim ended")
		}
		r.Release()
	})
}

// insertProbe is a map subscriber that holds OnInsert until released, as a
// subscriber that blocks on its own dependency would.
type insertProbe struct {
	release <-chan struct{}
	started chan struct{}
}

func (p insertProbe) OnInsert(context.Context, *sandbox.Sandbox) {
	close(p.started)
	<-p.release
}

func (insertProbe) OnStopping(context.Context, *sandbox.Sandbox) {}

func (insertProbe) OnNetworkRelease(context.Context, *sandbox.Sandbox) error { return nil }

// A registration the Delete's cancellation refused ends the resumed sandbox
// for that Delete: it reads as killed, as on the exits before the
// registration, so the execution-duration and crash accounting records the
// kill and not a failed registration. A refusal for the budget keeps reading
// as a failed registration.
func TestCheckpointResumeFresh_RegistrationRefusedByTheDeleteMarksTheSandboxKilled(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, _, _ := slowResumeServer(t)

		ctx, op, ok := s.registerDetachedCheckpoint(t.Context(), sbx)
		require.True(t, ok)
		defer op.cancel(nil)
		r, err := op.reserve(ctx, s.sandboxFactory.Sandboxes, sbx)
		require.NoError(t, err)
		defer r.Release()

		type ending struct {
			live  *sandbox.Sandbox
			ended bool
		}
		endDone := make(chan ending, 1)
		go func() {
			live, ended := s.endCheckpointForDelete(t.Context(), sbx.Runtime.SandboxID, "test-kill")
			endDone <- ending{live, ended}
		}()
		synctest.Wait()
		require.ErrorIs(t, context.Cause(ctx), errCheckpointDeleted, "the delete reached the checkpoint")

		// The cancellation landed after the checkpoint's last check and
		// before the registration, which refuses it.
		resumed := drainTestSandbox(t, "resumed")
		op.setRegistered(resumed.LifecycleID)
		err = s.markSandboxLive(ctx, resumed, r, true)
		require.ErrorContains(t, err, deletedMessage)
		_, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
		assert.False(t, live, "nothing went live")
		assert.Equal(t, sandbox.StopReasonKilled, resumed.GetStopReason(), "the refused sandbox ended for the delete")

		op.release()
		select {
		case got := <-endDone:
			assert.True(t, got.ended)
			assert.Nil(t, got.live)
		case <-time.After(testWait):
			t.Fatal("Delete kept waiting after the claim ended")
		}
	})

	t.Run("refused for the budget", func(t *testing.T) {
		t.Parallel()

		s, sbx, _, _ := slowResumeServer(t)
		r, err := s.sandboxFactory.Sandboxes.MarkStoppingReserved(t.Context(), sbx.Runtime.SandboxID, sbx.LifecycleID)
		require.NoError(t, err)
		defer r.Release()
		expired, cancel := context.WithTimeoutCause(t.Context(), -time.Second, errResumeAfterCheckpointTimeout)
		defer cancel()

		resumed := drainTestSandbox(t, "resumed")
		require.ErrorContains(t, s.markSandboxLive(expired, resumed, r, true), resumeBudgetMessage)
		assert.Equal(t, sandbox.StopReasonRegistrationFailed, resumed.GetStopReason(), "a budget is not a kill")
	})
}

// A Delete whose own deadline expires while the resumed sandbox's
// registration is still notifying subscribers is handed that sandbox: it is
// live, and the claim marks it as the checkpoint's before the registration,
// so the Delete kills it instead of answering over a live VM the API then
// stops tracking.
func TestCheckpointResumeFresh_DeleteThatTimedOutDuringRegistrationIsHandedTheSandbox(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, _, _ := slowResumeServer(t)

		ctx, op, ok := s.registerDetachedCheckpoint(t.Context(), sbx)
		require.True(t, ok)
		defer op.cancel(nil)
		r, err := op.reserve(ctx, s.sandboxFactory.Sandboxes, sbx)
		require.NoError(t, err)
		defer r.Release()

		release := make(chan struct{})
		defer close(release)
		probe := insertProbe{release: release, started: make(chan struct{})}
		s.sandboxFactory.Sandboxes.Subscribe(probe)

		type ending struct {
			live  *sandbox.Sandbox
			ended bool
		}
		endDone := make(chan ending, 1)
		deleteCtx, cancelDelete := context.WithTimeout(t.Context(), requestTimeout)
		defer cancelDelete()
		go func() {
			live, ended := s.endCheckpointForDelete(deleteCtx, sbx.Runtime.SandboxID, "test-kill")
			endDone <- ending{live, ended}
		}()
		synctest.Wait()
		require.ErrorIs(t, context.Cause(ctx), errCheckpointDeleted, "the delete reached the checkpoint")

		// The cancellation landed after the checkpoint's last check: the
		// registration goes ahead and blocks in a subscriber.
		resumed := drainTestSandbox(t, "resumed")
		op.setRegistered(resumed.LifecycleID)
		go func() { _ = r.MarkRunning(ctx, resumed) }()
		awaitStep(t, probe.started, "insert notification")

		time.Sleep(requestTimeout)
		select {
		case got := <-endDone:
			assert.True(t, got.ended)
			assert.Same(t, resumed, got.live, "the Delete is handed the sandbox that went live under its nose")
		case <-time.After(testWait):
			t.Fatal("the Delete did not answer at its deadline")
		}
	})
}

// A Delete whose deadline expires with nothing of the checkpoint's live,
// while a publication the cancellation did not reach is about to insert the
// resumed sandbox, is honoured by that publication: the checkpoint kills
// the sandbox it made live, with the Delete's reason, once.
func TestCheckpointResumeFresh_PublicationAfterAnAcknowledgedDeleteIsKilled(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, kills := slowResumeServer(t)

		ctx, op, ok := s.registerDetachedCheckpoint(t.Context(), sbx)
		require.True(t, ok)
		defer op.cancel(nil)
		r, err := op.reserve(ctx, s.sandboxFactory.Sandboxes, sbx)
		require.NoError(t, err)
		defer r.Release()

		type ending struct {
			live  *sandbox.Sandbox
			ended bool
		}
		endDone := make(chan ending, 1)
		deleteCtx, cancelDelete := context.WithTimeout(t.Context(), requestTimeout)
		defer cancelDelete()
		go func() {
			live, ended := s.endCheckpointForDelete(deleteCtx, sbx.Runtime.SandboxID, "test-kill")
			endDone <- ending{live, ended}
		}()
		synctest.Wait()
		time.Sleep(requestTimeout)
		select {
		case got := <-endDone:
			require.True(t, got.ended)
			require.Nil(t, got.live, "nothing of the checkpoint's was live when the Delete answered")
		case <-time.After(testWait):
			t.Fatal("the Delete did not answer at its deadline")
		}

		// The publication read no cancellation before the Delete's and goes
		// ahead, as the handler does, then honours the acknowledgement.
		resumed := drainTestSandbox(t, "resumed")
		resumed.Runtime.TeamID = sbx.Runtime.TeamID
		op.setRegistered(resumed.LifecycleID)
		require.NoError(t, r.MarkRunning(ctx, resumed))
		s.killForAcknowledgedDelete(t.Context(), op, steps, sbx.Runtime.SandboxID)
		synctest.Wait()

		_, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
		assert.False(t, live, "the published sandbox is marked stopping")
		assert.Equal(t, []string{"test-kill"}, kills.killed(), "one kill, with the Delete's reason")
		assert.Equal(t, sandbox.StopReasonKilled, resumed.GetStopReason())
		assert.Equal(t, int32(1), steps.asyncStops.Load(), "the published sandbox is stopped")
	})
}

// A Delete that answers NotFound at its deadline, over a checkpoint already
// ending for its own cause, records no acknowledgement: a publication that
// goes live afterwards is not killed with the reason of a Delete that said
// the sandbox was gone.
func TestCheckpointResumeFresh_DeleteAnsweringNotFoundLeavesNoAcknowledgement(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, kills := slowResumeServer(t)

		ctx, op, ok := s.registerDetachedCheckpoint(t.Context(), sbx)
		require.True(t, ok)
		r, err := op.reserve(ctx, s.sandboxFactory.Sandboxes, sbx)
		require.NoError(t, err)
		defer r.Release()
		op.cancel(errResumeAfterCheckpointTimeout)

		type ending struct {
			live  *sandbox.Sandbox
			ended bool
		}
		endDone := make(chan ending, 1)
		deleteCtx, cancelDelete := context.WithTimeout(t.Context(), requestTimeout)
		defer cancelDelete()
		go func() {
			live, ended := s.endCheckpointForDelete(deleteCtx, sbx.Runtime.SandboxID, "test-kill")
			endDone <- ending{live, ended}
		}()
		synctest.Wait()
		time.Sleep(requestTimeout)
		select {
		case got := <-endDone:
			require.False(t, got.ended, "the checkpoint ended for its own cause")
			require.Nil(t, got.live)
		case <-time.After(testWait):
			t.Fatal("the Delete did not answer at its deadline")
		}
		_, acknowledged := op.acknowledgedDelete()
		assert.False(t, acknowledged, "a Delete that answered NotFound left no acknowledgement")

		resumed := drainTestSandbox(t, "resumed")
		resumed.Runtime.TeamID = sbx.Runtime.TeamID
		op.setRegistered(resumed.LifecycleID)
		require.NoError(t, r.MarkRunning(ctx, resumed))
		s.killForAcknowledgedDelete(t.Context(), op, steps, sbx.Runtime.SandboxID)
		synctest.Wait()

		live, ok := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
		require.True(t, ok, "the published sandbox stays live")
		assert.Same(t, resumed, live)
		assert.Empty(t, kills.killed(), "no kill is published with that Delete's reason")
		assert.Zero(t, steps.asyncStops.Load())
	})
}

// A Delete at its deadline over a checkpoint ending for its own cause still
// reads the live map once: an original the map put back after a subscriber
// panic in the reservation is the checkpoint's, and is handed to the Delete
// to kill instead of being answered NotFound over.
func TestCheckpointResumeFresh_DeleteAtItsDeadlineIsHandedARestoredOriginal(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, _, _ := slowResumeServer(t)

		ctx, op, ok := s.registerDetachedCheckpoint(t.Context(), sbx)
		require.True(t, ok)

		type taken struct {
			sbx *sandbox.Sandbox
			err error
		}
		delDone := make(chan taken, 1)
		deleteCtx, cancelDelete := context.WithTimeout(t.Context(), requestTimeout)
		defer cancelDelete()
		var once sync.Once
		s.sandboxFactory.Sandboxes.Subscribe(stoppingProbe{probe: func() {
			once.Do(func() {
				// Shutdown ends the checkpoint under the reservation's
				// notification, a Delete arrives, then the subscriber panics:
				// the map drops the hold and puts the original back before
				// the Delete's deadline.
				op.cancel(errCheckpointServerShutdown)
				go func() {
					live, err := s.takeForDelete(deleteCtx, deleteRequest(sbx, "test-kill"), "test-kill", nil)
					delDone <- taken{live, err}
				}()
				synctest.Wait()
				panic("subscriber panicked")
			})
		}})
		require.PanicsWithValue(t, "subscriber panicked", func() {
			_, _ = op.reserve(ctx, s.sandboxFactory.Sandboxes, sbx)
		})
		assert.False(t, op.reservedSandbox())

		select {
		case got := <-delDone:
			require.NoError(t, got.err)
			require.Same(t, sbx, got.sbx, "the Delete is handed the restored original to kill")
		case <-time.After(testWait):
			t.Fatal("the Delete did not answer at its deadline")
		}
		_, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
		assert.False(t, live, "the Delete marked the restored original stopping")
		_, acknowledged := op.acknowledgedDelete()
		assert.False(t, acknowledged)
		op.release()
	})
}

// A Delete at its deadline over a checkpoint whose reservation was refused
// (a Pause took the sandbox between the claim and the reservation) killed
// nothing, although its cancellation landed on the claim: NotFound, with no
// acknowledgement for a kill another operation did.
func TestCheckpointResumeFresh_DeleteAtItsDeadlineOverARefusedReservationIsNotFound(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, _, kills := slowResumeServer(t)

		ctx, op, ok := s.registerDetachedCheckpoint(t.Context(), sbx)
		require.True(t, ok)
		defer op.cancel(nil)
		require.True(t, s.sandboxFactory.Sandboxes.MarkStopping(ctx, sbx.Runtime.SandboxID, sbx.LifecycleID))

		type ending struct {
			live  *sandbox.Sandbox
			ended bool
		}
		endDone := make(chan ending, 1)
		deleteCtx, cancelDelete := context.WithTimeout(t.Context(), requestTimeout)
		defer cancelDelete()
		go func() {
			live, ended := s.endCheckpointForDelete(deleteCtx, sbx.Runtime.SandboxID, "test-kill")
			endDone <- ending{live, ended}
		}()
		synctest.Wait()
		require.ErrorIs(t, context.Cause(ctx), errCheckpointDeleted, "the delete reached the checkpoint")

		_, err := op.reserve(ctx, s.sandboxFactory.Sandboxes, sbx)
		require.Error(t, err, "the checkpoint's reservation is refused")
		time.Sleep(requestTimeout)

		select {
		case got := <-endDone:
			assert.Nil(t, got.live)
			assert.False(t, got.ended, "the delete killed nothing")
		case <-time.After(testWait):
			t.Fatal("the Delete did not answer at its deadline")
		}
		_, acknowledged := op.acknowledgedDelete()
		assert.False(t, acknowledged, "no acknowledgement for a kill another operation did")
		_, publishes := deleteKillToPublish(op.cause(), err, op.reservedSandbox(), false)
		assert.False(t, publishes, "the checkpoint's end publishes no kill")
		op.release()
		assert.Empty(t, kills.killed())
	})
}

// A Delete at its deadline over a refused reservation whose taker's own
// subscriber then panicked, so the registry put the original back: the
// original is the checkpoint's, live again, and is handed to the Delete,
// which answers success for it.
func TestCheckpointResumeFresh_DeleteAtItsDeadlineOverARefusedReservationIsHandedTheRestoredOriginal(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, _, _ := slowResumeServer(t)

		ctx, op, ok := s.registerDetachedCheckpoint(t.Context(), sbx)
		require.True(t, ok)
		defer op.cancel(nil)

		type ending struct {
			live  *sandbox.Sandbox
			ended bool
		}
		endDone := make(chan ending, 1)
		deleteCtx, cancelDelete := context.WithTimeout(t.Context(), requestTimeout)
		defer cancelDelete()
		var once sync.Once
		s.sandboxFactory.Sandboxes.Subscribe(stoppingProbe{probe: func() {
			once.Do(func() {
				// Under the Pause's stopping notification the sandbox is out of
				// the live map: the checkpoint's reservation is refused and a
				// Delete's cancellation lands on the claim.
				_, err := op.reserve(ctx, s.sandboxFactory.Sandboxes, sbx)
				require.Error(t, err)
				go func() {
					live, ended := s.endCheckpointForDelete(deleteCtx, sbx.Runtime.SandboxID, "test-kill")
					endDone <- ending{live, ended}
				}()
				synctest.Wait()
				panic("subscriber panicked")
			})
		}})
		require.PanicsWithValue(t, "subscriber panicked", func() {
			s.sandboxFactory.Sandboxes.MarkStopping(ctx, sbx.Runtime.SandboxID, sbx.LifecycleID)
		})
		require.ErrorIs(t, context.Cause(ctx), errCheckpointDeleted)

		select {
		case got := <-endDone:
			assert.True(t, got.ended, "the Delete answers success")
			assert.Same(t, sbx, got.live, "the restored original is handed to the Delete")
		case <-time.After(testWait):
			t.Fatal("the Delete did not answer at its deadline")
		}
		_, acknowledged := op.acknowledgedDelete()
		assert.False(t, acknowledged, "the sandbox is in hand, nothing to acknowledge")
	})
}

// A Delete whose deadline expires while the reservation's subscriber still
// runs, before it panics and the map puts the original back, is honoured by
// the checkpoint as it ends: the restored original is killed with the
// Delete's reason, once, with no waiter left to do it.
func TestCheckpointResumeFresh_RestoreAfterAnAcknowledgedDeleteIsKilled(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, kills := slowResumeServer(t)
		defer steps.releaseAll()

		type ending struct {
			live  *sandbox.Sandbox
			ended bool
		}
		endDone := make(chan ending, 1)
		var once sync.Once
		s.sandboxFactory.Sandboxes.Subscribe(stoppingProbe{probe: func() {
			once.Do(func() {
				deleteCtx, cancelDelete := context.WithTimeout(t.Context(), requestTimeout)
				defer cancelDelete()
				go func() {
					live, ended := s.endCheckpointForDelete(deleteCtx, sbx.Runtime.SandboxID, "test-kill")
					endDone <- ending{live, ended}
				}()
				synctest.Wait()
				// Past the Delete's deadline: it has answered before the panic.
				time.Sleep(requestTimeout + time.Second)
				panic("subscriber panicked")
			})
		}})

		out := awaitOutcome(t, startCheckpointRecovering(t.Context(), s, steps, sbx))
		require.NotNil(t, out.panicked)
		synctest.Wait()

		select {
		case got := <-endDone:
			require.True(t, got.ended)
			require.Nil(t, got.live, "the original was out of the map when the Delete answered")
		case <-time.After(testWait):
			t.Fatal("the Delete did not answer at its deadline")
		}
		_, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
		assert.False(t, live, "the restored original is marked stopping by the checkpoint")
		assert.Equal(t, []string{"test-kill"}, kills.killed(), "one kill, with the Delete's reason")
		assert.Equal(t, sandbox.StopReasonKilled, sbx.GetStopReason())
	})
}

// A Delete that waits through a subscriber panic in the reservation is
// handed the original sandbox the map put back, and kills it: the deleted
// cause alone proves nothing about the sandbox.
func TestCheckpointResumeFresh_DeleteWaitingThroughTheReservationPanicKillsTheRestoredSandbox(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, kills := slowResumeServer(t)
		defer steps.releaseAll()

		type taken struct {
			sbx *sandbox.Sandbox
			err error
		}
		delDone := make(chan taken, 1)
		var once sync.Once
		s.sandboxFactory.Sandboxes.Subscribe(stoppingProbe{probe: func() {
			// The Delete's own marking of the restored sandbox notifies too.
			once.Do(func() {
				go func() {
					// The Delete read nothing live: the reservation removed the
					// sandbox before notifying. What it takes, Delete then kills.
					live, err := s.takeForDelete(t.Context(), deleteRequest(sbx, "test-kill"), "test-kill", nil)
					delDone <- taken{live, err}
				}()
				synctest.Wait()
				panic("subscriber panicked")
			})
		}})

		out := awaitOutcome(t, startCheckpointRecovering(t.Context(), s, steps, sbx))
		require.NotNil(t, out.panicked)
		synctest.Wait()

		select {
		case got := <-delDone:
			require.NoError(t, got.err)
			require.Same(t, sbx, got.sbx, "the Delete is handed the restored sandbox to kill")
		case <-time.After(testWait):
			t.Fatal("the Delete kept waiting")
		}
		_, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
		assert.False(t, live, "the Delete marked the restored sandbox stopping")
		assert.Empty(t, kills.killed(), "the checkpoint published no kill; the Delete's tail publishes its own")
	})
}

// Two Deletes acknowledged at their deadlines with nothing in hand keep the
// first reason: the checkpoint kills once, with it, and both answer success.
func TestTakeOrAcknowledgeKeepsTheFirstReason(t *testing.T) {
	t.Parallel()

	op := &detachedCheckpoint{original: "original"}
	none := func() (*sandbox.Sandbox, bool) { return nil, false }

	live, ended := op.takeOrAcknowledge(none, "first-kill", true)
	assert.Nil(t, live)
	assert.True(t, ended)
	live, ended = op.takeOrAcknowledge(none, "second-kill", true)
	assert.Nil(t, live)
	assert.True(t, ended, "the second Delete answers success too")
	reason, ok := op.acknowledgedDelete()
	require.True(t, ok)
	assert.Equal(t, "first-kill", reason)

	refused := &detachedCheckpoint{original: "original", refused: true}
	_, ended = refused.takeOrAcknowledge(none, "kill", true)
	assert.False(t, ended, "a refused reservation acknowledges nothing")
	_, ok = refused.acknowledgedDelete()
	assert.False(t, ok)
}

// stoppingCtxProbe is stoppingProbe with the notification's context.
type stoppingCtxProbe struct {
	probe func(context.Context)
}

func (stoppingCtxProbe) OnInsert(context.Context, *sandbox.Sandbox) {}

func (p stoppingCtxProbe) OnStopping(ctx context.Context, _ *sandbox.Sandbox) { p.probe(ctx) }

func (stoppingCtxProbe) OnNetworkRelease(context.Context, *sandbox.Sandbox) error { return nil }

func TestMarkKilledDuringCheckpoint(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct{ before, want sandbox.StopReason }{
		"checkpointing becomes killed": {before: sandbox.StopReasonCheckpointing, want: sandbox.StopReasonKilled},
		"unset becomes killed":         {before: "", want: sandbox.StopReasonKilled},
		"paused stays":                 {before: sandbox.StopReasonPaused, want: sandbox.StopReasonPaused},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			m := &sandbox.Metadata{}
			if tc.before != "" {
				m.SetStopReason(tc.before)
			}
			m.MarkKilledDuringCheckpoint()
			assert.Equal(t, tc.want, m.GetStopReason())
		})
	}
}

// A claim that ended between Delete's live-map miss and its lookup, because
// the resumed sandbox went live, still hands Delete that sandbox to kill
// instead of a NotFound over a live, unkilled VM.
func TestCheckpointResumeFresh_DeleteFindsTheSandboxThatWentLiveBeforeItLooked(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, _, _ := slowResumeServer(t)

		ctx, op, ok := s.registerDetachedCheckpoint(t.Context(), sbx)
		require.True(t, ok)
		defer op.cancel(nil)
		r, err := op.reserve(ctx, s.sandboxFactory.Sandboxes, sbx)
		require.NoError(t, err)
		defer r.Release()
		resumed := drainTestSandbox(t, "resumed")
		require.NoError(t, r.MarkRunning(ctx, resumed))
		op.release()

		live, ended := s.endCheckpointForDelete(t.Context(), sbx.Runtime.SandboxID, "test-kill")
		assert.True(t, ended)
		assert.Same(t, resumed, live, "Delete is handed the sandbox that went live")
	})
}

// The checkpoint publishes a Delete's kill only while it still owns the
// sandbox: not once the resumed sandbox went live (Delete kills it on the
// normal path) and not without a reservation (the map put it back).
func TestDeleteKillToPublish(t *testing.T) {
	t.Parallel()

	errOwn := errors.New("step failed")
	for name, tc := range map[string]struct {
		deleted  bool
		err      error
		reserved bool
		wentLive bool
		want     bool
	}{
		"owned and ended by the delete": {deleted: true, err: errOwn, reserved: true, want: true},
		"went live first":               {deleted: true, err: errOwn, reserved: true, wentLive: true},
		"no reservation taken":          {deleted: true, err: errOwn},
		"ended without a delete":        {err: errOwn, reserved: true},
		"succeeded":                     {deleted: true, reserved: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cause := errOwn
			if tc.deleted {
				cause = &deletedDuringCheckpointError{killReason: "test-kill"}
			}

			reason, ok := deleteKillToPublish(cause, tc.err, tc.reserved, tc.wentLive)
			assert.Equal(t, tc.want, ok)
			if tc.want {
				assert.Equal(t, "test-kill", reason)
			}
		})
	}
}

// A Delete that reached the claim while the reservation's subscriber
// panicked: the map put the sandbox back live, so the checkpoint, which
// ended with the delete cause but owns nothing, publishes no kill and
// relabels nothing.
func TestCheckpointResumeFresh_PanicInReservationDuringDeletePublishesNoKill(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, kills := slowResumeServer(t)
		defer steps.releaseAll()

		s.sandboxFactory.Sandboxes.Subscribe(stoppingProbe{probe: func() {
			s.detachedCheckpointFor(sbx.Runtime.SandboxID).cancel(&deletedDuringCheckpointError{killReason: "test-kill"})
			panic("subscriber panicked")
		}})

		out := awaitOutcome(t, startCheckpointRecovering(t.Context(), s, steps, sbx))
		synctest.Wait()

		require.NotNil(t, out.panicked)
		_, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
		require.True(t, live, "the map put the sandbox back")
		assert.Empty(t, kills.killed(), "the checkpoint owns nothing to kill")
		assert.Equal(t, sandbox.StopReasonCrashed, sbx.GetStopReason(), "no stop reason was recorded")
	})
}

// A checkpoint already ending on its budget when the Delete arrives is not
// that Delete's kill either: NotFound, nothing published.
func TestCheckpointResumeFresh_DeleteAfterBudgetIsNotFound(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, kills := slowResumeServer(t)
		defer steps.releaseAll()
		steps.blockStop = true

		done := startSlowCheckpoint(t.Context(), s, steps, sbx)
		require.ErrorContains(t, awaitCheckpoint(t, done), resumeBudgetMessage)

		delDone := make(chan error, 1)
		go func() {
			_, err := s.Delete(t.Context(), deleteRequest(sbx, "test-kill"))
			delDone <- err
		}()
		synctest.Wait()
		steps.releaseStop()
		assert.Equal(t, codes.NotFound, status.Code(awaitCheckpoint(t, delDone)))
		synctest.Wait()

		assert.Empty(t, kills.killed())
		assert.Equal(t, sandbox.StopReasonCheckpointing, sbx.GetStopReason())
	})
}

// A Delete whose own deadline expires while the rollback of a checkpoint
// that failed on its own still runs is answered as it is after the wait:
// that checkpoint is not this Delete's kill, so NotFound with nothing
// published, not success over a kill nobody did.
func TestCheckpointResumeFresh_DeleteAtItsDeadlineAfterOwnFailureIsNotFound(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, kills := slowResumeServer(t)
		defer steps.releaseAll()
		steps.blockStop = true
		steps.releaseResume()

		done := startSlowCheckpoint(t.Context(), s, steps, sbx)
		require.ErrorContains(t, awaitCheckpoint(t, done), errResumeStub.Error())

		deleteStart := time.Now()
		_, delErr := s.Delete(t.Context(), deleteRequest(sbx, "test-kill"))
		assert.Equal(t, codes.NotFound, status.Code(delErr), "the delete did not end the checkpoint")
		assert.Equal(t, requestTimeout, time.Since(deleteStart), "the Delete waited out its request budget")
		assert.NotNil(t, s.detachedCheckpointFor(sbx.Runtime.SandboxID), "the rollback still runs when the Delete answers")

		steps.releaseStop()
		synctest.Wait()

		assert.Empty(t, kills.killed(), "nothing is published as this delete's kill")
		assert.Equal(t, sandbox.StopReasonCheckpointing, sbx.GetStopReason())
		assert.Nil(t, s.detachedCheckpointFor(sbx.Runtime.SandboxID))
		assert.Zero(t, s.info.OutstandingWork())
	})
}

// A Delete that ended a checkpoint which never reserved the sandbox, because
// another operation took it between the claim and the reservation, killed
// nothing: the checkpoint publishes no kill, and the Delete answers as it
// would have with nothing in flight, NotFound, not success.
func TestCheckpointResumeFresh_DeleteOfACheckpointThatNeverReservedIsNotFound(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, _, kills := slowResumeServer(t)

		ctx, op, ok := s.registerDetachedCheckpoint(t.Context(), sbx)
		require.True(t, ok)
		defer op.cancel(nil)
		// A Pause took the sandbox out of the live map under the claim.
		require.True(t, s.sandboxFactory.Sandboxes.MarkStopping(ctx, sbx.Runtime.SandboxID, sbx.LifecycleID))

		type ending struct {
			live  *sandbox.Sandbox
			ended bool
		}
		endDone := make(chan ending, 1)
		go func() {
			live, ended := s.endCheckpointForDelete(t.Context(), sbx.Runtime.SandboxID, "test-kill")
			endDone <- ending{live, ended}
		}()
		synctest.Wait()
		require.ErrorIs(t, context.Cause(ctx), errCheckpointDeleted, "the delete reached the checkpoint")

		_, err := op.reserve(ctx, s.sandboxFactory.Sandboxes, sbx)
		require.Error(t, err, "the checkpoint's reservation is refused")
		assert.False(t, op.reservedSandbox())
		_, publishes := deleteKillToPublish(op.cause(), err, op.reservedSandbox(), false)
		assert.False(t, publishes, "the checkpoint's end publishes no kill")
		op.release()

		select {
		case got := <-endDone:
			assert.Nil(t, got.live)
			assert.False(t, got.ended, "the delete killed nothing")
		case <-time.After(testWait):
			t.Fatal("Delete kept waiting after the claim ended")
		}
		assert.Empty(t, kills.killed())
	})
}

// A Delete that read the sandbox from the live map and then lost the
// marking to the checkpoint's reservation ends the checkpoint like a miss
// would, instead of failing: the claim precedes the reservation for this.
func TestCheckpointResumeFresh_DeleteThatLostTheMarkingEndsTheCheckpoint(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, kills := slowResumeServer(t)
		defer steps.releaseAll()

		done := startSlowCheckpoint(t.Context(), s, steps, sbx)
		awaitStep(t, steps.resumeStarted, "resume")

		// sbx is what Delete read before the checkpoint reserved it.
		taken, err := s.takeForDelete(t.Context(), deleteRequest(sbx, "test-kill"), "test-kill", sbx)
		require.NoError(t, err)
		assert.Nil(t, taken, "the checkpoint ended with the kill")
		require.ErrorContains(t, awaitCheckpoint(t, done), deletedMessage)
		synctest.Wait()

		assert.Equal(t, []string{"test-kill"}, kills.killed())
		assert.Equal(t, sandbox.StopReasonKilled, sbx.GetStopReason())
	})
}

// What a Delete reads and what the checkpoint decides its kill from is the
// same value: a Delete whose cancel lands after a step failed for its own
// reason, before the checkpoint's outcome is decided (the phase budget is
// already cancelled by then), still gets its kill recorded and published,
// and is answered OK on the same ground.
func TestCheckpointResumeFresh_DeleteLandingAfterTheStepFailedIsStillTheKill(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, kills := slowResumeServer(t)
		defer steps.releaseAll()
		steps.releaseResume()
		steps.onStopAsync = func() {
			s.detachedCheckpointFor(sbx.Runtime.SandboxID).cancel(&deletedDuringCheckpointError{killReason: "test-kill"})
		}

		err := awaitCheckpoint(t, startSlowCheckpoint(t.Context(), s, steps, sbx))
		synctest.Wait()
		require.ErrorContains(t, err, errResumeStub.Error())

		assert.Equal(t, []string{"test-kill"}, kills.killed(), "the kill the delete asked for is published")
		assert.Equal(t, sandbox.StopReasonKilled, sbx.GetStopReason())
	})
}

// A lifecycle another operation registered under the ID once the
// checkpoint released it is not the Delete's to kill: after the join the
// Delete is handed only the lifecycle the checkpoint published.
func TestCheckpointResumeFresh_DeleteDoesNotTakeAStrangersLifecycle(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, _, _ := slowResumeServer(t)

		ctx, op, ok := s.registerDetachedCheckpoint(t.Context(), sbx)
		require.True(t, ok)
		defer op.cancel(nil)
		r, err := op.reserve(ctx, s.sandboxFactory.Sandboxes, sbx)
		require.NoError(t, err)

		type ending struct {
			live  *sandbox.Sandbox
			ended bool
		}
		endDone := make(chan ending, 1)
		go func() {
			live, ended := s.endCheckpointForDelete(t.Context(), sbx.Runtime.SandboxID, "test-kill")
			endDone <- ending{live, ended}
		}()
		synctest.Wait()

		// The checkpoint's rollback frees the ID, an API resume registers a
		// new lifecycle under it, then the claim ends.
		r.Release()
		stranger := drainTestSandbox(t, "stranger")
		r2, err := s.sandboxFactory.Sandboxes.Reserve(sbx.Runtime.SandboxID)
		require.NoError(t, err)
		require.NoError(t, r2.MarkRunning(ctx, stranger))
		defer r2.Release()
		op.release()

		select {
		case got := <-endDone:
			assert.Nil(t, got.live, "the stranger's lifecycle is not handed to the Delete")
			assert.True(t, got.ended, "the delete did end the checkpoint")
		case <-time.After(testWait):
			t.Fatal("Delete kept waiting after the claim ended")
		}
		live, ok := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
		require.True(t, ok)
		assert.Same(t, stranger, live)
	})
}

// A resumed sandbox torn down because the resume budget ran out was not
// killed: only a Delete's end records killed on it. Nor did it crash: it
// ended before it registered, and reads so to the lifecycle goroutine that
// would otherwise count the node's own teardown as a crash.
func TestCheckpointResumeFresh_BudgetDoesNotMarkTheResumedSandboxKilled(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		s, sbx, steps, _ := slowResumeServer(t)
		defer steps.releaseAll()
		steps.ignoreCancel = true
		steps.resumed = drainTestSandbox(t, "resumed")

		done := startSlowCheckpoint(t.Context(), s, steps, sbx)
		awaitStep(t, steps.resumeStarted, "resume")
		time.Sleep(testWait)
		steps.releaseResume()

		err := awaitCheckpoint(t, done)
		synctest.Wait()

		require.ErrorIs(t, steps.resumeCtxErr, context.DeadlineExceeded, "the budget did expire")
		require.ErrorContains(t, err, resumeBudgetMessage)
		assert.Equal(t, sandbox.StopReasonRegistrationFailed, steps.resumed.GetStopReason(), "neither killed nor crashed")
		assert.Equal(t, int32(2), steps.stopAndCloses.Load(), "the resumed sandbox is still torn down")
	})
}

func TestWithContextCause(t *testing.T) {
	t.Parallel()

	errOwn := errors.New("step failed on its own")
	errCause := errors.New("named cause")

	for name, tc := range map[string]struct {
		cancelled bool
		expired   bool
		cause     error
		err       error
		want      []error
		not       []error
	}{
		"own error keeps its cause out":       {cancelled: true, cause: errCause, err: errOwn, want: []error{errOwn}, not: []error{errCause}},
		"cancellation names the cause":        {cancelled: true, cause: errCause, err: context.Canceled, want: []error{context.Canceled, errCause}},
		"wrapped cancellation names it":       {cancelled: true, cause: errCause, err: errors.Join(errOwn, context.Canceled), want: []error{errOwn, context.Canceled, errCause}},
		"expired deadline names the cause":    {expired: true, cause: errCause, err: context.DeadlineExceeded, want: []error{context.DeadlineExceeded, errCause}},
		"a step's own deadline is not blamed": {cancelled: true, cause: errCause, err: context.DeadlineExceeded, want: []error{context.DeadlineExceeded}, not: []error{errCause}},
		"status-wrapped cancellation":         {cancelled: true, cause: errCause, err: status.Error(codes.Canceled, "envd init wait cancelled"), want: []error{errCause}},
		"status deadline under a deadline":    {expired: true, cause: errCause, err: status.Error(codes.DeadlineExceeded, "envd init wait timed out"), want: []error{errCause}},
		"status cancellation, live context":   {err: status.Error(codes.Canceled, "envd init wait cancelled"), not: []error{errCause}},
		"status deadline under a cancel":      {cancelled: true, cause: errCause, err: status.Error(codes.DeadlineExceeded, "envd init wait timed out"), not: []error{errCause}},
		"status cancel under a deadline":      {expired: true, cause: errCause, err: status.Error(codes.Canceled, "envd init wait cancelled"), not: []error{errCause}},
		"live context adds nothing":           {err: context.DeadlineExceeded, want: []error{context.DeadlineExceeded}},
		"unnamed cancellation":                {cancelled: true, err: context.Canceled, want: []error{context.Canceled}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			switch {
			case tc.expired:
				expired, cancel := context.WithTimeoutCause(ctx, -time.Second, tc.cause)
				defer cancel()
				ctx = expired
			case tc.cancelled:
				cancelled, cancel := context.WithCancelCause(ctx)
				cancel(tc.cause)
				ctx = cancelled
			}

			got := withContextCause(ctx, tc.err)
			for _, want := range tc.want {
				require.ErrorIs(t, got, want)
			}
			for _, not := range tc.not {
				require.NotErrorIs(t, got, not)
			}
			if tc.cause == nil || len(tc.not) > 0 {
				require.Equal(t, tc.err, got, "nothing to name, the error is returned as is")
			}
		})
	}
}
