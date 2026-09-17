package orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	sandboxredis "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// Once Drain returns with the counter at zero, nothing may be admitted: a
// TrackWork racing the flip must either land before Drain's Wait sees the
// counter, or be refused. The check and the Add are one step under drainMu
// for exactly this reason; an atomic bool splits them and lets an Add land
// after a Drain that already returned. Run under contention, where that
// window actually opens.
func TestTrackWork_NothingAdmittedBehindADrain(t *testing.T) {
	t.Parallel()

	for range 2000 {
		o := &Orchestrator{}
		start := make(chan struct{})
		roundOver := make(chan struct{})
		var wg sync.WaitGroup

		// Admitted work holds its slot until the round ends, so an admission
		// that slipped past the flip is still counted when Drain is checked.
		for range 32 {
			wg.Go(func() {
				<-start
				release, ok := o.TrackWork()
				if !ok {
					return
				}
				<-roundOver
				release()
			})
		}

		var drainErr error
		drainDone := make(chan struct{})
		go func() {
			defer close(drainDone)
			// Bounded: a correct Drain that admitted work must wait for the
			// held slots, so it returns on this deadline rather than nil.
			ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
			defer cancel()
			<-start
			drainErr = o.Drain(ctx)
		}()

		close(start)
		<-drainDone
		// A nil return means Drain saw the counter at zero after publishing
		// the flip, so nothing may be counted now. A split check-then-Add
		// lets an admission land after that zero and fails this.
		if drainErr == nil {
			require.Zero(t, o.OutstandingWork(), "Drain returned nil with work it never waited for")
		}
		close(roundOver)
		wg.Wait()
	}
}

// Three budgets nest, and each one exists to outlast the one inside it. A
// tracked pause holds its count through its terminal write, so the drain must
// wait longer than that; and the Redis transition key must outlive the drain,
// or a waiter reads the vanished key as success while the owner is still
// committing. Pinning the order here is what makes moving one of them alone
// fail the build instead of a deploy.
func TestDrainBudgetsNest(t *testing.T) {
	t.Parallel()

	assert.Equal(t, pauseTimeout+buildStatusWriteTimeout, trackedWorkBound,
		"the bound must be the pause plus the terminal write it falls through to")
	assert.Greater(t, trackedWorkBound+workDrainGrace, trackedWorkBound,
		"the drain must outlast the work it waits for")
	assert.Greater(t, sandboxredis.TransitionKeyTTL, trackedWorkBound,
		"the transition key must outlive the work that owns it")
}

// gatedKillClient holds a kill inside its node RPC, the same in-flight window
// the pause tests gate.
type gatedKillClient struct {
	orchestrator.SandboxServiceClient

	gate    <-chan struct{}
	entered chan struct{}
}

func (c *gatedKillClient) Delete(_ context.Context, request *orchestrator.SandboxDeleteRequest, _ ...grpc.CallOption) (*orchestrator.SandboxDeleteResponse, error) {
	close(c.entered)
	<-c.gate

	return &orchestrator.SandboxDeleteResponse{StopCompleted: request.GetWaitForStop()}, nil
}

// gatedPauseFixture holds a pause inside its node RPC until the returned
// release runs, which is the window a shutdown must not cut short.
func gatedPauseFixture(t *testing.T) (refusalFixture, func(), <-chan error) {
	t.Helper()

	f := newRefusalFixture(t, true, consts.LocalClusterID, nil)
	gate := make(chan struct{})
	node := f.o.GetNode(f.sbx.ClusterID, f.sbx.NodeID)
	require.NotNil(t, node)
	node.SetSandboxClient(&pauseStubClient{gate: gate})

	done := make(chan error, 1)
	go func() {
		done <- f.o.RemoveSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionPause})
	}()

	require.Eventually(t, func() bool {
		stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)

		return err == nil && stored.State == sandbox.StatePausing
	}, 5*time.Second, 5*time.Millisecond, "the pause must reach the node before the drain starts")

	return f, func() { close(gate) }, done
}

func TestDrain_WaitsForPauseInFlight(t *testing.T) {
	t.Parallel()

	f, release, done := gatedPauseFixture(t)
	assert.Equal(t, int64(1), f.o.OutstandingWork())

	waitCtx, cancelWait := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancelWait()
	require.ErrorIs(t, f.o.Drain(waitCtx), context.DeadlineExceeded,
		"a pause still writing its snapshot must hold the drain")

	release()
	require.NoError(t, <-done)
	require.NoError(t, f.o.Drain(t.Context()), "the drain must finish once the pause does")
	assert.Zero(t, f.o.OutstandingWork())

	snapshot, err := f.o.sqlcDB.GetLastSnapshot(t.Context(), f.sbx.SandboxID)
	require.NoError(t, err)
	assert.NotNil(t, snapshot.EnvBuild.FinishedAt, "the drained pause recorded its terminal build")
}

// The evictor sweeps on a context the drain does not cancel, so a drained API
// must refuse the pause it would otherwise start behind the drain's back.
func TestDrain_RefusesPausesAfterDraining(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, nil)
	require.NoError(t, f.o.Drain(t.Context()))

	// The request and the evictor reach the same entry point, and neither may
	// start a pause the drain is no longer waiting for.
	for _, opts := range []sandbox.RemoveOpts{
		{Action: sandbox.StateActionPause},
		{Action: sandbox.StateActionPause, Eviction: true, ExpectExecutionID: f.sbx.ExecutionID},
	} {
		require.ErrorIs(t, f.o.RemoveSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, opts), ErrDraining)

		stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
		require.NoError(t, err, "a refused pause leaves the sandbox for another replica")
		assert.Equal(t, sandbox.StateRunning, stored.State)

		_, err = f.o.sqlcDB.GetLastSnapshot(t.Context(), f.sbx.SandboxID)
		require.Error(t, err, "a refused pause must not record a snapshot")
	}

	assert.Zero(t, f.o.OutstandingWork())
}

func TestDrain_SurvivesCallerCancellation(t *testing.T) {
	t.Parallel()

	f, release, done := gatedPauseFixture(t)

	require.ErrorIs(t, f.o.Drain(cancelledContext(t)), context.Canceled)

	release()
	require.NoError(t, <-done)
}

func TestDrain_IdleReturnsImmediately(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, nil)

	start := time.Now()
	require.NoError(t, f.o.Drain(t.Context()))
	assert.Less(t, time.Since(start), time.Second, "an idle API must not delay shutdown")
}

func TestDrain_IgnoresKills(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, nil)
	gate := make(chan struct{})
	entered := make(chan struct{})
	node := f.o.GetNode(f.sbx.ClusterID, f.sbx.NodeID)
	require.NotNil(t, node)
	node.SetSandboxClient(&gatedKillClient{gate: gate, entered: entered})

	killed := make(chan error, 1)
	go func() {
		killed <- f.o.RemoveSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionKill})
	}()
	<-entered

	start := time.Now()
	assert.Zero(t, f.o.OutstandingWork(), "a kill is bound to its caller, so it is not tracked work")
	require.NoError(t, f.o.Drain(t.Context()))
	assert.Less(t, time.Since(start), time.Second)

	close(gate)
	require.NoError(t, <-killed)
}
