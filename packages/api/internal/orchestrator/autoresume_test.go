package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	redisreservations "github.com/e2b-dev/infra/packages/api/internal/sandbox/reservations/redis"
	sandboxredis "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

func newTestAutoResumeOrchestrator(t *testing.T) *Orchestrator {
	t.Helper()

	client := redis_utils.SetupInstance(t)
	storage, err := sandboxredis.NewStorage(client, noop.NewMeterProvider(), nil)
	require.NoError(t, err)
	go storage.Start(t.Context())
	t.Cleanup(func() { storage.Close(context.WithoutCancel(t.Context())) })

	return &Orchestrator{
		sandboxStore: sandbox.NewStore(
			storage,
			redisreservations.NewReservationStorage(client, storage.Notifier()),
			sandbox.Callbacks{
				AsyncNewlyCreatedSandbox: func(context.Context, sandbox.Sandbox, sandbox.CreationMetadata) {},
			},
		),
		nodes: smap.New[*nodemanager.Node](),
	}
}

func testSandboxForAutoResume(state sandbox.State) sandbox.Sandbox {
	return sandbox.Sandbox{
		SandboxID:         "test-sandbox",
		ExecutionID:       uuid.NewString(),
		TeamID:            uuid.New(),
		StartTime:         time.Now(),
		EndTime:           time.Now().Add(time.Hour),
		MaxInstanceLength: time.Hour,
		State:             state,
		NodeID:            "node-1",
		ClusterID:         uuid.New(),
	}
}

func addSandbox(t *testing.T, o *Orchestrator, sbx sandbox.Sandbox) {
	t.Helper()
	require.NoError(t, o.sandboxStore.Add(t.Context(), sbx, nil))
}

func registerNode(o *Orchestrator, sbx sandbox.Sandbox, ip string) {
	o.registerNode(&nodemanager.Node{
		ID:         sbx.NodeID,
		ClusterID:  sbx.ClusterID,
		IPAddress:  ip,
		WorkloadID: "test-node",
	})
}

func TestHandleExistingSandboxAutoResume(t *testing.T) {
	t.Parallel()

	t.Run("running sandbox returns node ip immediately", func(t *testing.T) {
		t.Parallel()

		o := newTestAutoResumeOrchestrator(t)
		sbx := testSandboxForAutoResume(sandbox.StateRunning)
		registerNode(o, sbx, "10.0.0.1")

		nodeIP, handled, err := o.HandleExistingSandboxAutoResume(t.Context(), sbx.TeamID, sbx.SandboxID, sbx, time.Minute)
		require.NoError(t, err)
		assert.True(t, handled)
		assert.Equal(t, "10.0.0.1", nodeIP)
	})

	t.Run("running sandbox with empty ip returns error", func(t *testing.T) {
		t.Parallel()

		o := newTestAutoResumeOrchestrator(t)
		sbx := testSandboxForAutoResume(sandbox.StateRunning)
		registerNode(o, sbx, "")

		nodeIP, handled, err := o.HandleExistingSandboxAutoResume(t.Context(), sbx.TeamID, sbx.SandboxID, sbx, time.Minute)
		require.ErrorContains(t, err, "routing info is not available")
		assert.False(t, handled)
		assert.Empty(t, nodeIP)
	})

	t.Run("snapshotting sandbox waits and routes when transition finishes", func(t *testing.T) {
		t.Parallel()

		o := newTestAutoResumeOrchestrator(t)
		sbx := testSandboxForAutoResume(sandbox.StateRunning)
		addSandbox(t, o, sbx)
		registerNode(o, sbx, "10.0.0.2")

		_, alreadyDone, finish, err := o.sandboxStore.StartRemoving(t.Context(), sbx.TeamID, sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionSnapshot})
		require.NoError(t, err)
		assert.False(t, alreadyDone)
		require.NotNil(t, finish)

		snapshottingSandbox, err := o.GetSandbox(t.Context(), sbx.TeamID, sbx.SandboxID)
		require.NoError(t, err)
		assert.Equal(t, sandbox.StateSnapshotting, snapshottingSandbox.State)

		go func() {
			time.Sleep(10 * time.Millisecond)
			finish(t.Context(), nil)
		}()

		nodeIP, handled, err := o.HandleExistingSandboxAutoResume(t.Context(), sbx.TeamID, sbx.SandboxID, snapshottingSandbox, time.Minute)
		require.NoError(t, err)
		assert.True(t, handled)
		assert.Equal(t, "10.0.0.2", nodeIP)
	})

	t.Run("pausing sandbox restored after a refusal routes to the running node", func(t *testing.T) {
		t.Parallel()

		o := newTestAutoResumeOrchestrator(t)
		sbx := testSandboxForAutoResume(sandbox.StateRunning)
		addSandbox(t, o, sbx)
		registerNode(o, sbx, "10.0.0.3")

		transition, alreadyDone, finish, err := o.sandboxStore.StartRemoving(t.Context(), sbx.TeamID, sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionPause})
		require.NoError(t, err)
		require.False(t, alreadyDone)

		go func() {
			time.Sleep(10 * time.Millisecond)
			_, err := o.sandboxStore.RestoreRunning(context.WithoutCancel(t.Context()), transition, 0)
			assert.NoError(t, err)
			finish(context.WithoutCancel(t.Context()), sandbox.ErrTransitionRestored)
		}()

		nodeIP, handled, err := o.HandleExistingSandboxAutoResume(t.Context(), sbx.TeamID, sbx.SandboxID, transition.Sandbox, time.Minute)
		require.NoError(t, err)
		assert.True(t, handled)
		assert.Equal(t, "10.0.0.3", nodeIP)
	})

	t.Run("pausing sandbox returns still transitioning after retries", func(t *testing.T) {
		t.Parallel()

		o := newTestAutoResumeOrchestrator(t)
		sbx := testSandboxForAutoResume(sandbox.StateRunning)
		addSandbox(t, o, sbx)

		_, alreadyDone, finish, err := o.sandboxStore.StartRemoving(t.Context(), sbx.TeamID, sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionPause})
		require.NoError(t, err)
		assert.False(t, alreadyDone)
		require.NotNil(t, finish)
		finish(t.Context(), nil)

		pausingSandbox, err := o.GetSandbox(t.Context(), sbx.TeamID, sbx.SandboxID)
		require.NoError(t, err)
		assert.Equal(t, sandbox.StatePausing, pausingSandbox.State)

		_, handled, err := o.HandleExistingSandboxAutoResume(t.Context(), sbx.TeamID, sbx.SandboxID, pausingSandbox, time.Minute)
		require.Error(t, err)
		assert.False(t, handled)
		assert.ErrorIs(t, err, ErrSandboxStillTransitioning)
	})

	t.Run("pausing sandbox wait failure returns still transitioning", func(t *testing.T) {
		t.Parallel()

		o := newTestAutoResumeOrchestrator(t)
		sbx := testSandboxForAutoResume(sandbox.StateRunning)
		addSandbox(t, o, sbx)

		_, alreadyDone, finish, err := o.sandboxStore.StartRemoving(t.Context(), sbx.TeamID, sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionPause})
		require.NoError(t, err)
		assert.False(t, alreadyDone)
		require.NotNil(t, finish)
		finish(t.Context(), errors.New("boom"))

		pausingSandbox, err := o.GetSandbox(t.Context(), sbx.TeamID, sbx.SandboxID)
		require.NoError(t, err)

		_, handled, err := o.HandleExistingSandboxAutoResume(t.Context(), sbx.TeamID, sbx.SandboxID, pausingSandbox, time.Minute)
		require.Error(t, err)
		assert.False(t, handled)
		assert.ErrorIs(t, err, ErrSandboxStillTransitioning)
	})

	t.Run("pausing sandbox wait timeout returns failed precondition", func(t *testing.T) {
		t.Parallel()

		o := newTestAutoResumeOrchestrator(t)
		sbx := testSandboxForAutoResume(sandbox.StateRunning)
		addSandbox(t, o, sbx)

		_, alreadyDone, _, err := o.sandboxStore.StartRemoving(t.Context(), sbx.TeamID, sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionPause})
		require.NoError(t, err)
		assert.False(t, alreadyDone)

		pausingSandbox, err := o.GetSandbox(t.Context(), sbx.TeamID, sbx.SandboxID)
		require.NoError(t, err)

		_, handled, err := o.HandleExistingSandboxAutoResume(t.Context(), sbx.TeamID, sbx.SandboxID, pausingSandbox, 5*time.Millisecond)
		require.Error(t, err)
		assert.False(t, handled)
		assert.ErrorIs(t, err, ErrSandboxStillTransitioning)
	})

	t.Run("killing sandbox returns not found", func(t *testing.T) {
		t.Parallel()

		o := newTestAutoResumeOrchestrator(t)
		sbx := testSandboxForAutoResume(sandbox.StateKilling)

		_, handled, err := o.HandleExistingSandboxAutoResume(t.Context(), sbx.TeamID, sbx.SandboxID, sbx, time.Minute)
		require.Error(t, err)
		assert.False(t, handled)
		assert.ErrorIs(t, err, sandbox.ErrNotFound)
	})

	t.Run("unknown sandbox state returns internal error", func(t *testing.T) {
		t.Parallel()

		o := newTestAutoResumeOrchestrator(t)
		sbx := testSandboxForAutoResume(sandbox.State("mystery"))

		_, handled, err := o.HandleExistingSandboxAutoResume(t.Context(), sbx.TeamID, sbx.SandboxID, sbx, time.Minute)
		require.Error(t, err)
		assert.False(t, handled)
		assert.EqualError(t, err, "sandbox is in an unknown state")
	})
}
