package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	redisreservations "github.com/e2b-dev/infra/packages/api/internal/sandbox/reservations/redis"
	sandboxredis "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

func newTestAutoResumeOrchestrator(t *testing.T, hooks ...redis.Hook) *Orchestrator {
	t.Helper()

	client := redis_utils.SetupInstance(t)
	for _, hook := range hooks {
		client.AddHook(hook)
	}
	storage, err := sandboxredis.NewStorage(client, noop.NewMeterProvider())
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

		resp, handled, err := o.HandleExistingSandboxAutoResume(t.Context(), sbx.TeamID, sbx.SandboxID, sbx, time.Minute)
		require.NoError(t, err)
		assert.True(t, handled)
		assert.Equal(t, "10.0.0.1", resp.GetOrchestratorIp())
	})

	t.Run("running sandbox with empty ip returns error", func(t *testing.T) {
		t.Parallel()

		o := newTestAutoResumeOrchestrator(t)
		sbx := testSandboxForAutoResume(sandbox.StateRunning)
		registerNode(o, sbx, "")

		resp, handled, err := o.HandleExistingSandboxAutoResume(t.Context(), sbx.TeamID, sbx.SandboxID, sbx, time.Minute)
		require.ErrorContains(t, err, "routing info is not available")
		assert.False(t, handled)
		assert.Nil(t, resp)
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

		resp, handled, err := o.HandleExistingSandboxAutoResume(t.Context(), sbx.TeamID, sbx.SandboxID, snapshottingSandbox, time.Minute)
		require.NoError(t, err)
		assert.True(t, handled)
		assert.Equal(t, "10.0.0.2", resp.GetOrchestratorIp())
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
			_, err := o.sandboxStore.RestoreRunning(context.WithoutCancel(t.Context()), transition, sandbox.RestoreOpts{})
			assert.NoError(t, err)
			finish(context.WithoutCancel(t.Context()), sandbox.ErrTransitionRestored)
		}()

		resp, handled, err := o.HandleExistingSandboxAutoResume(t.Context(), sbx.TeamID, sbx.SandboxID, transition.Sandbox, time.Minute)
		require.NoError(t, err)
		assert.True(t, handled)
		assert.Equal(t, "10.0.0.3", resp.GetOrchestratorIp())
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

// transitionWaitProbe closes waiting once, after being armed, the transition
// key of sandboxID is read twice: by WaitForStateChange and again by the
// wait it starts after subscribing to the transition's completion.
type transitionWaitProbe struct {
	sandboxID string
	armed     atomic.Bool
	reads     atomic.Int32
	waiting   chan struct{}
	once      sync.Once
}

func (p *transitionWaitProbe) DialHook(next redis.DialHook) redis.DialHook { return next }

func (p *transitionWaitProbe) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (p *transitionWaitProbe) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if p.armed.Load() && cmd.Name() == "get" && strings.HasSuffix(fmt.Sprint(cmd.Args()[1]), ":transition:"+p.sandboxID) && p.reads.Add(1) == 2 {
			p.once.Do(func() { close(p.waiting) })
		}

		return err
	}
}

// A running sandbox, and one that returns to Running from a checkpoint
// while the caller waits, are answered with the placement saved on the
// record. A full-router sandbox has no orchestrator in the node cache, and
// on-demand discovery has no clusters, so any orchestrator lookup would fail.
func TestHandleExistingSandboxAutoResume_ReturnsSavedPlacement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		placement *orchestrator.SandboxNetworkPlacement
		// checkpointed is the placement the checkpointed VM reports: the same
		// router and execution with a new endpoint.
		checkpointed *orchestrator.SandboxNetworkPlacement
		nodeIP       string
	}{
		{name: "legacy", nodeIP: "10.0.0.1"},
		{
			name:         "egress-router",
			placement:    &orchestrator.SandboxNetworkPlacement{NetworkMode: "egress-router", RouterId: "router-a", IngressAddress: "10.0.0.1", IngressPort: 5007},
			checkpointed: &orchestrator.SandboxNetworkPlacement{NetworkMode: "egress-router", RouterId: "router-a", IngressAddress: "10.0.0.1", IngressPort: 5008},
			nodeIP:       "10.0.0.1",
		},
		{
			name:         "full-router",
			placement:    &orchestrator.SandboxNetworkPlacement{NetworkMode: "full-router", RouterId: "router-b", IngressAddress: "10.8.0.2", IngressPort: 5010},
			checkpointed: &orchestrator.SandboxNetworkPlacement{NetworkMode: "full-router", RouterId: "router-b", IngressAddress: "10.8.0.3", IngressPort: 5011},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sbx := testSandboxForAutoResume(sandbox.StateRunning)
			probe := &transitionWaitProbe{sandboxID: sbx.SandboxID, waiting: make(chan struct{})}
			o := newTestAutoResumeOrchestrator(t, probe)
			if tt.nodeIP != "" {
				registerNode(o, sbx, tt.nodeIP)
			}
			want := func(placement *orchestrator.SandboxNetworkPlacement) *proxygrpc.SandboxResumeResponse {
				if placement == nil {
					return &proxygrpc.SandboxResumeResponse{OrchestratorIp: tt.nodeIP}
				}
				placement.ExecutionId = sbx.ExecutionID

				return &proxygrpc.SandboxResumeResponse{OrchestratorIp: tt.nodeIP, NetworkPlacement: placement}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()

			running := want(tt.placement)
			sbx.NetworkPlacement = tt.placement
			addSandbox(t, o, sbx)
			resp, handled, err := o.HandleExistingSandboxAutoResume(ctx, sbx.TeamID, sbx.SandboxID, sbx, time.Minute)
			require.NoError(t, err)
			assert.True(t, handled)
			assert.Truef(t, proto.Equal(running, resp), "want %v, got %v", running, resp)

			transition, _, finish, err := o.sandboxStore.StartRemoving(ctx, sbx.TeamID, sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionSnapshot})
			require.NoError(t, err)
			require.Equal(t, sandbox.StateSnapshotting, transition.Sandbox.State)
			probe.armed.Store(true)
			type result struct {
				resp    *proxygrpc.SandboxResumeResponse
				handled bool
				err     error
			}
			results := make(chan result, 1)
			go func() {
				resp, handled, err := o.HandleExistingSandboxAutoResume(ctx, sbx.TeamID, sbx.SandboxID, transition.Sandbox, time.Minute)
				results <- result{resp, handled, err}
			}()
			select {
			case <-probe.waiting:
			case <-ctx.Done():
				t.Fatal("auto-resume did not wait for the checkpoint")
			}

			restored := want(tt.checkpointed)
			require.NoError(t, o.restoreCheckpointed(ctx, transition, &orchestrator.SandboxCheckpointNetwork{Placement: tt.checkpointed},
				func(err error) { finish(ctx, err) }))
			got := <-results
			require.NoError(t, got.err)
			assert.True(t, got.handled)
			assert.Truef(t, proto.Equal(restored, got.resp), "want %v, got %v", restored, got.resp)
		})
	}

	t.Run("unusable full-router placement fails without an orchestrator fallback", func(t *testing.T) {
		t.Parallel()

		o := newTestAutoResumeOrchestrator(t)
		sbx := testSandboxForAutoResume(sandbox.StateRunning)
		registerNode(o, sbx, "10.0.0.1")
		sbx.NetworkPlacement = &orchestrator.SandboxNetworkPlacement{ExecutionId: sbx.ExecutionID, NetworkMode: "full-router", RouterId: "router-b", IngressAddress: "10.8.0.2"}

		resp, handled, err := o.HandleExistingSandboxAutoResume(t.Context(), sbx.TeamID, sbx.SandboxID, sbx, time.Minute)
		require.ErrorContains(t, err, "no usable router ingress")
		assert.False(t, handled)
		assert.Nil(t, resp)
	})
}
