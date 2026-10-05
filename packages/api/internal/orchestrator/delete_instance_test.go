package orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	analyticscollector "github.com/e2b-dev/infra/packages/api/internal/analytics_collector"
	"github.com/e2b-dev/infra/packages/api/internal/api"
	snapshotcache "github.com/e2b-dev/infra/packages/api/internal/cache/snapshots"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	redisreservations "github.com/e2b-dev/infra/packages/api/internal/sandbox/reservations/redis"
	sandboxredis "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// pauseStubClient answers every Pause with a fixed error (nil = success).
type pauseStubClient struct {
	orchestrator.SandboxServiceClient

	err error
	// gate, when set, holds the answer until closed.
	gate <-chan struct{}
	// onPause, when set, runs before the answer — a test's chance to change
	// the record underneath the restore.
	onPause func()

	mu      sync.Mutex
	deletes int
}

func (c *pauseStubClient) Delete(context.Context, *orchestrator.SandboxDeleteRequest, ...grpc.CallOption) (*emptypb.Empty, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deletes++

	return &emptypb.Empty{}, nil
}

func (c *pauseStubClient) deleteCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.deletes
}

func (c *pauseStubClient) Pause(_ context.Context, _ *orchestrator.SandboxPauseRequest, _ ...grpc.CallOption) (*orchestrator.SandboxPauseResponse, error) {
	if c.gate != nil {
		<-c.gate
	}
	if c.onPause != nil {
		c.onPause()
	}
	if c.err != nil {
		return nil, c.err
	}

	return &orchestrator.SandboxPauseResponse{}, nil
}

// recordingCollector counts InstanceStopped emissions — the stopped-analytics
// channel the restore must not fire.
type recordingCollector struct {
	analyticscollector.AnalyticsCollectorClient

	mu      sync.Mutex
	stopped int
}

func (r *recordingCollector) InstanceStopped(context.Context, *analyticscollector.InstanceStoppedEvent, ...grpc.CallOption) (*emptypb.Empty, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopped++

	return &emptypb.Empty{}, nil
}

func (r *recordingCollector) stoppedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.stopped
}

type refusalFixture struct {
	o        *Orchestrator
	recorder *recordingCollector
	sbx      sandbox.Sandbox
	reader   *sdkmetric.ManualReader
}

// restoreOutcomes returns the pause-refusal-restore counter by (outcome, caller).
func (f refusalFixture) restoreOutcomes(t *testing.T) map[string]int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, f.reader.Collect(t.Context(), &rm))
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.ApiOrchestratorPauseRefusalRestore) {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				var outcome, caller string
				for _, kv := range dp.Attributes.ToSlice() {
					switch string(kv.Key) {
					case "outcome":
						outcome = kv.Value.Emit()
					case "caller":
						caller = kv.Value.Emit()
					}
				}
				out[outcome+"/"+caller] += dp.Value
			}
		}
	}

	return out
}

func refusedPauseErr() error {
	return status.Error(codes.ResourceExhausted, "node is busy persisting sandbox, please retry")
}

func newRefusalFixture(t *testing.T, restoreFlag bool, clusterID uuid.UUID, pauseErr error, hooks ...redis.Hook) refusalFixture {
	t.Helper()

	db := testutils.SetupDatabase(t)
	redisClient := redis_utils.SetupInstance(t)
	for _, hook := range hooks {
		redisClient.AddHook(hook)
	}

	storage, err := sandboxredis.NewStorage(redisClient, noop.NewMeterProvider(), nil)
	require.NoError(t, err)
	go storage.Start(t.Context())
	t.Cleanup(func() { storage.Close(context.WithoutCancel(t.Context())) })

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.PauseRefusalRestoreFlag.Key()).VariationForAll(restoreFlag))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	posthog, err := analyticscollector.NewPosthogClient(t.Context(), "")
	require.NoError(t, err)

	sem, err := utils.NewAdjustableSemaphore(1)
	require.NoError(t, err)

	teamID := testutils.CreateTestTeam(t, db)
	baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)
	sourceBuildID := testutils.CreateTestBuild(t, t.Context(), db, baseTemplateID, string(types.BuildStatusUploaded))

	node := nodemanager.NewTestNode("node-1", api.NodeStatusReady, 0, 8)
	node.ClusterID = clusterID
	node.SetSandboxClient(&pauseStubClient{err: pauseErr})

	recorder := &recordingCollector{}
	reader := sdkmetric.NewManualReader()
	restoreCounter, err := telemetry.GetCounter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator"), telemetry.ApiOrchestratorPauseRefusalRestore)
	require.NoError(t, err)

	o := &Orchestrator{
		pauseRefusalRestoreCounter: restoreCounter,
		sqlcDB:                     db.SqlcClient,
		snapshotUpsertSem:          sem,
		sandboxStore: sandbox.NewStore(
			storage,
			redisreservations.NewReservationStorage(redisClient, storage.Notifier()),
			sandbox.Callbacks{
				AsyncNewlyCreatedSandbox: func(context.Context, sandbox.Sandbox, sandbox.CreationMetadata) {},
			},
		),
		featureFlagsClient: ff,
		posthogClient:      posthog,
		analytics:          analyticscollector.NewAnalyticsWithClient(recorder),
		snapshotCache:      snapshotcache.NewSnapshotCache(db.SqlcClient, redisClient),
		nodes:              smap.New[*nodemanager.Node](),
	}
	o.nodes.Insert(o.scopedNodeID(clusterID, node.ID), node)

	sbx := sandbox.Sandbox{
		SandboxID:         "sbx-" + uuid.NewString()[:8],
		TemplateID:        baseTemplateID,
		ExecutionID:       uuid.NewString(),
		TeamID:            teamID,
		BuildID:           sourceBuildID,
		BaseTemplateID:    baseTemplateID,
		MaxInstanceLength: time.Hour,
		StartTime:         time.Now(),
		EndTime:           time.Now().Add(time.Hour),
		VCpu:              2,
		RamMB:             512,
		NodeID:            node.ID,
		ClusterID:         clusterID,
		State:             sandbox.StateRunning,
	}
	require.NoError(t, o.sandboxStore.Add(t.Context(), sbx, nil))

	return refusalFixture{o: o, recorder: recorder, sbx: sbx, reader: reader}
}

func (f refusalFixture) removePause(t *testing.T) error {
	t.Helper()

	return f.o.RemoveSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionPause})
}

// A retryable refusal with the restore flag on leaves the sandbox in service
// on a local-cluster node: record intact (Running, pre-pause expiry, not
// expired) and no stopped-analytics emission.
func TestRemoveSandbox_RefusalRestoresLocalNode(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, refusedPauseErr())

	err := f.removePause(t)
	require.ErrorIs(t, err, PauseQueueExhaustedError{})

	stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
	require.NoError(t, err, "the record must survive a restored refusal")
	assert.Equal(t, sandbox.StateRunning, stored.State)
	assert.WithinDuration(t, f.sbx.EndTime, stored.EndTime, time.Second, "the restore must reinstate the expiry StartRemoving clamped")
	assert.WithinDuration(t, time.Now().Add(refusalRetryAfter), stored.RefusedUntil, time.Second, "the record carries the eviction retry window")

	expired, err := f.o.sandboxStore.ExpiredItems(t.Context())
	require.NoError(t, err)
	for _, e := range expired {
		assert.NotEqual(t, f.sbx.SandboxID, e.SandboxID, "a restored sandbox must not read as expired")
	}

	time.Sleep(300 * time.Millisecond)
	assert.Zero(t, f.recorder.stoppedCount(), "a restored sandbox must not emit stopped analytics")
	assert.Equal(t, map[string]int64{"restored/request": 1}, f.restoreOutcomes(t))

	// A retried pause later still transitions: the record is Running again.
	_, _, finish, err := f.o.sandboxStore.StartRemoving(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionPause})
	require.NoError(t, err, "a retried pause must be admissible after the restore")
	finish(t.Context(), nil)
}

// The evictor is the dominant caller, and the label the rollout is read
// through: an eviction's restore is counted as one.
func TestRemoveSandbox_RefusalRestoreCountsTheEvictionCaller(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, refusedPauseErr())
	_, err := f.o.sandboxStore.Update(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, func(s sandbox.Sandbox) (sandbox.Sandbox, error) {
		s.EndTime = time.Now().Add(-time.Minute)

		return s, nil
	})
	require.NoError(t, err)

	err = f.o.RemoveSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionPause, Eviction: true, ExpectExecutionID: f.sbx.ExecutionID})
	require.ErrorIs(t, err, PauseQueueExhaustedError{})

	stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, sandbox.StateRunning, stored.State)
	assert.WithinDuration(t, time.Now(), stored.RefusedSince, time.Second, "the first refusal is stamped for the retry budget")
	assert.Equal(t, map[string]int64{"restored/eviction": 1}, f.restoreOutcomes(t))
}

// A refused-and-restored pause tells its concurrent waiters so: a plain
// waiter gets ErrTransitionRestored (the resume endpoint turns that into
// "already running", connect re-reads), a joining pause gets the same 503.
func TestRemoveSandbox_RefusalInformsWaiters(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, refusedPauseErr())

	started := make(chan struct{})
	waiter := make(chan error, 1)
	go func() {
		for {
			sbx, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
			if err == nil && sbx.State == sandbox.StatePausing {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		close(started)
		waiter <- f.o.sandboxStore.WaitForStateChange(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
	}()

	// Hold the node's answer until the waiter is in place.
	node, ok := f.o.nodes.Get(f.o.scopedNodeID(consts.LocalClusterID, "node-1"))
	require.True(t, ok)
	node.SetSandboxClient(&pauseStubClient{err: refusedPauseErr(), gate: started})

	err := f.removePause(t)
	require.ErrorIs(t, err, PauseQueueExhaustedError{})

	select {
	case werr := <-waiter:
		require.ErrorIs(t, werr, sandbox.ErrTransitionRestored)
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not wake")
	}

	// A pause that joins after the restore is admitted afresh — the record is Running.
	_, _, finish, err := f.o.sandboxStore.StartRemoving(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionPause})
	require.NoError(t, err)
	finish(t.Context(), nil)
}

// If the record cannot be restored after a refusal, the sandbox is killed on
// the node right away: the node refused before it stopped the sandbox, so
// its route is still live and would otherwise outlive the record until the
// orphan reconciler.
func TestRemoveSandbox_FailedRestoreKillsTheRefusedSandbox(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, refusedPauseErr())
	node, ok := f.o.nodes.Get(f.o.scopedNodeID(consts.LocalClusterID, "node-1"))
	require.True(t, ok)
	stub := &pauseStubClient{err: refusedPauseErr()}
	stub.onPause = func() {
		// Move the record off Pausing underneath the pause so RestoreRunning refuses.
		_, err := f.o.sandboxStore.Update(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, func(s sandbox.Sandbox) (sandbox.Sandbox, error) {
			s.State = sandbox.StateKilling

			return s, nil
		})
		require.NoError(t, err)
	}
	node.SetSandboxClient(stub)

	err := f.removePause(t)
	require.ErrorIs(t, err, ErrSandboxOperationFailed)

	assert.Equal(t, 1, stub.deleteCount(), "the node is asked to kill the sandbox now, not by the reconciler later")
	_, err = f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
	require.ErrorIs(t, err, sandbox.ErrNotFound)
	assert.Equal(t, map[string]int64{"restore_failed/request": 1}, f.restoreOutcomes(t))
}

// A refused pause whose record was removed and the ID reclaimed by a new
// incarnation while the RPC was in flight must leave that incarnation alone:
// its record is not rewritten with the stale one, not removed, and its VM is
// not killed on the node.
func TestRemoveSandbox_SupersededRefusalLeavesNewIncarnationAlone(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, refusedPauseErr())
	node, ok := f.o.nodes.Get(f.o.scopedNodeID(consts.LocalClusterID, "node-1"))
	require.True(t, ok)

	resumed := f.sbx
	resumed.ExecutionID = uuid.NewString()
	resumed.MaxInstanceLength = 3 * time.Hour
	resumed.EndTime = time.Now().Add(2 * time.Hour)

	stub := &pauseStubClient{err: refusedPauseErr()}
	stub.onPause = func() {
		f.o.sandboxStore.Remove(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
		require.NoError(t, f.o.sandboxStore.Add(t.Context(), resumed, nil))
	}
	node.SetSandboxClient(stub)

	err := f.removePause(t)
	require.ErrorIs(t, err, ErrSandboxNotFound, "the caller's sandbox is gone, as when it was removed before the pause")
	require.ErrorIs(t, err, sandbox.ErrExecutionMismatch)
	require.NotErrorIs(t, err, PauseQueueExhaustedError{})

	stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
	require.NoError(t, err, "the incarnation that reclaimed the ID must survive")
	assert.Equal(t, resumed.ExecutionID, stored.ExecutionID)
	assert.Equal(t, sandbox.StateRunning, stored.State)
	assert.WithinDuration(t, resumed.EndTime, stored.EndTime, time.Second, "its expiry is its own, not the stale rollback's")
	assert.True(t, stored.RefusedUntil.IsZero(), "the refusal is the old incarnation's, not stamped on the new one")

	assert.Zero(t, stub.deleteCount(), "the node must not be asked to kill the sandbox that now owns the ID")
	time.Sleep(200 * time.Millisecond)
	assert.Zero(t, f.recorder.stoppedCount(), "no stopped event for a removal someone else already completed")
	assert.Equal(t, map[string]int64{"superseded/request": 1}, f.restoreOutcomes(t))
}

// Flag off: a refusal still ends today's way — record removed, stopped
// analytics emitted, today's generic error to the caller. No 503 may promise
// a retry the record cannot honor. The refused VM is still live on the node,
// so it is killed with the record.
func TestRemoveSandbox_RefusalFlagOffRemovesToday(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, false, consts.LocalClusterID, refusedPauseErr())
	node, ok := f.o.nodes.Get(f.o.scopedNodeID(consts.LocalClusterID, "node-1"))
	require.True(t, ok)
	stub := &pauseStubClient{err: refusedPauseErr()}
	node.SetSandboxClient(stub)

	err := f.removePause(t)
	require.ErrorIs(t, err, ErrSandboxOperationFailed)
	require.NotErrorIs(t, err, PauseQueueExhaustedError{})

	_, err = f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
	require.ErrorIs(t, err, sandbox.ErrNotFound, "flag off keeps today's removal")
	assert.Equal(t, 1, stub.deleteCount(), "the refused VM goes with its record, not with the reconciler later")

	require.Eventually(t, func() bool { return f.recorder.stoppedCount() == 1 },
		3*time.Second, 10*time.Millisecond, "flag off keeps today's stopped-analytics emission")
}

// A successful pause removes and emits exactly as before, flag on or not.
func TestRemoveSandbox_SuccessRemovesAndEmits(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, nil)

	require.NoError(t, f.removePause(t))

	_, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
	require.ErrorIs(t, err, sandbox.ErrNotFound)

	require.Eventually(t, func() bool { return f.recorder.stoppedCount() == 1 },
		3*time.Second, 10*time.Millisecond)
}

// A fatal (non-retryable) pause failure removes and emits exactly as before.
func TestRemoveSandbox_FatalFailureRemovesAndEmits(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, status.Error(codes.Internal, "snapshot failed"))

	err := f.removePause(t)
	require.ErrorIs(t, err, ErrSandboxOperationFailed)

	_, err = f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
	require.ErrorIs(t, err, sandbox.ErrNotFound)

	require.Eventually(t, func() bool { return f.recorder.stoppedCount() == 1 },
		3*time.Second, 10*time.Millisecond)
}

// The Pausing→Running edge exists for exactly one reason — the refusal
// restore — and Pausing gains no other exit.
func TestPausingTransitionsPinRestoreEdge(t *testing.T) {
	t.Parallel()

	assert.Equal(t,
		map[sandbox.State]bool{sandbox.StateKilling: true, sandbox.StateRunning: true},
		sandbox.AllowedTransitions[sandbox.StatePausing])
}
