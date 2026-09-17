package orchestrator

import (
	"context"
	"errors"
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
	e2bcatalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// pauseStubClient answers every Pause with a fixed error (nil = success).
type pauseStubClient struct {
	orchestrator.SandboxServiceClient

	err            error
	deleteErr      error
	storageDurable *bool
	stopCompleted  *bool
	// gate, when set, holds the answer until closed.
	gate <-chan struct{}
	// onPause, when set, runs before the answer — a test's chance to change
	// the record underneath the restore.
	onPause func()

	mu         sync.Mutex
	deletes    int
	lastDelete *orchestrator.SandboxDeleteRequest
	lastPause  *orchestrator.SandboxPauseRequest
}

func (c *pauseStubClient) Delete(_ context.Context, request *orchestrator.SandboxDeleteRequest, _ ...grpc.CallOption) (*orchestrator.SandboxDeleteResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deletes++
	c.lastDelete = request

	completed := request.GetWaitForStop()
	if c.stopCompleted != nil {
		completed = *c.stopCompleted
	}

	return &orchestrator.SandboxDeleteResponse{StopCompleted: completed}, c.deleteErr
}

func (c *pauseStubClient) deleteCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.deletes
}

func (c *pauseStubClient) lastDeleteRequest() *orchestrator.SandboxDeleteRequest {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.lastDelete
}

func (c *pauseStubClient) lastPauseRequest() *orchestrator.SandboxPauseRequest {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.lastPause
}

func (c *pauseStubClient) Pause(_ context.Context, request *orchestrator.SandboxPauseRequest, _ ...grpc.CallOption) (*orchestrator.SandboxPauseResponse, error) {
	c.mu.Lock()
	c.lastPause = request
	c.mu.Unlock()
	if c.gate != nil {
		<-c.gate
	}
	if c.onPause != nil {
		c.onPause()
	}
	if c.err != nil {
		return nil, c.err
	}

	durable := request.GetWaitForStorage()
	if c.storageDurable != nil {
		durable = *c.storageDurable
	}

	return &orchestrator.SandboxPauseResponse{StorageDurable: durable}, nil
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
	client   *pauseStubClient
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
	client := &pauseStubClient{err: pauseErr}
	node.SetSandboxClient(client)

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
				AddSandboxToRoutingTable: func(context.Context, sandbox.Sandbox) {},
				AsyncNewlyCreatedSandbox: func(context.Context, sandbox.Sandbox, sandbox.CreationMetadata) {},
			},
		),
		routingCatalog:     e2bcatalog.NewRedisSandboxCatalog(redisClient),
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

	return refusalFixture{o: o, recorder: recorder, sbx: sbx, reader: reader, client: client}
}

func (f refusalFixture) removePause(t *testing.T) error {
	t.Helper()

	return f.o.RemoveSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionPause})
}

// A retryable refusal with the restore flag on leaves the sandbox in service
// on a local-cluster node: record intact (Running, pre-pause expiry, not
// expired), routing re-registered, and no stopped-analytics emission.
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

	info, err := f.o.routingCatalog.GetSandbox(t.Context(), f.sbx.SandboxID)
	require.NoError(t, err, "routing must be re-registered for a local-cluster node")
	assert.Equal(t, f.sbx.ExecutionID, info.ExecutionID)

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

// failingCatalog refuses every write: the route cannot come back.
type failingCatalog struct {
	e2bcatalog.SandboxesCatalog
}

func (failingCatalog) RestoreSandbox(context.Context, string, *e2bcatalog.SandboxInfo, time.Duration) error {
	return errors.New("catalog unavailable")
}

// If the route cannot be re-registered, the restore fails closed: the record
// is removed and the caller gets today's error, never a Running record with
// no route behind it.
func TestRemoveSandbox_RefusalRouteFailureFallsBackToRemoval(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, refusedPauseErr())
	f.o.routingCatalog = failingCatalog{SandboxesCatalog: f.o.routingCatalog}

	err := f.removePause(t)
	require.ErrorIs(t, err, ErrSandboxOperationFailed)
	require.NotErrorIs(t, err, PauseQueueExhaustedError{})

	_, err = f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
	require.ErrorIs(t, err, sandbox.ErrNotFound, "a sandbox whose route cannot be restored is removed as today")
	require.Eventually(t, func() bool { return f.recorder.stoppedCount() == 1 },
		3*time.Second, 10*time.Millisecond, "and its stopped event is emitted as today")
	assert.Equal(t, map[string]int64{"route_restore_failed/request": 1}, f.restoreOutcomes(t))
}

// If the record cannot be restored after a refusal the API would have
// asked the edge to restore, the sandbox is killed on the node right away:
// the kill carries the catalog delete, so the route goes with the record
// instead of outliving it until the orphan reconciler.
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
		f.o.sandboxStore.Remove(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, f.sbx.ExecutionID)
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

// The edge answers Aborted when the node refused but the route could not be
// put back: the sandbox cannot be kept, so the record goes, the VM is killed
// now, and the outcome is counted as a failed route restore.
func TestRemoveSandbox_RefusedRouteLostRemovesAndKills(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, refusedPauseErr())
	node, ok := f.o.nodes.Get(f.o.scopedNodeID(consts.LocalClusterID, "node-1"))
	require.True(t, ok)
	stub := &pauseStubClient{err: status.Error(codes.Aborted, "failed to restore sandbox in catalog after a refused request")}
	node.SetSandboxClient(stub)

	err := f.removePause(t)
	require.ErrorIs(t, err, ErrSandboxOperationFailed)

	assert.Equal(t, 1, stub.deleteCount(), "the node is asked to kill the sandbox now, not by the reconciler later")
	_, err = f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
	require.ErrorIs(t, err, sandbox.ErrNotFound)
	assert.Equal(t, map[string]int64{"route_restore_failed/request": 1}, f.restoreOutcomes(t))
}

// On a remote-cluster node the routing catalog is the edge's: the delete rode
// as metadata on the RPC and the edge restores the entry itself on a refusal,
// so the API must not write it — the cluster guard makes it a no-op. Driven at
// the restore seam: the full pause chain persists a snapshot row whose cluster
// foreign key the test vocabulary cannot satisfy.
func TestRestoreRefusedPause_ClusterNodeSkipsCatalog(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, uuid.New(), refusedPauseErr())

	transition, _, finish, err := f.o.sandboxStore.StartRemoving(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionPause})
	require.NoError(t, err)

	require.Equal(t, restoreOutcomeRestored, f.o.restoreRefusedPause(t.Context(), transition))
	finish(t.Context(), PauseQueueExhaustedError{})

	stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, sandbox.StateRunning, stored.State)

	_, err = f.o.routingCatalog.GetSandbox(t.Context(), f.sbx.SandboxID)
	require.Error(t, err, "a cluster node's routing is the edge's to restore; the API's catalog must stay untouched")
}

// Flag off: a refusal still ends today's way — record removed, stopped
// analytics emitted, today's generic error to the caller. No 503 may promise
// a retry the record cannot honor.
func TestRemoveSandbox_RefusalFlagOffRemovesToday(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, false, consts.LocalClusterID, refusedPauseErr())

	err := f.removePause(t)
	require.ErrorIs(t, err, ErrSandboxOperationFailed)
	require.NotErrorIs(t, err, PauseQueueExhaustedError{})

	_, err = f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
	require.ErrorIs(t, err, sandbox.ErrNotFound, "flag off keeps today's removal")

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

func TestRemoveSandboxWithEvidence_ConfirmedPauseCarriesSnapshotAndRemainingLifetime(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, nil)
	evidence, err := f.o.RemoveSandboxWithEvidence(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{
		Action: sandbox.StateActionPause, ExpectExecutionID: f.sbx.ExecutionID,
	})
	require.NoError(t, err)
	assert.True(t, evidence.Confirmed)
	assert.False(t, evidence.AlreadyInProgress)
	assert.Equal(t, f.sbx.ExecutionID, evidence.ExecutionID)
	assert.NotEmpty(t, evidence.SnapshotBuildID)
	assert.InDelta(t, time.Hour.Seconds(), evidence.RemainingLifetime.Seconds(), 5)
	require.Equal(t, f.sbx.ExecutionID, f.client.lastPauseRequest().GetExecutionId())
	require.True(t, f.client.lastPauseRequest().GetWaitForStorage())
}

func TestRemoveSandboxWithEvidence_DeleteWaitsForExecutionStop(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, nil)
	evidence, err := f.o.RemoveSandboxWithEvidence(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{
		Action: sandbox.StateActionKill, ExpectExecutionID: f.sbx.ExecutionID,
	})
	require.NoError(t, err)
	require.True(t, evidence.Confirmed)
	req := f.client.lastDeleteRequest()
	require.Equal(t, f.sbx.ExecutionID, req.GetExecutionId())
	require.True(t, req.GetWaitForStop())
}

func TestRemoveSandbox_LegacyDeleteRemainsAsync(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, nil)
	require.NoError(t, f.o.RemoveSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{
		Action: sandbox.StateActionKill,
	}))
	req := f.client.lastDeleteRequest()
	require.Equal(t, f.sbx.ExecutionID, req.GetExecutionId())
	require.False(t, req.GetWaitForStop())
}

func TestRemoveSandboxWithEvidence_OlderNodeWithoutStopAcknowledgementStaysUnconfirmed(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, nil)
	node := f.o.GetNode(f.sbx.ClusterID, f.sbx.NodeID)
	require.NotNil(t, node)
	completed := false
	node.SetSandboxClient(&pauseStubClient{stopCompleted: &completed})

	evidence, err := f.o.RemoveSandboxWithEvidence(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{
		Action: sandbox.StateActionKill, ExpectExecutionID: f.sbx.ExecutionID,
	})
	require.ErrorIs(t, err, ErrSandboxOperationFailed)
	require.False(t, evidence.Confirmed)
}

func TestRemoveSandboxWithEvidence_InFlightRemovalIsNeverTerminalProof(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, nil)
	_, _, finish, err := f.o.sandboxStore.StartRemoving(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{
		Action: sandbox.StateActionKill, ExpectExecutionID: f.sbx.ExecutionID,
	})
	require.NoError(t, err)
	go func() {
		time.Sleep(50 * time.Millisecond)
		finish(context.WithoutCancel(t.Context()), nil)
	}()

	evidence, err := f.o.RemoveSandboxWithEvidence(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{
		Action: sandbox.StateActionKill, ExpectExecutionID: f.sbx.ExecutionID,
	})
	require.NoError(t, err)
	assert.False(t, evidence.Confirmed)
	assert.True(t, evidence.AlreadyInProgress)
}

func TestRemoveSandboxWithEvidence_NodeFailureAndMissingRegistryStayUnconfirmed(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, nil)
	node, ok := f.o.nodes.Get(f.o.scopedNodeID(consts.LocalClusterID, "node-1"))
	require.True(t, ok)
	node.SetSandboxClient(&pauseStubClient{deleteErr: errors.New("node transport lost")})

	evidence, err := f.o.RemoveSandboxWithEvidence(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{
		Action: sandbox.StateActionKill, ExpectExecutionID: f.sbx.ExecutionID,
	})
	require.ErrorIs(t, err, ErrSandboxOperationFailed)
	assert.False(t, evidence.Confirmed)
	_, getErr := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
	require.ErrorIs(t, getErr, sandbox.ErrNotFound, "legacy registry absence is not promoted to completion evidence")
}

func TestRemoveSandboxWithEvidence_PauseWithoutStorageConfirmationStaysUnconfirmed(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, nil)
	node, ok := f.o.nodes.Get(f.o.scopedNodeID(consts.LocalClusterID, "node-1"))
	require.True(t, ok)
	durable := false
	node.SetSandboxClient(&pauseStubClient{storageDurable: &durable})

	evidence, err := f.o.RemoveSandboxWithEvidence(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{
		Action: sandbox.StateActionPause, ExpectExecutionID: f.sbx.ExecutionID,
	})
	require.ErrorIs(t, err, ErrSandboxOperationFailed)
	assert.False(t, evidence.Confirmed)
	assert.Empty(t, evidence.SnapshotBuildID)
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
