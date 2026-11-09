package orchestrator

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	"github.com/google/uuid"
	protovalidatemw "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/protovalidate"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	analyticscollector "github.com/e2b-dev/infra/packages/api/internal/analytics_collector"
	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	redisreservations "github.com/e2b-dev/infra/packages/api/internal/sandbox/reservations/redis"
	sandboxredis "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	placementpb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/sandbox/placement"
	workerpb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	catalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

const placementCaller = "spiffe://test.internal/ns/platform/sa/coordinator"

// placementClient serves the service as the API does, validated, with peer
// as the caller. A set lose drops the next successful reply after the
// handler applied it.
func placementClient(t *testing.T, service *SandboxPlacementService, peer mtls.Peer, lose *atomic.Bool) placementpb.SandboxPlacementServiceClient {
	t.Helper()
	validator, err := protovalidate.New()
	require.NoError(t, err)
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		result, err := next(mtls.ContextWithPeer(ctx, peer), req)
		if err == nil && lose != nil && lose.Swap(false) {
			return nil, status.Error(codes.Unavailable, "lost reply after application")
		}

		return result, err
	}, protovalidatemw.UnaryServerInterceptor(validator)))
	placementpb.RegisterSandboxPlacementServiceServer(server, service)
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return placementpb.NewSandboxPlacementServiceClient(conn)
}

type placementHarness struct {
	o              *Orchestrator
	store          *sandbox.Store
	source, target *nodemanager.Node
	stops          *recordingCollector
}

// localPlacementHarness is an Orchestrator over a real store on Redis with
// two local nodes on the same CPU, and the service authorizing one caller.
func localPlacementHarness(t *testing.T) (*placementHarness, *SandboxPlacementService) {
	t.Helper()
	ctx := t.Context()
	redisClient := redisutils.SetupInstance(t)
	storage, err := sandboxredis.NewStorage(redisClient, noop.NewMeterProvider())
	require.NoError(t, err)
	go storage.Start(ctx)
	t.Cleanup(func() { storage.Close(context.WithoutCancel(ctx)) })
	store := sandbox.NewStore(storage, redisreservations.NewReservationStorage(redisClient, storage.Notifier()), sandbox.Callbacks{})
	flags, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	t.Cleanup(func() { _ = flags.Close(context.WithoutCancel(ctx)) })
	posthog, err := analyticscollector.NewPosthogClient(ctx, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = posthog.Close() })
	stops := &recordingCollector{}
	o := &Orchestrator{sandboxStore: store, nodes: smap.New[*nodemanager.Node](), featureFlagsClient: flags, posthogClient: posthog, analytics: analyticscollector.NewAnalyticsWithClient(stops)}
	h := &placementHarness{o: o, store: store, stops: stops}
	h.source = h.node("node-a", api.NodeStatusReady, nodemanager.WithServiceInstanceID(uuid.NewString()))
	h.target = h.node("node-b", api.NodeStatusReady, nodemanager.WithServiceInstanceID(uuid.NewString()))
	callers, err := mtls.ParseAllowList([]string{placementCaller}, mtls.WithoutWildcards())
	require.NoError(t, err)

	return h, NewSandboxPlacementService(h.o, callers)
}

func (h *placementHarness) node(id string, status api.NodeStatus, opts ...nodemanager.TestOptions) *nodemanager.Node {
	node := nodemanager.NewTestNode(id, status, 0, 8, append([]nodemanager.TestOptions{nodemanager.WithCPUInfo("x86_64", "6", "106")}, opts...)...)
	node.ClusterID = consts.LocalClusterID
	h.o.registerNode(node)

	return node
}

func (h *placementHarness) running(t *testing.T) sandbox.Sandbox {
	t.Helper()

	return h.runningWith(t, func(*sandbox.Sandbox) {})
}

// runningWith stores a running Full-Router execution on the source.
func (h *placementHarness) runningWith(t *testing.T, change func(*sandbox.Sandbox)) sandbox.Sandbox {
	t.Helper()

	executionID := uuid.NewString()
	sbx := sandbox.Sandbox{
		SandboxID:         "sbx" + uuid.NewString()[:8],
		TemplateID:        "template",
		TeamID:            uuid.New(),
		ExecutionID:       executionID,
		NodeID:            h.source.ID,
		ClusterID:         consts.LocalClusterID,
		StartTime:         time.Now().Add(-time.Hour),
		EndTime:           time.Now().Add(time.Hour),
		MaxInstanceLength: 24 * time.Hour,
		State:             sandbox.StateRunning,
		NetworkPlacement: &workerpb.SandboxNetworkPlacement{
			ExecutionId:    executionID,
			NetworkMode:    string(catalog.NetworkModeFullRouter),
			RouterId:       uuid.NewString(),
			IngressAddress: "10.0.0.1",
			IngressPort:    5007,
		},
	}
	change(&sbx)
	require.NoError(t, h.store.Add(t.Context(), sbx, nil))

	return sbx
}

func (h *placementHarness) get(t *testing.T, sbx sandbox.Sandbox) sandbox.Sandbox {
	t.Helper()

	stored, err := h.store.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)

	return stored
}

func executionHoldOf(sbx sandbox.Sandbox) *placementpb.ExecutionHold {
	return placementpb.ExecutionHold_builder{TeamId: sbx.TeamID.String(), SandboxId: sbx.SandboxID, ExecutionId: sbx.ExecutionID, OperationId: uuid.NewString(), ExpectedSourceNodeId: sbx.NodeID}.Build()
}

func holdRequest(hold *placementpb.ExecutionHold) *placementpb.HoldExecutionRequest {
	return holdRequestUntil(hold, time.Now().Add(time.Minute).Truncate(time.Millisecond))
}

func holdRequestUntil(hold *placementpb.ExecutionHold, until time.Time) *placementpb.HoldExecutionRequest {
	return placementpb.HoldExecutionRequest_builder{Hold: hold, HoldUntil: timestamppb.New(until)}.Build()
}

func resolveRequest(hold *placementpb.ExecutionHold, action placementpb.ResolutionAction, target string) *placementpb.ResolveExecutionHoldRequest {
	return placementpb.ResolveExecutionHoldRequest_builder{Hold: hold, Action: action, TargetNodeId: target}.Build()
}

// A move off a shutting-down node admits a ready target, holds the execution
// against ordinary lifecycle changes, and settles placement on the target.
// Repeating a call after a lost reply is safe.
func TestPlacementMove(t *testing.T) {
	t.Parallel()
	h, service := localPlacementHarness(t)
	h.source = h.node(h.source.ID, api.NodeStatusShuttingDown, nodemanager.WithServiceInstanceID(uuid.NewString()))
	sbx := h.running(t)
	var lose atomic.Bool
	client := placementClient(t, service, mtls.Peer{ID: placementCaller, ChainVerified: true}, &lose)
	ctx := t.Context()

	placement, err := client.GetSandboxPlacement(ctx, placementpb.GetSandboxPlacementRequest_builder{TeamId: sbx.TeamID.String(), SandboxId: sbx.SandboxID, ExpectedExecutionId: sbx.ExecutionID, ExpectedSourceNodeId: sbx.NodeID, TargetNodeId: h.target.ID}.Build())
	require.NoError(t, err)
	assert.Equal(t, h.source.Metadata().ServiceInstanceID, placement.GetSource().GetServiceInstanceId())
	assert.Equal(t, h.target.IPAddress, placement.GetTarget().GetIpAddress())
	assert.Equal(t, sbx.NetworkPlacement.GetRouterId(), placement.GetRouterId())
	assert.True(t, placement.HasVolumesAttached(), "an API that knows the field always reports it")
	assert.False(t, placement.GetVolumesAttached())

	hold := executionHoldOf(sbx)
	lose.Store(true)
	_, err = client.HoldExecution(ctx, holdRequest(hold))
	require.Equal(t, codes.Unavailable, status.Code(err))
	assert.Equal(t, hold.GetOperationId(), h.get(t, sbx).LifecycleHold, "the hold applied before the reply was lost")
	_, err = client.HoldExecution(ctx, holdRequest(hold))
	require.NoError(t, err, "the same hold again")
	_, started, _, err := h.store.StartRemoving(ctx, sbx.TeamID, sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionKill})
	require.ErrorIs(t, err, sandbox.ErrLifecycleHeld)
	assert.False(t, started)
	_, err = client.HoldExecution(ctx, holdRequest(executionHoldOf(sbx)))
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "another operation's hold")

	move := resolveRequest(hold, placementpb.ResolutionAction_RESOLUTION_ACTION_MOVE, h.target.ID)
	lose.Store(true)
	_, err = client.ResolveExecutionHold(ctx, move)
	require.Equal(t, codes.Unavailable, status.Code(err))
	moved := h.get(t, sbx)
	assert.Equal(t, h.target.ID, moved.NodeID)
	assert.Empty(t, moved.LifecycleHold)
	assert.True(t, sbx.EndTime.Equal(moved.EndTime), "only the node and the hold changed")
	_, err = client.ResolveExecutionHold(ctx, move)
	require.NoError(t, err, "the move again finds its own move")
	_, err = client.ResolveExecutionHold(ctx, resolveRequest(hold, placementpb.ResolutionAction_RESOLUTION_ACTION_MOVE, "node-elsewhere"))
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "a move to another node is not this one")
	_, err = client.HoldExecution(ctx, holdRequest(hold))
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "the hold names a placement the execution has left")
	_, err = client.ResolveExecutionHold(ctx, resolveRequest(hold, placementpb.ResolutionAction_RESOLUTION_ACTION_MOVE, ""))
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = client.HoldExecution(ctx, &placementpb.HoldExecutionRequest{})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// A deadline is bounded by the team's migration budgets and carries
// milliseconds at most, so a caller cannot keep a record unkillable for
// longer than the flags allow.
func TestPlacementHoldDeadline(t *testing.T) {
	t.Parallel()
	h, service := localPlacementHarness(t)
	sbx := h.running(t)
	client := placementClient(t, service, mtls.Peer{ID: placementCaller, ChainVerified: true}, nil)
	ctx := t.Context()
	flags := h.o.featureFlagsClient
	budget := time.Duration(flags.IntFlag(ctx, featureflags.SandboxMigrationOperationBudgetSeconds)+flags.IntFlag(ctx, featureflags.SandboxMigrationTerminationBudgetSeconds)) * time.Second

	_, err := client.HoldExecution(ctx, holdRequestUntil(executionHoldOf(sbx), time.Now().Add(budget+2*holdBudgetSlack).Truncate(time.Millisecond)))
	require.Equal(t, codes.InvalidArgument, status.Code(err), "past the budgets")
	_, err = client.HoldExecution(ctx, holdRequestUntil(executionHoldOf(sbx), time.Now().Add(time.Minute).Truncate(time.Millisecond).Add(time.Microsecond)))
	require.Equal(t, codes.InvalidArgument, status.Code(err), "finer than a millisecond")
	_, err = client.HoldExecution(ctx, holdRequestUntil(executionHoldOf(sbx), time.Now().Add(-time.Second).Truncate(time.Millisecond)))
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "already passed")
	assert.Empty(t, h.get(t, sbx).LifecycleHold)
	_, err = client.HoldExecution(ctx, holdRequestUntil(executionHoldOf(sbx), time.Now().Add(budget).Truncate(time.Millisecond)))
	require.NoError(t, err, "within the budgets")
}

// A release leaves the record where it is, and an end removes it, reports
// the stop and releases the reservation once: the repeats find nothing to
// do and must not release a successor's pending start.
func TestPlacementReleaseAndEnd(t *testing.T) {
	t.Parallel()
	h, service := localPlacementHarness(t)
	ctx := t.Context()
	client := placementClient(t, service, mtls.Peer{ID: placementCaller, ChainVerified: true}, nil)

	released := h.running(t)
	hold := executionHoldOf(released)
	_, err := client.HoldExecution(ctx, holdRequest(hold))
	require.NoError(t, err)
	release := resolveRequest(hold, placementpb.ResolutionAction_RESOLUTION_ACTION_RELEASE, "")
	_, err = client.ResolveExecutionHold(ctx, release)
	require.NoError(t, err)
	stored := h.get(t, released)
	assert.Empty(t, stored.LifecycleHold)
	assert.Equal(t, released.NodeID, stored.NodeID)
	_, err = client.ResolveExecutionHold(ctx, release)
	require.NoError(t, err, "the release again finds the record free")

	ended := h.running(t)
	hold = executionHoldOf(ended)
	end := resolveRequest(hold, placementpb.ResolutionAction_RESOLUTION_ACTION_END, "")
	_, err = client.HoldExecution(ctx, holdRequest(hold))
	require.NoError(t, err)
	_, err = client.ResolveExecutionHold(ctx, end)
	require.NoError(t, err)
	_, err = h.store.Get(ctx, ended.TeamID, ended.SandboxID)
	require.ErrorIs(t, err, sandbox.ErrNotFound)
	require.Eventually(t, func() bool { return h.stops.stoppedCount() == 1 }, time.Second, time.Millisecond)
	finish, wait, err := h.store.Reserve(ctx, ended.TeamID, ended.SandboxID, 10)
	require.NoError(t, err)
	require.NotNil(t, finish)
	require.Nil(t, wait)
	_, err = client.ResolveExecutionHold(ctx, end)
	require.NoError(t, err, "the end again finds the record gone")
	finishAgain, waitAgain, err := h.store.Reserve(ctx, ended.TeamID, ended.SandboxID, 10)
	require.NoError(t, err)
	assert.Nil(t, finishAgain, "the end again releases no successor's pending start")
	assert.NotNil(t, waitAgain)
	assert.Equal(t, 1, h.stops.stoppedCount(), "the end again reports no second stop")
	finish(ended, nil)
}

// An end after the hold lapsed ends the record through the ordinary kill
// while it still runs the held execution on the source, and leaves a record
// that moved or changed execution since alone.
func TestPlacementEndAfterTheHoldLapsed(t *testing.T) {
	t.Parallel()
	h, service := localPlacementHarness(t)
	ctx := t.Context()
	client := placementClient(t, service, mtls.Peer{ID: placementCaller, ChainVerified: true}, nil)
	h.source.SetSandboxClient(&placementWorkerClient{})

	lapsed := h.running(t)
	hold := h.lapsedHold(t, lapsed)
	end := resolveRequest(hold, placementpb.ResolutionAction_RESOLUTION_ACTION_END, "")
	_, err := client.ResolveExecutionHold(ctx, end)
	require.NoError(t, err)
	_, err = h.store.Get(ctx, lapsed.TeamID, lapsed.SandboxID)
	require.ErrorIs(t, err, sandbox.ErrNotFound, "the ordinary kill ended the record")
	require.Eventually(t, func() bool { return h.stops.stoppedCount() == 1 }, time.Second, time.Millisecond)
	_, err = client.ResolveExecutionHold(ctx, end)
	require.NoError(t, err, "the end again finds the record gone")

	moved := h.running(t)
	hold = h.lapsedHold(t, moved)
	later := executionHoldOf(moved)
	_, err = client.HoldExecution(ctx, holdRequest(later))
	require.NoError(t, err)
	_, err = client.ResolveExecutionHold(ctx, resolveRequest(later, placementpb.ResolutionAction_RESOLUTION_ACTION_MOVE, h.target.ID))
	require.NoError(t, err)
	_, err = client.ResolveExecutionHold(ctx, resolveRequest(hold, placementpb.ResolutionAction_RESOLUTION_ACTION_END, ""))
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "a later operation moved the execution")
	assert.Equal(t, h.target.ID, h.get(t, moved).NodeID, "the record stays on the target")
	assert.Equal(t, 1, h.stops.stoppedCount())
}

// lapsedHold writes a hold of sbx whose deadline has passed, as a hold the
// store took earlier looks once its deadline lapsed, and returns it.
func (h *placementHarness) lapsedHold(t *testing.T, sbx sandbox.Sandbox) *placementpb.ExecutionHold {
	t.Helper()

	hold := executionHoldOf(sbx)
	_, err := h.store.Update(t.Context(), sbx.TeamID, sbx.SandboxID, func(s sandbox.Sandbox) (sandbox.Sandbox, error) {
		s.LifecycleHold, s.LifecycleHoldUntil = hold.GetOperationId(), time.Now().Add(-time.Minute).UnixMilli()

		return s, nil
	})
	require.NoError(t, err)

	return hold
}

// placementWorkerClient answers the kill of a guest the worker stopped.
type placementWorkerClient struct {
	workerpb.SandboxServiceClient
}

func (placementWorkerClient) Delete(context.Context, *workerpb.SandboxDeleteRequest, ...grpc.CallOption) (*emptypb.Empty, error) {
	return nil, status.Error(codes.NotFound, "no such sandbox")
}

func TestPlacementCaller(t *testing.T) {
	t.Parallel()
	for _, peer := range []mtls.Peer{{}, {ID: placementCaller}, {ID: "spiffe://test.internal/ns/platform/sa/client-proxy", ChainVerified: true}} {
		t.Run(peer.ID+fmt.Sprint(peer.ChainVerified), func(t *testing.T) {
			t.Parallel()
			callers, err := mtls.ParseAllowList([]string{placementCaller}, mtls.WithoutWildcards())
			require.NoError(t, err)
			client := placementClient(t, NewSandboxPlacementService(nil, callers), peer, nil)
			_, err = client.GetSandboxPlacement(t.Context(), placementpb.GetSandboxPlacementRequest_builder{TeamId: uuid.NewString(), SandboxId: "sbx", ExpectedExecutionId: uuid.NewString(), ExpectedSourceNodeId: "node-a", TargetNodeId: "node-b"}.Build())
			expected := codes.Unauthenticated
			if peer.ChainVerified {
				expected = codes.PermissionDenied
			}
			require.Equal(t, expected, status.Code(err))
		})
	}
}

// The placement reports attached volumes from the stored record and leaves
// the decision to the caller.
func TestPlacementReportsVolumeMounts(t *testing.T) {
	t.Parallel()
	h, service := localPlacementHarness(t)
	sbx := h.runningWith(t, func(s *sandbox.Sandbox) {
		s.VolumeMounts = []*types.SandboxVolumeMountConfig{{ID: uuid.NewString(), Type: "persistent", Name: "data", Path: "/data"}}
	})
	client := placementClient(t, service, mtls.Peer{ID: placementCaller, ChainVerified: true}, nil)

	placement, err := client.GetSandboxPlacement(t.Context(), placementpb.GetSandboxPlacementRequest_builder{TeamId: sbx.TeamID.String(), SandboxId: sbx.SandboxID, ExpectedExecutionId: sbx.ExecutionID, ExpectedSourceNodeId: sbx.NodeID, TargetNodeId: h.target.ID}.Build())
	require.NoError(t, err)
	assert.True(t, placement.GetVolumesAttached())
}

// Every refusal names its reason: the caller shows it to the operator.
func TestPlacementRefusals(t *testing.T) {
	t.Parallel()
	for change, want := range map[string]struct {
		code    codes.Code
		message string
	}{
		"source":               {codes.FailedPrecondition, "placement changed"},
		"execution":            {codes.FailedPrecondition, "placement changed"},
		"remote":               {codes.FailedPrecondition, "local cluster"},
		"unknown":              {codes.NotFound, "unknown"},
		"unhealthy source":     {codes.FailedPrecondition, "is unhealthy"},
		"draining target":      {codes.FailedPrecondition, "is draining"},
		"shutting down target": {codes.FailedPrecondition, "is shutting_down"},
		"cpu":                  {codes.FailedPrecondition, "CPU differs"},
	} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			h, service := localPlacementHarness(t)
			sbx := h.runningWith(t, func(s *sandbox.Sandbox) {
				if change == "remote" {
					s.ClusterID = uuid.New()
				}
			})
			req := placementpb.GetSandboxPlacementRequest_builder{TeamId: sbx.TeamID.String(), SandboxId: sbx.SandboxID, ExpectedExecutionId: sbx.ExecutionID, ExpectedSourceNodeId: sbx.NodeID, TargetNodeId: h.target.ID}.Build()
			switch change {
			case "source":
				req.SetExpectedSourceNodeId("old")
			case "execution":
				req.SetExpectedExecutionId(uuid.NewString())
			case "unknown":
				req.SetTargetNodeId("node-" + uuid.NewString())
			case "unhealthy source":
				h.node(h.source.ID, api.NodeStatusUnhealthy, nodemanager.WithServiceInstanceID(uuid.NewString()))
			case "draining target":
				h.node(h.target.ID, api.NodeStatusDraining, nodemanager.WithServiceInstanceID(uuid.NewString()))
			case "shutting down target":
				h.node(h.target.ID, api.NodeStatusShuttingDown, nodemanager.WithServiceInstanceID(uuid.NewString()))
			case "cpu":
				h.target = h.node("node-b", api.NodeStatusReady, nodemanager.WithCPUInfo("x86_64", "6", "999"), nodemanager.WithServiceInstanceID(uuid.NewString()))
			}
			client := placementClient(t, service, mtls.Peer{ID: placementCaller, ChainVerified: true}, nil)
			_, err := client.GetSandboxPlacement(t.Context(), req)
			require.Equal(t, want.code, status.Code(err), err)
			assert.Contains(t, status.Convert(err).Message(), want.message)
		})
	}
}

type placementWorker struct {
	workerpb.UnimplementedSandboxServiceServer

	items []*workerpb.RunningSandbox
}

func (w *placementWorker) List(context.Context, *emptypb.Empty) (*workerpb.SandboxListResponse, error) {
	return &workerpb.SandboxListResponse{Sandboxes: w.items}, nil
}

// The node read lists what the node and the store agree is running on it:
// a stale execution, an ended guest, a record placed elsewhere and a guest
// without a record are left out, a held record carries its deadline, and a
// node that cannot be read fails the list.
func TestAdminNodeSandboxes(t *testing.T) {
	t.Parallel()
	h, _ := localPlacementHarness(t)
	ctx := t.Context()
	sbx := h.running(t)
	stale := h.running(t)
	ended := h.running(t)
	elsewhere := h.runningWith(t, func(s *sandbox.Sandbox) { s.NodeID = h.target.ID })
	held := h.running(t)
	volume := h.runningWith(t, func(s *sandbox.Sandbox) {
		s.VolumeMounts = []*types.SandboxVolumeMountConfig{{ID: uuid.NewString(), Type: "persistent", Name: "data", Path: "/data"}}
	})
	legacy := h.runningWith(t, func(s *sandbox.Sandbox) { s.NetworkPlacement = nil })
	until := time.Now().Add(time.Minute).Truncate(time.Millisecond)
	require.NoError(t, h.store.HoldExecution(ctx, sandbox.Hold{TeamID: held.TeamID, SandboxID: held.SandboxID, ExecutionID: held.ExecutionID, SourceNodeID: held.NodeID, ID: uuid.NewString()}, until))
	w := &placementWorker{}
	for _, s := range []sandbox.Sandbox{sbx, stale, ended, elsewhere, held, volume, legacy} {
		w.items = append(w.items, &workerpb.RunningSandbox{SandboxId: s.SandboxID, TeamId: s.TeamID.String(), ExecutionId: s.ExecutionID})
	}
	w.items[1].ExecutionId = uuid.NewString()
	w.items[2].Ended = true
	w.items[2].DiskKept = true
	w.items = append(w.items, &workerpb.RunningSandbox{SandboxId: "absent", TeamId: uuid.NewString(), ExecutionId: uuid.NewString()})
	server := grpc.NewServer()
	workerpb.RegisterSandboxServiceServer(server, w)
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	h.source.SetSandboxClient(workerpb.NewSandboxServiceClient(conn))

	_, err = h.o.AdminNodeSandboxes(ctx, consts.LocalClusterID, "node-"+uuid.NewString())
	require.ErrorIs(t, err, ErrNodeNotFound)
	listed, err := h.o.AdminNodeSandboxes(ctx, consts.LocalClusterID, h.source.ID)
	require.NoError(t, err)
	assert.Equal(t, h.source.ID, listed.NodeID)
	require.Len(t, listed.Items, 4)
	assert.Equal(t, api.NodeSandbox{
		TeamID: sbx.TeamID, SandboxID: sbx.SandboxID, ExecutionID: uuid.MustParse(sbx.ExecutionID),
		NetworkMode: sbx.NetworkPlacement.GetNetworkMode(), VolumesAttached: false,
	}, listed.Items[0])
	assert.Equal(t, held.SandboxID, listed.Items[1].SandboxID)
	assert.Equal(t, until.UnixMilli(), listed.Items[1].LifecycleHoldUntil.UnixMilli())
	assert.Equal(t, volume.SandboxID, listed.Items[2].SandboxID)
	assert.True(t, listed.Items[2].VolumesAttached)
	assert.Equal(t, legacy.SandboxID, listed.Items[3].SandboxID)
	assert.Equal(t, "legacy", listed.Items[3].NetworkMode, "a record without a Router placement runs on the legacy network")
	server.Stop()
	_, err = h.o.AdminNodeSandboxes(ctx, consts.LocalClusterID, h.source.ID)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrNodeNotFound)
}
