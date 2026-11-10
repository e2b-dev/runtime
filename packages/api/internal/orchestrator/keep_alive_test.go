package orchestrator

import (
	"context"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	redisreservations "github.com/e2b-dev/infra/packages/api/internal/sandbox/reservations/redis"
	sandboxredis "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	infogrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func TestGetMaxTTLNormal(t *testing.T) {
	t.Parallel()
	now := time.Now()
	ttl := getMaxAllowedTTL(now, now, 2*time.Hour, 3*time.Hour)
	if ttl != 2*time.Hour {
		t.Fatalf("expected 2 hours, got %v", ttl)
	}
}

func TestGetMaxTTLMax(t *testing.T) {
	t.Parallel()
	now := time.Now()
	ttl := getMaxAllowedTTL(now, now, 4*time.Hour, 3*time.Hour)
	if ttl != 3*time.Hour {
		t.Fatalf("expected 3 hours, got %v", ttl)
	}
}

func TestGetMaxTTLExpired(t *testing.T) {
	t.Parallel()
	now := time.Now()
	ttl := getMaxAllowedTTL(now, now.Add(-2*time.Hour), 4*time.Hour, time.Hour)
	if ttl != 0 {
		t.Fatalf("expected 0 hours, got %v", ttl)
	}
}

// The expiry sweep reads this index, so it is asserted alongside the record.
const sandboxExpirationIndexKey = "sandbox:storage:global:expiration"

const keepAliveFailedMsg = "Error when setting sandbox timeout"

// keepAliveNodeClient applies every Update and then answers with err.
type keepAliveNodeClient struct {
	orchestrator.SandboxServiceClient

	err        error
	calls      atomic.Int64
	appliedEnd atomic.Int64
}

func (c *keepAliveNodeClient) Update(_ context.Context, req *orchestrator.SandboxUpdateRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	c.calls.Add(1)
	c.appliedEnd.Store(req.GetEndTime().AsTime().UnixMilli())
	if c.err != nil {
		return nil, c.err
	}

	return &emptypb.Empty{}, nil
}

func newKeepAliveReplica(t *testing.T, rc redis.UniversalClient, discovery servicediscovery.Discoverer) *Orchestrator {
	t.Helper()

	storage, err := sandboxredis.NewStorage(rc, noop.NewMeterProvider())
	require.NoError(t, err)
	go storage.Start(t.Context())
	t.Cleanup(func() { storage.Close(context.WithoutCancel(t.Context())) })

	return &Orchestrator{
		nodes: smap.New[*nodemanager.Node](),
		sandboxStore: sandbox.NewStore(
			storage,
			redisreservations.NewReservationStorage(rc, storage.Notifier()),
			sandbox.Callbacks{
				AsyncNewlyCreatedSandbox: func(context.Context, sandbox.Sandbox, sandbox.CreationMetadata) {},
			},
		),
		nodeDiscovery: discovery,
		clusters:      clusters.NewTestPool(),
		tel:           telemetry.NewNoopClient(),
		nodeCreds:     insecure.NewCredentials(),
	}
}

func newKeepAliveNode(nodeID string, client orchestrator.SandboxServiceClient) *nodemanager.Node {
	node := nodemanager.NewTestNode(nodeID, api.NodeStatusReady, 0, 8)
	node.ClusterID = consts.LocalClusterID
	node.SetSandboxClient(client)

	return node
}

func addKeepAliveNode(o *Orchestrator, nodeID string, client orchestrator.SandboxServiceClient) {
	o.nodes.Insert(o.scopedNodeID(consts.LocalClusterID, nodeID), newKeepAliveNode(nodeID, client))
}

func addKeepAliveSandbox(t *testing.T, o *Orchestrator, nodeID string, endsIn time.Duration) sandbox.Sandbox {
	t.Helper()

	now := time.Now()
	sbx := sandbox.Sandbox{
		SandboxID:         "sbx-" + uuid.NewString()[:8],
		TemplateID:        "template",
		ExecutionID:       uuid.NewString(),
		TeamID:            uuid.New(),
		MaxInstanceLength: 24 * time.Hour,
		StartTime:         now,
		EndTime:           now.Add(endsIn),
		VCpu:              2,
		RamMB:             512,
		NodeID:            nodeID,
		ClusterID:         consts.LocalClusterID,
		State:             sandbox.StateRunning,
	}
	require.NoError(t, o.sandboxStore.Add(t.Context(), sbx, nil))

	return sbx
}

// expiryState returns the stored EndTime and the expiration index score.
func expiryState(t *testing.T, o *Orchestrator, rc redis.UniversalClient, sbx sandbox.Sandbox) (time.Time, time.Time) {
	t.Helper()

	got, err := o.sandboxStore.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)

	member := redis_utils.CreateKey(sbx.TeamID.String(), sbx.SandboxID, sbx.ExecutionID)
	score, err := rc.ZScore(t.Context(), sandboxExpirationIndexKey, member).Result()
	require.NoError(t, err)

	return got.EndTime, time.UnixMilli(int64(score))
}

func requireExpiryUnchanged(t *testing.T, o *Orchestrator, rc redis.UniversalClient, sbx sandbox.Sandbox, want time.Time) {
	t.Helper()

	stored, indexed := expiryState(t, o, rc, sbx)
	assert.WithinDuration(t, want, stored, time.Millisecond)
	assert.WithinDuration(t, want, indexed, time.Millisecond)
}

func requireRefusedKeepAlive(t *testing.T, apiErr *api.APIError) {
	t.Helper()

	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusInternalServerError, apiErr.Code)
	assert.Equal(t, keepAliveFailedMsg, apiErr.ClientMsg)
	require.ErrorIs(t, apiErr.Err, errNodeNotInPool)
}

func TestKeepAliveFor_NodeNotInPoolLeavesExpiryUntouched(t *testing.T) {
	t.Parallel()

	rc := redis_utils.SetupInstance(t)
	discovery := &countingDiscovery{}
	o := newKeepAliveReplica(t, rc, discovery)
	// The node the record points at exists but was dropped from this replica's pool.
	client := &keepAliveNodeClient{}
	dropped := newKeepAliveNode("dropped-node", client)
	sbx := addKeepAliveSandbox(t, o, dropped.ID, 30*time.Second)

	for range 5 {
		_, apiErr := o.KeepAliveFor(t.Context(), sbx.TeamID, sbx.SandboxID, time.Hour, false)
		requireRefusedKeepAlive(t, apiErr)
	}

	requireExpiryUnchanged(t, o, rc, sbx, sbx.EndTime)
	assert.Zero(t, client.calls.Load())
	assert.Equal(t, int64(5), discovery.calls.Load(), "every refused call consults on-demand discovery once")
}

// fakeSandboxServer accepts every Update.
type fakeSandboxServer struct {
	orchestrator.UnimplementedSandboxServiceServer

	calls      atomic.Int64
	appliedEnd atomic.Int64
}

func (s *fakeSandboxServer) Update(_ context.Context, req *orchestrator.SandboxUpdateRequest) (*emptypb.Empty, error) {
	s.calls.Add(1)
	s.appliedEnd.Store(req.GetEndTime().AsTime().UnixMilli())

	return &emptypb.Empty{}, nil
}

func TestKeepAliveFor_NodeFoundByDiscoveryExtends(t *testing.T) {
	t.Parallel()

	const nodeID = "late-node"

	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	infogrpc.RegisterInfoServiceServer(srv, &fakeInfoServer{nodeID: nodeID})
	sandboxSrv := &fakeSandboxServer{}
	orchestrator.RegisterSandboxServiceServer(srv, sandboxSrv)
	go srv.Serve(lis)
	t.Cleanup(srv.GracefulStop)

	rc := redis_utils.SetupInstance(t)
	o := newKeepAliveReplica(t, rc, &fixedDiscovery{instances: []servicediscovery.Instance{{
		WorkloadID: "workload-late",
		IPAddress:  "127.0.0.1",
		Port:       uint16(lis.Addr().(*net.TCPAddr).Port),
		Backend:    servicediscovery.BackendNomad,
	}}})
	sbx := addKeepAliveSandbox(t, o, nodeID, 30*time.Second)

	requested := time.Now()
	got, apiErr := o.KeepAliveFor(t.Context(), sbx.TeamID, sbx.SandboxID, time.Hour, false)
	require.Nil(t, apiErr)

	assert.WithinDuration(t, requested.Add(time.Hour), got.EndTime, 5*time.Second)
	requireExpiryUnchanged(t, o, rc, sbx, got.EndTime)
	assert.Equal(t, int64(1), sandboxSrv.calls.Load())
	assert.Equal(t, got.EndTime.UnixMilli(), sandboxSrv.appliedEnd.Load())
}

// A node in the pool may have applied the update even when the call fails, and
// expiring the record while the host is unresponsive kills a sandbox that may recover, so
// the extended deadline is kept on purpose.
func TestKeepAliveFor_NodeInPoolFailureKeepsExtendedDeadline(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		err      error
		wantCode int
	}{
		"unreachable node":                      {err: status.Error(codes.Unavailable, "connection refused"), wantCode: http.StatusInternalServerError},
		"lost acknowledgement after node apply": {err: status.Error(codes.DeadlineExceeded, "context deadline exceeded"), wantCode: http.StatusInternalServerError},
		"sandbox missing on node":               {err: status.Error(codes.NotFound, "sandbox not found"), wantCode: http.StatusNotFound},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			rc := redis_utils.SetupInstance(t)
			discovery := &countingDiscovery{}
			o := newKeepAliveReplica(t, rc, discovery)
			client := &keepAliveNodeClient{err: tt.err}
			addKeepAliveNode(o, "node-1", client)
			sbx := addKeepAliveSandbox(t, o, "node-1", 30*time.Second)

			requested := time.Now()
			_, apiErr := o.KeepAliveFor(t.Context(), sbx.TeamID, sbx.SandboxID, time.Hour, false)
			require.NotNil(t, apiErr)
			assert.Equal(t, tt.wantCode, apiErr.Code)
			require.NotErrorIs(t, apiErr.Err, errNodeNotInPool)

			stored, indexed := expiryState(t, o, rc, sbx)
			assert.WithinDuration(t, requested.Add(time.Hour), stored, 5*time.Second)
			assert.WithinDuration(t, stored, indexed, time.Millisecond)
			assert.Equal(t, stored.UnixMilli(), client.appliedEnd.Load())
			assert.Equal(t, int64(1), client.calls.Load())
			assert.Zero(t, discovery.calls.Load())
		})
	}
}

func TestKeepAliveFor_ReplicaWithoutNodeCannotOverwriteNewerDeadline(t *testing.T) {
	t.Parallel()

	rc := redis_utils.SetupInstance(t)
	owner := newKeepAliveReplica(t, rc, &countingDiscovery{})
	addKeepAliveNode(owner, "node-1", &keepAliveNodeClient{})
	stale := newKeepAliveReplica(t, rc, &countingDiscovery{})
	sbx := addKeepAliveSandbox(t, owner, "node-1", 30*time.Second)

	_, apiErr := stale.KeepAliveFor(t.Context(), sbx.TeamID, sbx.SandboxID, 2*time.Hour, false)
	requireRefusedKeepAlive(t, apiErr)
	requireExpiryUnchanged(t, owner, rc, sbx, sbx.EndTime)

	extended, apiErr := owner.KeepAliveFor(t.Context(), sbx.TeamID, sbx.SandboxID, time.Hour, false)
	require.Nil(t, apiErr)

	_, apiErr = stale.KeepAliveFor(t.Context(), sbx.TeamID, sbx.SandboxID, 2*time.Hour, false)
	requireRefusedKeepAlive(t, apiErr)
	requireExpiryUnchanged(t, owner, rc, sbx, extended.EndTime)

	// The stale replica asks for more than the owner, so any write of its own
	// would leave a later deadline than the owner's.
	var wg sync.WaitGroup
	var ownerResult atomic.Pointer[sandbox.Sandbox]
	requested := time.Now()
	wg.Go(func() {
		got, apiErr := owner.KeepAliveFor(t.Context(), sbx.TeamID, sbx.SandboxID, 90*time.Minute, false)
		assert.Nil(t, apiErr)
		ownerResult.Store(got)
	})
	for range 8 {
		wg.Go(func() {
			_, apiErr := stale.KeepAliveFor(t.Context(), sbx.TeamID, sbx.SandboxID, 2*time.Hour, false)
			assert.NotNil(t, apiErr)
		})
	}
	wg.Wait()

	require.NotNil(t, ownerResult.Load())
	assert.WithinDuration(t, requested.Add(90*time.Minute), ownerResult.Load().EndTime, 5*time.Second)
	requireExpiryUnchanged(t, owner, rc, sbx, ownerResult.Load().EndTime)
}

func TestKeepAliveFor_ShorterRequestWithNodeAbsentKeepsTodaysBehaviour(t *testing.T) {
	t.Parallel()

	rc := redis_utils.SetupInstance(t)
	discovery := &countingDiscovery{}
	o := newKeepAliveReplica(t, rc, discovery)
	sbx := addKeepAliveSandbox(t, o, "absent-node", time.Hour)

	_, apiErr := o.KeepAliveFor(t.Context(), sbx.TeamID, sbx.SandboxID, 10*time.Minute, false)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusInternalServerError, apiErr.Code)
	assert.Equal(t, keepAliveFailedMsg, apiErr.ClientMsg)
	require.NotErrorIs(t, apiErr.Err, errNodeNotInPool, "a request that does not extend never reaches the pool check")

	requireExpiryUnchanged(t, o, rc, sbx, sbx.EndTime)
	assert.Equal(t, int64(1), discovery.calls.Load())
}

// Moving the deadline earlier never prolongs ownership, so it is stored even
// when the node is absent; the node call then fails as before.
func TestKeepAliveFor_ShorteningWithNodeAbsentIsStored(t *testing.T) {
	t.Parallel()

	rc := redis_utils.SetupInstance(t)
	o := newKeepAliveReplica(t, rc, &countingDiscovery{})
	sbx := addKeepAliveSandbox(t, o, "absent-node", time.Hour)

	requested := time.Now()
	_, apiErr := o.KeepAliveFor(t.Context(), sbx.TeamID, sbx.SandboxID, 10*time.Second, true)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusInternalServerError, apiErr.Code)
	require.NotErrorIs(t, apiErr.Err, errNodeNotInPool)

	stored, indexed := expiryState(t, o, rc, sbx)
	assert.WithinDuration(t, requested.Add(10*time.Second), stored, 5*time.Second)
	assert.WithinDuration(t, stored, indexed, time.Millisecond)
}

func TestKeepAliveFor_NodeInPoolExtends(t *testing.T) {
	t.Parallel()

	rc := redis_utils.SetupInstance(t)
	discovery := &countingDiscovery{}
	o := newKeepAliveReplica(t, rc, discovery)
	client := &keepAliveNodeClient{}
	addKeepAliveNode(o, "node-1", client)
	sbx := addKeepAliveSandbox(t, o, "node-1", 30*time.Second)

	requested := time.Now()
	got, apiErr := o.KeepAliveFor(t.Context(), sbx.TeamID, sbx.SandboxID, time.Hour, false)
	require.Nil(t, apiErr)

	assert.WithinDuration(t, requested.Add(time.Hour), got.EndTime, 5*time.Second)
	requireExpiryUnchanged(t, o, rc, sbx, got.EndTime)
	assert.Equal(t, int64(1), client.calls.Load())
	assert.Equal(t, got.EndTime.UnixMilli(), client.appliedEnd.Load())
	assert.Zero(t, discovery.calls.Load())
}
