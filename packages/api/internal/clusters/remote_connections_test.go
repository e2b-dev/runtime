package clusters

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	infogrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	edgeapi "github.com/e2b-dev/infra/packages/shared/pkg/http/edge"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// echoRoutingServer answers ServiceInfo with the instance ID the call was
// routed by, standing in for the remote edge proxy.
type echoRoutingServer struct {
	infogrpc.UnimplementedInfoServiceServer
}

func (echoRoutingServer) ServiceInfo(ctx context.Context, _ *emptypb.Empty) (*infogrpc.ServiceInfoResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	ids := md.Get(consts.EdgeRpcServiceInstanceIDHeader)
	if len(ids) != 1 {
		return nil, status.Errorf(codes.InvalidArgument, "want one instance id, got %v", ids)
	}

	return &infogrpc.ServiceInfoResponse{ServiceId: ids[0]}, nil
}

func echoRoutingEndpoint(t *testing.T) string {
	t.Helper()

	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	infogrpc.RegisterInfoServiceServer(server, echoRoutingServer{})
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	return lis.Addr().String()
}

// newTestRemoteConnections pools connections to an echoRoutingEndpoint for the
// given cluster.
func newTestRemoteConnections(t *testing.T, flags *featureflags.Client, clusterID uuid.UUID) *remoteConnections {
	t.Helper()

	endpoint := echoRoutingEndpoint(t)
	connections := newRemoteConnections(telemetry.NewNoopClient(), flags, clusterID, endpoint, false, insecure.NewCredentials())

	return connections
}

// Every instance of a remote cluster shares the pool, which grows with the
// cell so no connection carries more than its share of instances, and each
// call still carries its own instance's routing header.
func TestRemoteConnections_PoolGrowsWithTheCell(t *testing.T) {
	t.Parallel()

	connections := newTestRemoteConnections(t, nil, uuid.New())

	const instances = 300
	perConnection := map[*grpc.ClientConn]int{}
	for i := range instances {
		id := fmt.Sprintf("svc-%d", i)
		client, err := connections.client(t.Context(), &instanceAuthorization{secret: "shh", serviceInstanceID: id})
		require.NoError(t, err)

		info, err := client.Info.ServiceInfo(t.Context(), &emptypb.Empty{})
		require.NoError(t, err)
		assert.Equal(t, id, info.GetServiceId(), "the call must be routed to its own instance")

		perConnection[client.Connection]++
	}

	assert.Len(t, perConnection, (instances+featureflags.RemoteClusterInstancesPerConnectionFlag.Fallback()-1)/featureflags.RemoteClusterInstancesPerConnectionFlag.Fallback())
	for _, n := range perConnection {
		assert.LessOrEqual(t, n, featureflags.RemoteClusterInstancesPerConnectionFlag.Fallback())
	}
}

// A small cell still spreads across the minimum pool.
func TestRemoteConnections_SmallCellUsesTheMinimumPool(t *testing.T) {
	t.Parallel()

	connections := newTestRemoteConnections(t, nil, uuid.New())

	distinct := map[*grpc.ClientConn]struct{}{}
	for i := range 2 * featureflags.RemoteClusterMinConnectionsFlag.Fallback() {
		client, err := connections.client(t.Context(), &instanceAuthorization{serviceInstanceID: fmt.Sprintf("svc-%d", i)})
		require.NoError(t, err)

		distinct[client.Connection] = struct{}{}
	}

	assert.Len(t, distinct, featureflags.RemoteClusterMinConnectionsFlag.Fallback())
}

// Closing one instance must retire only that instance: its calls fail and it
// reads as shut down, while the connection it shared keeps serving the rest,
// and its slot goes to the next instance that joins.
func TestRemoteConnections_ClosingAnInstanceLeavesTheConnectionOpen(t *testing.T) {
	t.Parallel()

	connections := newTestRemoteConnections(t, nil, uuid.New())

	// Put two instances on each connection of the minimum pool.
	clients := make([]*GRPCClient, 0, 2*featureflags.RemoteClusterMinConnectionsFlag.Fallback())
	for i := range 2 * featureflags.RemoteClusterMinConnectionsFlag.Fallback() {
		client, err := connections.client(t.Context(), &instanceAuthorization{serviceInstanceID: fmt.Sprintf("svc-%d", i)})
		require.NoError(t, err)

		clients = append(clients, client)
	}

	closed, open := clients[0], clients[featureflags.RemoteClusterMinConnectionsFlag.Fallback()]
	require.Same(t, closed.Connection, open.Connection)

	require.NoError(t, closed.Close())

	assert.Equal(t, connectivity.Shutdown, closed.State(), "a closed instance reads as shut down")
	_, err := closed.Info.ServiceInfo(t.Context(), &emptypb.Empty{})
	assert.Equal(t, codes.Canceled, status.Code(err))

	assert.NotEqual(t, connectivity.Shutdown, open.State())
	info, err := open.Info.ServiceInfo(t.Context(), &emptypb.Empty{})
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("svc-%d", featureflags.RemoteClusterMinConnectionsFlag.Fallback()), info.GetServiceId())

	// Every other connection still carries two instances, so the freed slot is
	// the least used.
	next, err := connections.client(t.Context(), &instanceAuthorization{serviceInstanceID: "svc-next"})
	require.NoError(t, err)
	assert.Same(t, closed.Connection, next.Connection, "the freed slot is reused")
}

// Flags targeted at a cluster size its pool.
func TestRemoteConnections_FlagsSizeThePoolPerCluster(t *testing.T) {
	t.Parallel()

	source := ldtestdata.DataSource()
	clusterID := uuid.New()
	targetCluster := func(flag featureflags.IntFlag, value int) {
		source.Update(source.Flag(flag.Key()).
			Variations(ldvalue.Int(flag.Fallback()), ldvalue.Int(value)).
			FallthroughVariationIndex(0).
			IfMatchContext(featureflags.ClusterKind, "key", ldvalue.String(clusterID.String())).
			ThenReturnIndex(1))
	}
	targetCluster(featureflags.RemoteClusterMinConnectionsFlag, 1)
	targetCluster(featureflags.RemoteClusterInstancesPerConnectionFlag, 2)
	flags, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { _ = flags.Close(context.WithoutCancel(t.Context())) })

	connections := newTestRemoteConnections(t, flags, clusterID)

	perConnection := map[*grpc.ClientConn]int{}
	for i := range 5 {
		client, err := connections.client(t.Context(), &instanceAuthorization{serviceInstanceID: fmt.Sprintf("svc-%d", i)})
		require.NoError(t, err)

		perConnection[client.Connection]++
	}

	assert.ElementsMatch(t, []int{2, 2, 1}, slices.Collect(maps.Values(perConnection)))
}

func TestRemoteConnections_CloseShutsDownEveryConnection(t *testing.T) {
	t.Parallel()

	connections := newTestRemoteConnections(t, nil, uuid.New())

	client, err := connections.client(t.Context(), &instanceAuthorization{serviceInstanceID: "svc-a"})
	require.NoError(t, err)

	require.NoError(t, connections.Close())
	assert.Equal(t, connectivity.Shutdown, client.State())

	_, err = connections.client(t.Context(), &instanceAuthorization{serviceInstanceID: "svc-b"})
	assert.Error(t, err, "a closed cluster hands out no new clients")
}

// edgeServer stands in for a remote cluster's edge: discovery and the gRPC
// proxy on one listener. It lists the given instances, fails ServiceInfo for
// the ones in failing, and records each gRPC client connection it serves.
// onDiscovery, when set, sees every discovery request.
type edgeServer struct {
	infogrpc.UnimplementedInfoServiceServer

	instances   []string
	failing     map[string]bool
	onDiscovery func(*http.Request)

	mu          sync.Mutex
	grpcClients map[string]struct{}
}

func (e *edgeServer) ServiceInfo(ctx context.Context, _ *emptypb.Empty) (*infogrpc.ServiceInfoResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	ids := md.Get(consts.EdgeRpcServiceInstanceIDHeader)
	if len(ids) != 1 {
		return nil, status.Errorf(codes.InvalidArgument, "want one instance id, got %v", ids)
	}
	if e.failing[ids[0]] {
		return nil, status.Error(codes.Unavailable, "instance is not ready")
	}

	return &infogrpc.ServiceInfoResponse{ServiceId: ids[0]}, nil
}

func (e *edgeServer) connections() int {
	e.mu.Lock()
	defer e.mu.Unlock()

	return len(e.grpcClients)
}

func (e *edgeServer) start(t *testing.T) string {
	t.Helper()

	grpcServer := grpc.NewServer()
	infogrpc.RegisterInfoServiceServer(grpcServer, e)

	orchestrators := make([]edgeapi.ClusterOrchestratorNode, 0, len(e.instances))
	for _, id := range e.instances {
		orchestrators = append(orchestrators, edgeapi.ClusterOrchestratorNode{ServiceInstanceID: id, NodeID: "node-" + id})
	}
	discovery, err := json.Marshal(map[string]any{"orchestrators": orchestrators})
	require.NoError(t, err)

	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	server := &http.Server{
		Protocols:         &protocols,
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
				e.mu.Lock()
				e.grpcClients[r.RemoteAddr] = struct{}{}
				e.mu.Unlock()

				grpcServer.ServeHTTP(w, r)

				return
			}

			if e.onDiscovery != nil {
				e.onDiscovery(r)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(discovery)
		}),
	}

	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() { _ = server.Close() })

	return lis.Addr().String()
}

// A remote cluster's sync rounds reach every instance over the shared pool:
// the connections stay within the pool however many instances there are, and
// instances that fail their first sync and are retried each round do not dial
// new ones.
func TestRemoteCluster_SyncRoundsShareThePool(t *testing.T) {
	t.Parallel()

	edge := &edgeServer{failing: map[string]bool{}, grpcClients: map[string]struct{}{}}
	for i := range 20 {
		id := fmt.Sprintf("svc-%d", i)
		edge.instances = append(edge.instances, id)
		if i%5 == 0 {
			edge.failing[id] = true
		}
	}
	endpoint := edge.start(t)

	cluster, err := newRemoteCluster(telemetry.NewNoopClient(), nil, queries.Cluster{ID: uuid.New(), Endpoint: endpoint})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cluster.Close(context.WithoutCancel(t.Context())) })

	require.NoError(t, cluster.SyncInstances(t.Context()))
	require.Equal(t, len(edge.instances)-len(edge.failing), cluster.instances.Count())
	afterFirstRound := edge.connections()
	assert.LessOrEqual(t, afterFirstRound, featureflags.RemoteClusterMinConnectionsFlag.Fallback())

	for range 2 {
		require.NoError(t, cluster.SyncInstances(t.Context()))
	}
	assert.Equal(t, afterFirstRound, edge.connections(), "later rounds must not open new connections")
}

// A shared connection that loses the edge logs its move into
// TRANSIENT_FAILURE once, naming the cluster and which connection it is.
//
//nolint:paralleltest // ReplaceGlobals mutates process state
func TestRemoteConnections_LogTransientFailure(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	t.Cleanup(logger.ReplaceGlobals(t.Context(), logger.NewTracedLoggerFromCore(core)))

	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	infogrpc.RegisterInfoServiceServer(server, echoRoutingServer{})
	go func() { _ = server.Serve(lis) }()

	clusterID := uuid.New()
	connections := newRemoteConnections(telemetry.NewNoopClient(), nil, clusterID, lis.Addr().String(), false, insecure.NewCredentials())
	t.Cleanup(func() { _ = connections.Close() })

	client, err := connections.client(t.Context(), &instanceAuthorization{serviceInstanceID: "svc-a"})
	require.NoError(t, err)
	_, err = client.Info.ServiceInfo(t.Context(), &emptypb.Empty{})
	require.NoError(t, err)

	server.Stop()

	var failure []observer.LoggedEntry
	require.Eventually(t, func() bool {
		// A call reconnects the idle connection, which then fails to dial.
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		_, _ = client.Info.ServiceInfo(ctx, &emptypb.Empty{})

		failure = logs.FilterMessage("Remote cluster connection changed state").
			FilterField(zap.Stringer("to", connectivity.TransientFailure)).All()

		return len(failure) > 0
	}, 10*time.Second, 50*time.Millisecond)

	fields := failure[0].ContextMap()
	assert.Equal(t, zapcore.WarnLevel, failure[0].Level)
	assert.Equal(t, clusterID.String(), fields["cluster.id"])
	assert.EqualValues(t, 0, fields["connection_index"])
}
