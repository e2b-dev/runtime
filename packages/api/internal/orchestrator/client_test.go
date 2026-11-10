package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	nomadapi "github.com/hashicorp/nomad/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	infogrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery/nomad"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// newTestOrchestrator creates a minimal Orchestrator with only the fields
// needed for node lookup / discovery tests. Production fields (sandboxStore,
// analytics, redis, etc.) are left nil because the code paths under test never
// touch them.
func newTestOrchestrator(t *testing.T, nomadClient *nomadapi.Client) *Orchestrator {
	t.Helper()

	ctx := t.Context()
	logger.ReplaceGlobals(ctx, logger.NewNopLogger())

	return &Orchestrator{
		nodes:         smap.New[*nodemanager.Node](),
		nodeDiscovery: nomad.NewServices(nomadClient, []string{"orchestrator"}),
		tel:           telemetry.NewNoopClient(),
		nodeCreds:     insecure.NewCredentials(),
	}
}

func newNomadMock(t *testing.T, handler http.HandlerFunc) *nomadapi.Client {
	t.Helper()

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	client, err := nomadapi.NewClient(&nomadapi.Config{Address: srv.URL})
	require.NoError(t, err)

	return client
}

// fakeInfoServer implements the minimum InfoServiceServer surface needed by
// nodemanager.New: it responds to ServiceInfo with a canned response carrying
// the given nodeID and Healthy status.
type fakeInfoServer struct {
	infogrpc.UnimplementedInfoServiceServer

	nodeID string
}

func (s *fakeInfoServer) ServiceInfo(context.Context, *emptypb.Empty) (*infogrpc.ServiceInfoResponse, error) {
	return &infogrpc.ServiceInfoResponse{
		NodeId:         s.nodeID,
		ServiceId:      "test-service-instance",
		ServiceStatus:  infogrpc.ServiceInfoStatus_Healthy,
		MetricCpuCount: 4,
	}, nil
}

// startFakeOrchestratorGRPC starts a fake ServiceInfo server on an ephemeral
// port and returns it; a fixed port flakes on shared runners.
func startFakeOrchestratorGRPC(t *testing.T, nodeID string) int {
	t.Helper()

	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	infogrpc.RegisterInfoServiceServer(srv, &fakeInfoServer{nodeID: nodeID})

	go srv.Serve(lis)
	t.Cleanup(srv.GracefulStop)

	return lis.Addr().(*net.TCPAddr).Port
}

// TestGetOrConnectNode_CacheHit verifies that when a node is already in the
// cache, getOrConnectNode returns it immediately without triggering any
// discovery. This is the fast path exercised on every sandbox operation.
func TestGetOrConnectNode_CacheHit(t *testing.T) {
	t.Parallel()

	o := newTestOrchestrator(t, nil)

	clusterID := uuid.New()
	testNode := nodemanager.NewTestNode("node-1", api.NodeStatusReady, 3, 4)
	testNode.ClusterID = clusterID
	o.nodes.Insert(o.scopedNodeID(clusterID, "node-1"), testNode)

	got := o.getOrConnectNode(t.Context(), clusterID, "node-1")
	require.NotNil(t, got)
	assert.Equal(t, "node-1", got.ID)
}

func TestGetOrConnectNode_CacheHit_LocalCluster(t *testing.T) {
	t.Parallel()

	o := newTestOrchestrator(t, nil)

	testNode := nodemanager.NewTestNode("local-node", api.NodeStatusReady, 2, 4)
	testNode.ClusterID = consts.LocalClusterID
	o.nodes.Insert("local-node", testNode)

	got := o.getOrConnectNode(t.Context(), consts.LocalClusterID, "local-node")
	require.NotNil(t, got)
	assert.Equal(t, "local-node", got.ID)
}

// TestGetOrConnectNode_CacheMiss_TriggersNomadDiscovery verifies that when a Nomad node is NOT
// in the cache, getOrConnectNode triggers on-demand Nomad service discovery rather than immediately returning nil.
//
// This handles scenario when a new orchestrator node has joined the cluster and may not be in cache yet
func TestGetOrConnectNode_CacheMiss_TriggersNomadDiscovery(t *testing.T) {
	t.Parallel()

	var discoveryAttempts atomic.Int32

	nomadClient := newNomadMock(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/service/orchestrator" {
			discoveryAttempts.Add(1)

			// Return a service registration. connectToNode will fail at the
			// gRPC level (nodemanager.New dials the fake address), but the
			// important thing is that discovery WAS attempted.
			resp := []*nomadapi.ServiceRegistration{
				{
					ID:          "_nomad-task-abc-orchestrator",
					ServiceName: "orchestrator",
					NodeID:      "abcdef1234567890abcdef1234567890abcdef12",
					Address:     "127.0.0.1",
					Port:        int(consts.OrchestratorAPIPort),
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)

			return
		}

		http.NotFound(w, r)
	})

	o := newTestOrchestrator(t, nomadClient)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	// Request a node that isn't in cache — should trigger discovery.
	o.getOrConnectNode(ctx, consts.LocalClusterID, "nonexistent-node")

	// The node won't be found because connectToNode fails at the gRPC level,
	// but discovery MUST have been attempted.
	assert.Positive(t, discoveryAttempts.Load(), "expected on-demand Nomad discovery to be triggered")
}

// TestGetOrConnectNode_ConcurrentCacheMiss_SharesDiscovery verifies that
// multiple concurrent getOrConnectNode calls for the same missing node share
// a single discovery attempt
func TestGetOrConnectNode_ConcurrentCacheMiss_SharesDiscovery(t *testing.T) {
	t.Parallel()

	var discoveryAttempts atomic.Int32

	nomadClient := newNomadMock(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/service/orchestrator" {
			discoveryAttempts.Add(1)
			// Slow response to ensure concurrent callers overlap.
			time.Sleep(100 * time.Millisecond)

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode([]*nomadapi.ServiceRegistration{})

			return
		}

		http.NotFound(w, r)
	})

	o := newTestOrchestrator(t, nomadClient)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Fire 10 concurrent lookups for the same missing node.
	const concurrency = 10
	done := make(chan struct{}, concurrency)
	for range concurrency {
		go func() {
			defer func() { done <- struct{}{} }()
			o.getOrConnectNode(ctx, consts.LocalClusterID, "missing-node")
		}()
	}

	for range concurrency {
		<-done
	}

	// should collapse all 10 calls into ≤2
	// one flight, possibly one more if the first completed before a late caller arrived
	assert.LessOrEqual(t, discoveryAttempts.Load(), int32(2),
		"singleflight should deduplicate concurrent discovery attempts")
}

// TestConnectToNode_SingleflightDedup verifies that concurrent connectToNode
// calls for the same WorkloadID share a single connection attempt
func TestConnectToNode_SingleflightDedup(t *testing.T) {
	t.Parallel()

	o := newTestOrchestrator(t, nil)

	// grpc.NewClient is lazy — it returns immediately — and nodemanager.New
	// then fails at the ServiceInfo RPC call
	discovery := nodemanager.NodePlaneInstance{
		WorkloadID:          "abcdef12",
		OrchestratorAddress: "127.0.0.1:1",
		IPAddress:           "127.0.0.1",
	}

	const concurrency = 10
	errs := make(chan error, concurrency)

	for range concurrency {
		go func() {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			errs <- o.connectToNode(ctx, discovery)
		}()
	}

	for range concurrency {
		<-errs
	}

	// After all calls complete, verify the node map has at most 1 entry
	// (or 0 if all failed)
	assert.LessOrEqual(t, o.nodes.Count(), 1, "singleflight should prevent duplicate registrations")
}

// TestGetOrConnectNode_CacheMiss_DiscoversAndConnects is the end-to-end
// test for a race condition. It simulates following scenario:
//
//  1. A new orchestrator node is running and reachable (fake gRPC server).
//  2. Nomad service discovery knows about it (mock HTTP API).
//  3. This API instance has NOT yet synced (node is absent from o.nodes).
//  4. A handler calls getOrConnectNode for a sandbox on that node.
//
// The fake gRPC server and the mocked service registration share an ephemeral
// port, so nomad discovery dials the right listener.
func TestGetOrConnectNode_CacheMiss_DiscoversAndConnects(t *testing.T) {
	t.Parallel()

	orchestratorNodeID := "orch-node-42"
	nomadFullID := "aabbccdd11223344aabbccdd11223344aabbccdd"

	port := startFakeOrchestratorGRPC(t, orchestratorNodeID)

	nomadClient := newNomadMock(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/service/orchestrator" {
			resp := []*nomadapi.ServiceRegistration{
				{
					ID:          "_nomad-task-def-orchestrator",
					ServiceName: "orchestrator",
					NodeID:      nomadFullID,
					Address:     "127.0.0.1",
					Port:        port,
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)

			return
		}

		http.NotFound(w, r)
	})

	o := newTestOrchestrator(t, nomadClient)

	// 3. Verify the node is NOT in cache.
	assert.Nil(t, o.GetNode(consts.LocalClusterID, orchestratorNodeID))

	// 4. getOrConnectNode triggers the full discovery path:
	//    cache miss → discoverNomadNode → listNomadNodes → connectToNode → gRPC ServiceInfo → registerNode
	node := o.getOrConnectNode(t.Context(), consts.LocalClusterID, orchestratorNodeID)
	require.NotNil(t, node, "getOrConnectNode must discover and connect the node via Nomad")
	assert.Equal(t, orchestratorNodeID, node.ID)
	assert.Equal(t, nomadFullID[:consts.NodeIDLength], node.WorkloadID)
	assert.Equal(t, servicediscovery.BackendNomad, node.Backend, "the discovering backend's platform must reach the node catalog")
}

// TestRegisterNode_NoDuplicates verifies that registerNode is idempotent
// when the same scoped key is used — the last write wins, and the map never
// grows beyond the number of unique keys.
func TestRegisterNode_NoDuplicates(t *testing.T) {
	t.Parallel()

	o := newTestOrchestrator(t, nil)

	clusterID := uuid.New()
	wg := sync.WaitGroup{}
	for i := range 50 {
		wg.Go(func() {
			node := nodemanager.NewTestNode(fmt.Sprintf("node-%d", i%5), api.NodeStatusReady, 2, 4)
			node.ClusterID = clusterID
			o.registerNode(node)
		})
	}

	wg.Wait()
	assert.Equal(t, 5, o.nodes.Count())
}

// TestRegistersClusterOrchestrators covers which discovery path owns
// orchestrator nodes. Local-cluster instances are owned by the node discovery
// path (connectToNode) and must not be registered a second time from the
// clusters registry — except when the node discovery loop is disabled
// (ENVIRONMENT=local), where the clusters registry is the only source of
// orchestrator nodes and skipping it would leave the API with zero nodes.
func TestRegistersClusterOrchestrators(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                          string
		clusterID                     uuid.UUID
		localClusterOwnsOrchestrators bool
		want                          bool
	}{
		{
			name:                          "local cluster with node discovery running",
			clusterID:                     consts.LocalClusterID,
			localClusterOwnsOrchestrators: false,
			want:                          false,
		},
		{
			name:                          "local cluster without node discovery",
			clusterID:                     consts.LocalClusterID,
			localClusterOwnsOrchestrators: true,
			want:                          true,
		},
		{
			name:                          "remote cluster with node discovery running",
			clusterID:                     uuid.New(),
			localClusterOwnsOrchestrators: false,
			want:                          true,
		},
		{
			name:                          "remote cluster without node discovery",
			clusterID:                     uuid.New(),
			localClusterOwnsOrchestrators: true,
			want:                          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			o := newTestOrchestrator(t, nil)
			o.localClusterOwnsOrchestrators = tt.localClusterOwnsOrchestrators

			assert.Equal(t, tt.want, o.registersClusterOrchestrators(tt.clusterID))
		})
	}
}

// TestConnectToClusterNode_SkipsLocalCluster verifies that the ownership check
// short-circuits connectToClusterNode before it touches the instance. The nil
// instance is intentional: it makes a regression to unconditional registration
// fail loudly instead of silently duplicating the node.
func TestConnectToClusterNode_SkipsLocalCluster(t *testing.T) {
	t.Parallel()

	o := newTestOrchestrator(t, nil)
	o.localClusterOwnsOrchestrators = false

	o.connectToClusterNode(t.Context(), &clusters.Cluster{ID: consts.LocalClusterID}, nil)

	assert.Zero(t, o.nodes.Count())
}

// fixedDiscovery returns a canned instance list, so a test can drive the node
// plane with a backend value the Nomad listers cannot produce.
type fixedDiscovery struct {
	instances []servicediscovery.Instance
}

func (d *fixedDiscovery) ListInstances(context.Context) ([]servicediscovery.Instance, error) {
	return d.instances, nil
}

func (d *fixedDiscovery) Start(context.Context) {}

func (d *fixedDiscovery) Stop(context.Context) {}

// Both hops of the tag — discovered instance to node-plane instance, and that
// to the catalog node — are satisfied by a constant while the only test driving
// them discovers through Nomad. Hardcoding "nomad" at either site was green.
func TestGetOrConnectNode_CarriesTheDiscoveryBackendToTheCatalog(t *testing.T) {
	t.Parallel()

	for name, platform := range map[string]string{
		"nomad":      servicediscovery.BackendNomad,
		"kubernetes": servicediscovery.BackendKubernetes,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			nodeID := "orch-" + name
			port := startFakeOrchestratorGRPC(t, nodeID)

			o := &Orchestrator{
				nodes: smap.New[*nodemanager.Node](),
				nodeDiscovery: &fixedDiscovery{instances: []servicediscovery.Instance{{
					WorkloadID: "workload-" + name,
					IPAddress:  "127.0.0.1",
					Port:       uint16(port),
					Backend:    platform,
				}}},
				tel:       telemetry.NewNoopClient(),
				nodeCreds: insecure.NewCredentials(),
			}

			node := o.getOrConnectNode(t.Context(), consts.LocalClusterID, nodeID)
			require.NotNil(t, node)
			assert.Equal(t, platform, node.Backend)
		})
	}
}

// countingDiscovery records whether the node plane was consulted.
type countingDiscovery struct {
	calls atomic.Int64
}

func (d *countingDiscovery) ListInstances(context.Context) ([]servicediscovery.Instance, error) {
	d.calls.Add(1)

	return nil, nil
}

func (d *countingDiscovery) Start(context.Context) {}

func (d *countingDiscovery) Stop(context.Context) {}

// When the local clusters registry owns orchestrator nodes, the on-demand path
// must not also discover them through the node plane. Both would register the
// same process — once under the node ID it reports over gRPC, once under its
// discovery item ID — and its capacity and sandboxes would count twice. The
// periodic loop already honours this; the on-demand path used to be a no-op
// only because the node plane happened to fail in a local environment.
func TestGetOrConnectNode_LocalRegistryOwnershipSkipsTheNodePlane(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		registryOwns bool
		wantCalls    int64
	}{
		"registry owns orchestrators: the node plane is not consulted": {registryOwns: true, wantCalls: 0},
		"registry does not own them: the node plane is":                {registryOwns: false, wantCalls: 1},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			discovery := &countingDiscovery{}
			o := &Orchestrator{
				nodes:                         smap.New[*nodemanager.Node](),
				nodeDiscovery:                 discovery,
				clusters:                      clusters.NewTestPool(),
				localClusterOwnsOrchestrators: tt.registryOwns,
			}

			o.getOrConnectNode(t.Context(), consts.LocalClusterID, "absent-node")

			assert.Equal(t, tt.wantCalls, discovery.calls.Load())
		})
	}
}
