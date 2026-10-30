package orchestrator

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	sandboxredis "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	orchestratorgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	sandbox_network "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-network"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery/nomad"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// Node versions as orchestrator-ee reports them: the build that added
// egress_proxy_tls, and the one just before it.
const (
	buildWithEgressProxyTLS   = "0.16.202610011012-6bbb0090f80-ee"
	buildBeforeEgressProxyTLS = "0.16.202610011007-5dfa8dfb8aa-ee"
)

func addRunningSandbox(t *testing.T, o *Orchestrator) sandbox.Sandbox {
	t.Helper()

	return addSandboxInState(t, o, sandbox.StateRunning, "node-1")
}

func addSandboxInState(t *testing.T, o *Orchestrator, state sandbox.State, nodeID string) sandbox.Sandbox {
	t.Helper()

	sbx := sandbox.Sandbox{
		SandboxID:         "sbx-" + uuid.NewString()[:8],
		TemplateID:        "tpl-1",
		ExecutionID:       uuid.NewString(),
		TeamID:            uuid.New(),
		BuildID:           uuid.New(),
		MaxInstanceLength: time.Hour,
		StartTime:         time.Now(),
		EndTime:           time.Now().Add(time.Hour),
		VCpu:              2,
		RamMB:             512,
		NodeID:            nodeID,
		ClusterID:         uuid.Nil,
		State:             state,
	}
	require.NoError(t, o.sandboxStore.Add(t.Context(), sbx, nil))

	return sbx
}

func tlsProxy() *sandbox_network.EgressProxyConfig {
	return &sandbox_network.EgressProxyConfig{
		Address: "proxy.example.com:1080",
		TLS:     &sandbox_network.EgressProxyTLSConfig{Enabled: true},
	}
}

func TestUpdateSandboxNetworkConfig_PlacedNetworkRefusesProxyBeforeStore(t *testing.T) {
	t.Parallel()
	o := newCreateSandboxTestOrchestrator(t, nodemanager.WithOrchestratorVersion(buildWithEgressProxyTLS))
	sbx := addRunningSandbox(t, o)
	_, err := o.sandboxStore.Update(t.Context(), sbx.TeamID, sbx.SandboxID, func(sbx sandbox.Sandbox) (sandbox.Sandbox, error) {
		sbx.NetworkPlacement = &orchestratorgrpc.SandboxNetworkPlacement{NetworkMode: "egress-router"}
		sbx.Network = &types.SandboxNetworkConfig{Egress: &types.SandboxNetworkEgressConfig{AllowedAddresses: []string{"example.com"}}}

		return sbx, nil
	})
	require.NoError(t, err)

	apiErr := o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID, nil, nil, nil, nil, nil, tlsProxy())
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusBadRequest, apiErr.Code)
	stored, err := o.sandboxStore.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, &types.SandboxNetworkEgressConfig{AllowedAddresses: []string{"example.com"}}, stored.Network.Egress,
		"a rejected proxy must not replace the API's committed configuration")

	require.Nil(t, o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID, nil, nil, nil, nil, nil, nil),
		"removing the proxy is still accepted on a routed execution")
}

// A live network update never goes through placement, so the node the sandbox
// already runs on has to be checked for the capability instead.
func TestUpdateSandboxNetworkConfig_EgressProxyTLSGate(t *testing.T) {
	t.Parallel()

	t.Run("old node refuses tls and stores nothing", func(t *testing.T) {
		t.Parallel()

		o := newCreateSandboxTestOrchestrator(t, nodemanager.WithOrchestratorVersion(buildBeforeEgressProxyTLS))
		sbx := addRunningSandbox(t, o)

		apiErr := o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID, nil, nil, nil, nil, nil, tlsProxy())
		require.NotNil(t, apiErr, "a node that would drop the TLS block must not be sent it")
		assert.Equal(t, http.StatusServiceUnavailable, apiErr.Code)
		assert.Equal(t, errCodeFeatureUnsupported, apiErr.ErrorCode)

		stored, err := o.sandboxStore.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
		require.NoError(t, err)
		assert.False(t, stored.Network.HasEgressProxyTLS(),
			"a refused config must not be stored, or GET would report TLS the hop does not use")
	})

	t.Run("new node accepts tls", func(t *testing.T) {
		t.Parallel()

		o := newCreateSandboxTestOrchestrator(t, nodemanager.WithOrchestratorVersion(buildWithEgressProxyTLS))
		sbx := addRunningSandbox(t, o)

		apiErr := o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID, nil, nil, nil, nil, nil, tlsProxy())
		require.Nil(t, apiErr)

		stored, err := o.sandboxStore.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
		require.NoError(t, err)
		assert.True(t, stored.Network.HasEgressProxyTLS())
	})

	t.Run("old node still accepts a proxy without tls", func(t *testing.T) {
		t.Parallel()

		o := newCreateSandboxTestOrchestrator(t, nodemanager.WithOrchestratorVersion(buildBeforeEgressProxyTLS))
		sbx := addRunningSandbox(t, o)

		apiErr := o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID, nil, nil, nil, nil, nil,
			&sandbox_network.EgressProxyConfig{Address: "proxy.example.com:1080"})
		require.Nil(t, apiErr, "the gate applies only to configs that ask for TLS")
	})

	t.Run("sandbox that is not running refuses tls and stores nothing", func(t *testing.T) {
		t.Parallel()

		o := newCreateSandboxTestOrchestrator(t, nodemanager.WithOrchestratorVersion(buildBeforeEgressProxyTLS))
		sbx := addSandboxInState(t, o, sandbox.StateSnapshotting, "node-1")

		apiErr := o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID, nil, nil, nil, nil, nil, tlsProxy())
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusConflict, apiErr.Code)

		stored, err := o.sandboxStore.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
		require.NoError(t, err)
		assert.False(t, stored.Network.HasEgressProxyTLS())
	})

	t.Run("node discovery cannot find refuses tls and stores nothing", func(t *testing.T) {
		t.Parallel()

		o := newCreateSandboxTestOrchestrator(t, nodemanager.WithOrchestratorVersion(buildWithEgressProxyTLS))
		o.nodeDiscovery = nomadDiscovery(t, nil)
		sbx := addSandboxInState(t, o, sandbox.StateRunning, "node-unknown")

		apiErr := o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID, nil, nil, nil, nil, nil, tlsProxy())
		require.NotNil(t, apiErr, "a node whose version cannot be read must not be sent TLS")
		assert.Equal(t, http.StatusInternalServerError, apiErr.Code)

		stored, err := o.sandboxStore.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
		require.NoError(t, err)
		assert.False(t, stored.Network.HasEgressProxyTLS())
	})

	t.Run("missing sandbox is not found", func(t *testing.T) {
		t.Parallel()

		o := newCreateSandboxTestOrchestrator(t, nodemanager.WithOrchestratorVersion(buildWithEgressProxyTLS))

		apiErr := o.UpdateSandboxNetworkConfig(t.Context(), uuid.New(), "sbx-missing", nil, nil, nil, nil, nil, tlsProxy())
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusNotFound, apiErr.Code)
	})
}

type capturingSandboxUpdateClient struct {
	orchestratorgrpc.SandboxServiceClient

	req *orchestratorgrpc.SandboxUpdateRequest
}

func (c *capturingSandboxUpdateClient) Update(_ context.Context, req *orchestratorgrpc.SandboxUpdateRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	c.req = req

	return &emptypb.Empty{}, nil
}

// An update replaces dnsServers like every other egress field: leaving it out
// resets it to the default, which allows the default nameserver.
func TestUpdateSandboxNetworkConfig_DnsServers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		stored         *[]string
		update         *[]string
		wantStored     *[]string
		wantNameserver bool
	}{
		{"omitted resets an empty list to the default", &[]string{}, nil, nil, true},
		{"listed default server reopens it", &[]string{}, &[]string{"8.8.8.8"}, &[]string{"8.8.8.8"}, true},
		{"empty list closes the default nameserver", nil, &[]string{}, &[]string{}, false},
		{"other server replaces the default", nil, &[]string{"1.1.1.1"}, &[]string{"1.1.1.1"}, false},
		{"omitted keeps the default", nil, nil, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sbx := sandbox.Sandbox{
				SandboxID: "sbx-dns", TeamID: uuid.New(), NodeID: "node-dns",
				ClusterID: uuid.Nil, State: sandbox.StateRunning,
				StartTime: time.Now(), EndTime: time.Now().Add(time.Hour), MaxInstanceLength: time.Hour,
				Network: &types.SandboxNetworkConfig{Egress: &types.SandboxNetworkEgressConfig{
					AllowedAddresses: []string{"example.com"},
					DeniedAddresses:  []string{sandbox_network.AllInternetTrafficCIDR},
					DnsServers:       tt.stored,
				}},
			}
			storage := &updateRejectionStorage{sbx: sbx}
			o := &Orchestrator{
				sandboxStore: sandbox.NewStore(storage, nil, sandbox.Callbacks{}),
				nodes:        smap.New[*nodemanager.Node](),
			}
			client := &capturingSandboxUpdateClient{}
			node := nodemanager.NewTestNode(sbx.NodeID, api.NodeStatusReady, 0, 2)
			node.ClusterID = sbx.ClusterID
			node.SetSandboxClient(client)
			o.registerNode(node)

			apiErr := o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID,
				[]string{"example.com"}, []string{sandbox_network.AllInternetTrafficCIDR}, nil, nil, tt.update, nil)
			require.Nil(t, apiErr)

			require.NotNil(t, storage.sbx.Network)
			require.NotNil(t, storage.sbx.Network.Egress)
			assert.Equal(t, tt.wantStored, storage.sbx.Network.Egress.DnsServers)

			require.NotNil(t, client.req, "the update must reach the node")
			cidrs := client.req.GetEgress().GetAllowedCidrs()
			if tt.wantNameserver {
				assert.Contains(t, cidrs, defaultNameserverCIDR)
			} else {
				assert.NotContains(t, cidrs, defaultNameserverCIDR)
			}
			for _, server := range utils.DerefOrDefault(tt.update, nil) {
				assert.Contains(t, cidrs, server+"/32")
			}
		})
	}
}

// A checkpoint records the network of the execution it left running, as
// the node reports it, in the write that puts the record back to Running,
// so the proxy check follows the VM that runs. A sandbox the Router served,
// resumed in a new VM on the worker's network, takes a proxy afterwards.
// One placed on the Router again still refuses a proxy before anything is
// stored, and still takes its removal. A node that predates the report and
// a placement of another execution leave the recorded placement, and a
// refused checkpoint records nothing.
func TestCheckpointRecordsTheRunningNetwork(t *testing.T) {
	t.Parallel()

	proxy := &sandbox_network.EgressProxyConfig{Address: "proxy.example.com:1080"}
	for name, c := range checkpointCallers {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			f, client := checkpointFixture(t, func(context.Context) error { return nil })
			placed := routedPlacement(f.sbx.ExecutionID)
			setPlacement(t, f, placed)
			checkpoint := func(response *orchestratorgrpc.SandboxCheckpointResponse) {
				t.Helper()
				client.response = response
				require.NoError(t, c.call(t.Context(), f.o, f.sbx.TeamID, f.sbx.SandboxID))
			}

			checkpoint(&orchestratorgrpc.SandboxCheckpointResponse{})
			assert.True(t, proto.Equal(placed, runningPlacement(t, f)), "a node that predates the report leaves the placement")
			checkpoint(reportedNetwork(routedPlacement(uuid.NewString())))
			assert.True(t, proto.Equal(placed, runningPlacement(t, f)), "a placement of another execution is refused")

			checkpoint(reportedNetwork(nil))
			require.Nil(t, runningPlacement(t, f), "the replacement runs on the worker's network")
			require.Nil(t, f.o.UpdateSandboxNetworkConfig(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, nil, nil, nil, nil, nil, proxy),
				"the replacement takes a proxy")

			setPlacement(t, f, nil)
			checkpoint(reportedNetwork(placed))
			assert.True(t, proto.Equal(placed, runningPlacement(t, f)), "a replacement placed on the Router again is recorded")
			apiErr := f.o.UpdateSandboxNetworkConfig(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, []string{"example.com"}, nil, nil, nil, nil, proxy)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Code, "and refuses a proxy before anything is stored")
			stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
			require.NoError(t, err)
			assert.Equal(t, proxy.Address, stored.Network.Egress.EgressProxyAddress, "the stored configuration is the earlier one")
			require.Nil(t, f.o.UpdateSandboxNetworkConfig(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, nil, nil, nil, nil, nil, nil), "and takes the proxy's removal")

			client.checkpoint = func(context.Context) error {
				return status.Error(codes.FailedPrecondition, "envd too old")
			}
			client.response = reportedNetwork(nil)
			require.Error(t, c.call(t.Context(), f.o, f.sbx.TeamID, f.sbx.SandboxID))
			assert.True(t, proto.Equal(placed, runningPlacement(t, f)), "a refused checkpoint records nothing")
		})
	}
}

// A completed checkpoint whose record cannot be written back leaves it
// Snapshotting with its old placement, instead of Running on a network its
// VM left, and says so: a proxy update is refused as a state change, not
// against the stale placement, and the sandbox is not killed. A successor
// that replaces the record just before the write keeps its record as it
// wrote it, and the completion changes nothing of it.
func TestCheckpointNeverServesAStalePlacement(t *testing.T) {
	t.Parallel()

	for name, c := range checkpointCallers {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			hook := &checkpointRestoreHook{}
			f, client := checkpointFixture(t, func(context.Context) error { return nil }, hook)
			placed := routedPlacement(f.sbx.ExecutionID)
			setPlacement(t, f, placed)
			client.response = reportedNetwork(nil)

			hook.fail = true
			hook.armed.Store(true)
			err := c.call(t.Context(), f.o, f.sbx.TeamID, f.sbx.SandboxID)
			require.ErrorContains(t, err, "the record store is unavailable")
			stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
			require.NoError(t, err)
			assert.Equal(t, sandbox.StateSnapshotting, stored.State)
			assert.True(t, proto.Equal(placed, stored.NetworkPlacement))
			assert.Zero(t, client.deletes.Load(), "the healthy VM is not killed")
			apiErr := f.o.UpdateSandboxNetworkConfig(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, nil, nil, nil, nil, nil,
				&sandbox_network.EgressProxyConfig{Address: "proxy.example.com:1080"})
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusConflict, apiErr.Code)
			require.NoError(t, f.o.sandboxStore.WaitForStateChange(t.Context(), f.sbx.TeamID, f.sbx.SandboxID), "the transition ended")

			// The record goes back to Running for the next case, and a
			// successor replaces it just before the checkpoint's write.
			_, err = f.o.sandboxStore.Update(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, func(sbx sandbox.Sandbox) (sandbox.Sandbox, error) {
				sbx.State = sandbox.StateRunning

				return sbx, nil
			})
			require.NoError(t, err)
			successor := successorOf(f.sbx)
			successor.NetworkPlacement = routedPlacement(successor.ExecutionID)
			var written sandbox.Sandbox
			hook.fail = false
			hook.before = func(ctx context.Context) error {
				if err := f.o.sandboxStore.Add(ctx, successor, nil); err != nil {
					return err
				}
				var err error
				written, err = f.o.sandboxStore.Get(ctx, f.sbx.TeamID, f.sbx.SandboxID)

				return err
			}
			hook.armed.Store(true)
			require.NoError(t, c.call(t.Context(), f.o, f.sbx.TeamID, f.sbx.SandboxID))
			require.NoError(t, f.o.sandboxStore.WaitForStateChange(t.Context(), f.sbx.TeamID, f.sbx.SandboxID))
			after, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
			require.NoError(t, err)
			assert.Equal(t, successor.ExecutionID, after.ExecutionID)
			assert.Equal(t, written, after, "the successor's record is as it wrote it")
		})
	}
}

func TestCheckpointRestoreConflictAndStateChange(t *testing.T) {
	t.Parallel()

	for caller, c := range checkpointCallers {
		for _, state := range []sandbox.State{sandbox.StateSnapshotting, sandbox.StateKilling} {
			t.Run(caller+"/"+string(state), func(t *testing.T) {
				t.Parallel()

				hook := &checkpointRestoreHook{}
				f, client := checkpointFixture(t, func(context.Context) error { return nil }, hook)
				setPlacement(t, f, routedPlacement(f.sbx.ExecutionID))
				client.response = reportedNetwork(nil)
				var written sandbox.Sandbox
				rewrites := 0
				hook.before = func(ctx context.Context) error {
					var err error
					written, err = f.o.sandboxStore.Get(ctx, f.sbx.TeamID, f.sbx.SandboxID)
					if err != nil {
						return err
					}
					rewrites++
					written.State = state
					written.Metadata = map[string]string{"rewrite": strconv.Itoa(rewrites)}
					// Keep rewriting Snapshotting under both compares. Killing
					// models another operation taking over before restoration.
					hook.armed.Store(state == sandbox.StateSnapshotting)

					return f.o.sandboxStore.Add(ctx, written, nil)
				}
				hook.armed.Store(true)

				err := c.call(t.Context(), f.o, f.sbx.TeamID, f.sbx.SandboxID)
				if state == sandbox.StateSnapshotting {
					require.ErrorIs(t, err, sandbox.ErrRestoreConflict, "a failed restore must not report checkpoint success")
					assert.GreaterOrEqual(t, rewrites, 2, "both compare attempts failed")
				} else {
					require.NoError(t, err, "a completed checkpoint yields to the operation that took over")
				}
				stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
				require.NoError(t, err)
				assert.Equal(t, written, stored, "neither restoration nor completion overwrites the record")
				assert.Zero(t, client.deletes.Load(), "restoration never kills the VM")
				require.NoError(t, f.o.sandboxStore.WaitForStateChange(t.Context(), f.sbx.TeamID, f.sbx.SandboxID))
			})
		}
	}
}

func routedPlacement(execution string) *orchestratorgrpc.SandboxNetworkPlacement {
	return &orchestratorgrpc.SandboxNetworkPlacement{ExecutionId: execution, NetworkMode: "egress-router", RouterId: "router"}
}

func reportedNetwork(placement *orchestratorgrpc.SandboxNetworkPlacement) *orchestratorgrpc.SandboxCheckpointResponse {
	return &orchestratorgrpc.SandboxCheckpointResponse{Network: &orchestratorgrpc.SandboxCheckpointNetwork{Placement: placement}}
}

func setPlacement(t *testing.T, f refusalFixture, placement *orchestratorgrpc.SandboxNetworkPlacement) {
	t.Helper()
	_, err := f.o.sandboxStore.Update(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, func(sbx sandbox.Sandbox) (sandbox.Sandbox, error) {
		sbx.NetworkPlacement = placement

		return sbx, nil
	})
	require.NoError(t, err)
}

// runningPlacement returns the placement of the fixture's record, which must
// be Running.
func runningPlacement(t *testing.T, f refusalFixture) *orchestratorgrpc.SandboxNetworkPlacement {
	t.Helper()
	stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
	require.NoError(t, err)
	require.Equal(t, sandbox.StateRunning, stored.State)

	return stored.NetworkPlacement
}

// refusingSandboxUpdateClient answers every update with the node's refusal.
type refusingSandboxUpdateClient struct {
	orchestratorgrpc.SandboxServiceClient
}

func (refusingSandboxUpdateClient) Update(context.Context, *orchestratorgrpc.SandboxUpdateRequest, ...grpc.CallOption) (*emptypb.Empty, error) {
	return nil, status.Error(codes.InvalidArgument, "the node cannot apply this config")
}

// A config the node refuses is answered with 400 and the node's reason, and
// it is never stored, so a later snapshot cannot carry a config that never
// took effect.
func TestUpdateSandboxNetworkConfig_NodeRefusalIsNeverStored(t *testing.T) {
	t.Parallel()

	previous := &types.SandboxNetworkEgressConfig{AllowedAddresses: []string{"example.com"}, DeniedAddresses: []string{sandbox_network.AllInternetTrafficCIDR}}
	allow := false
	sbx := sandbox.Sandbox{
		SandboxID: "sbx-refused", TeamID: uuid.New(), NodeID: "node-refused",
		ClusterID: uuid.Nil, State: sandbox.StateRunning,
		StartTime: time.Now(), EndTime: time.Now().Add(time.Hour), MaxInstanceLength: time.Hour,
		Network:             &types.SandboxNetworkConfig{Egress: previous},
		AllowInternetAccess: &allow,
	}
	storage := &updateRejectionStorage{sbx: sbx}
	o := &Orchestrator{
		sandboxStore: sandbox.NewStore(storage, nil, sandbox.Callbacks{}),
		nodes:        smap.New[*nodemanager.Node](),
	}
	node := nodemanager.NewTestNode(sbx.NodeID, api.NodeStatusReady, 0, 2)
	node.ClusterID = sbx.ClusterID
	node.SetSandboxClient(refusingSandboxUpdateClient{})
	o.registerNode(node)

	open := true
	apiErr := o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID,
		[]string{"other.example.com"}, nil, nil, &open, nil, nil)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusBadRequest, apiErr.Code)
	assert.Equal(t, "the node cannot apply this config", apiErr.ClientMsg)

	require.NotNil(t, storage.sbx.Network)
	assert.Equal(t, previous, storage.sbx.Network.Egress, "the refused config is not stored")
	require.NotNil(t, storage.sbx.AllowInternetAccess)
	assert.False(t, *storage.sbx.AllowInternetAccess)
}

// serializingStorage holds a record lock across each Update, as the Redis
// storage does. An Update that finds the lock held signals queued before it
// waits; each written record's first allowed address is offered on written.
// Get reads without the lock, as a Redis read does.
type serializingStorage struct {
	sandbox.Storage

	lock    sync.Mutex
	mu      sync.Mutex
	sbx     sandbox.Sandbox
	queued  chan struct{}
	written chan string
}

func (s *serializingStorage) Get(context.Context, uuid.UUID, string) (sandbox.Sandbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.sbx, nil
}

func (s *serializingStorage) Update(_ context.Context, _ uuid.UUID, _ string, update func(sandbox.Sandbox) (sandbox.Sandbox, error)) (sandbox.Sandbox, error) {
	if !s.lock.TryLock() {
		s.queued <- struct{}{}
		s.lock.Lock()
	}

	// The record is a copy, as a Redis read gives one.
	s.mu.Lock()
	current := s.sbx
	if s.sbx.Network != nil {
		network := *s.sbx.Network
		current.Network = &network
	}
	s.mu.Unlock()
	next, err := update(current)
	if err != nil {
		s.lock.Unlock()

		return sandbox.Sandbox{}, err
	}
	s.mu.Lock()
	s.sbx = next
	s.mu.Unlock()
	s.lock.Unlock()
	if next.Network != nil && next.Network.Egress != nil && len(next.Network.Egress.AllowedAddresses) > 0 {
		select {
		case s.written <- next.Network.Egress.AllowedAddresses[0]:
		default:
		}
	}

	return next, nil
}

// gatedRefusingClient signals each update's allowed domain and refuses it
// once the test releases that domain.
type gatedRefusingClient struct {
	orchestratorgrpc.SandboxServiceClient

	entered chan string
	release map[string]chan struct{}
}

func (c *gatedRefusingClient) Update(ctx context.Context, req *orchestratorgrpc.SandboxUpdateRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	domains := req.GetEgress().GetAllowedDomains()
	if len(domains) != 1 || c.release[domains[0]] == nil {
		return nil, status.Errorf(codes.Internal, "unexpected allowed domains %v", domains)
	}

	select {
	case c.entered <- domains[0]:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-c.release[domains[0]]:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return nil, status.Error(codes.InvalidArgument, "the node cannot apply this config")
}

// signalTimeout only turns a hang into a failure; no step is ordered by a timer.
const signalTimeout = 5 * time.Second

// receiveSignal waits for the next value on ch.
func receiveSignal[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()

	select {
	case v := <-ch:
		return v
	case <-time.After(signalTimeout):
		require.FailNow(t, "timed out waiting for "+what)

		var zero T

		return zero
	}
}

// Two overlapping updates the node both refuses leave the stored network as
// it was: the node is called under the store lock, before anything is stored.
// B reaches the store while A's node call is pending, and B's refusal comes
// only after A has returned: the interleaving in which a store-then-apply
// handler that restores on refusal leaves A's refused config stored.
func TestUpdateSandboxNetworkConfig_OverlappingRefusalsLeaveTheStoredNetwork(t *testing.T) {
	t.Parallel()

	const a, b = "a.example.com", "b.example.com"
	previous := &types.SandboxNetworkEgressConfig{AllowedAddresses: []string{"o.example.com"}}
	sbx := sandbox.Sandbox{
		SandboxID: "sbx-overlap", TeamID: uuid.New(), NodeID: "node-overlap",
		ClusterID: uuid.Nil, State: sandbox.StateRunning,
		StartTime: time.Now(), EndTime: time.Now().Add(time.Hour), MaxInstanceLength: time.Hour,
		Network: &types.SandboxNetworkConfig{Egress: previous},
	}
	storage := &serializingStorage{sbx: sbx, queued: make(chan struct{}, 4), written: make(chan string, 4)}
	o := &Orchestrator{
		sandboxStore: sandbox.NewStore(storage, nil, sandbox.Callbacks{}),
		nodes:        smap.New[*nodemanager.Node](),
	}
	client := &gatedRefusingClient{
		entered: make(chan string, 2),
		release: map[string]chan struct{}{a: make(chan struct{}), b: make(chan struct{})},
	}
	node := nodemanager.NewTestNode(sbx.NodeID, api.NodeStatusReady, 0, 2)
	node.ClusterID = sbx.ClusterID
	node.SetSandboxClient(client)
	o.registerNode(node)

	update := func(domain string) <-chan int {
		done := make(chan int, 1)
		go func() {
			code := 0
			if apiErr := o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID, []string{domain}, nil, nil, nil, nil, nil); apiErr != nil {
				code = apiErr.Code
			}
			done <- code
		}()

		return done
	}

	aDone := update(a)
	require.Equal(t, a, receiveSignal(t, client.entered, "A's node call"))

	// B is queued behind A's lock or, under a handler that stores before it
	// calls the node, already stored.
	bDone := update(b)
	deadline := time.After(signalTimeout)
	for arrived := false; !arrived; {
		select {
		case <-storage.queued:
			arrived = true
		case v := <-storage.written:
			arrived = v == b
		case got := <-client.entered:
			require.Failf(t, "the node was called before A's call returned", "called for %s", got)
		case <-deadline:
			require.FailNow(t, "timed out waiting for B's store update")
		}
	}
	select {
	case got := <-client.entered:
		require.Failf(t, "the node was called before A's call returned", "called for %s", got)
	default:
	}

	close(client.release[a])
	assert.Equal(t, http.StatusBadRequest, receiveSignal(t, aDone, "A's answer"))
	require.Equal(t, b, receiveSignal(t, client.entered, "B's node call"))
	close(client.release[b])
	assert.Equal(t, http.StatusBadRequest, receiveSignal(t, bDone, "B's answer"))

	storage.mu.Lock()
	defer storage.mu.Unlock()
	require.NotNil(t, storage.sbx.Network)
	require.NotNil(t, storage.sbx.Network.Egress)
	assert.Equal(t, []string{"o.example.com"}, storage.sbx.Network.Egress.AllowedAddresses, "no refused config is left stored")
}

// deadlineRecordingClient records the deadline of the node call and refuses it.
type deadlineRecordingClient struct {
	orchestratorgrpc.SandboxServiceClient

	deadline time.Time
	ok       bool
}

func (c *deadlineRecordingClient) Update(ctx context.Context, _ *orchestratorgrpc.SandboxUpdateRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	c.deadline, c.ok = ctx.Deadline()

	return nil, status.Error(codes.InvalidArgument, "refused")
}

// The node call made under the store lock carries the bound, so a slow node
// holds the lock for at most networkUpdateNodeTimeout.
func TestUpdateSandboxNetworkConfig_NodeCallIsBounded(t *testing.T) {
	t.Parallel()

	sbx := sandbox.Sandbox{
		SandboxID: "sbx-bounded", TeamID: uuid.New(), NodeID: "node-bounded",
		ClusterID: uuid.Nil, State: sandbox.StateRunning,
		StartTime: time.Now(), EndTime: time.Now().Add(time.Hour), MaxInstanceLength: time.Hour,
	}
	storage := &updateRejectionStorage{sbx: sbx}
	o := &Orchestrator{
		sandboxStore: sandbox.NewStore(storage, nil, sandbox.Callbacks{}),
		nodes:        smap.New[*nodemanager.Node](),
	}
	client := &deadlineRecordingClient{}
	node := nodemanager.NewTestNode(sbx.NodeID, api.NodeStatusReady, 0, 2)
	node.ClusterID = sbx.ClusterID
	node.SetSandboxClient(client)
	o.registerNode(node)

	start := time.Now()
	apiErr := o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID, []string{"example.com"}, nil, nil, nil, nil, nil)
	require.NotNil(t, apiErr)
	require.True(t, client.ok, "the node call has a deadline")
	assert.LessOrEqual(t, client.deadline.Sub(start), networkUpdateNodeTimeout+time.Second)
	assert.Less(t, networkUpdateNodeTimeout, time.Minute/2, "the bound stays well inside the store lock's expiry")
}

// lockCountingStorage serves Get and counts the Updates, which take the
// sandbox's store lock.
type lockCountingStorage struct {
	sandbox.Storage

	sbx     sandbox.Sandbox
	updates int
}

func (s *lockCountingStorage) Get(context.Context, uuid.UUID, string) (sandbox.Sandbox, error) {
	return s.sbx, nil
}

func (s *lockCountingStorage) Update(_ context.Context, _ uuid.UUID, _ string, update func(sandbox.Sandbox) (sandbox.Sandbox, error)) (sandbox.Sandbox, error) {
	s.updates++
	next, err := update(s.sbx)
	if err == nil {
		s.sbx = next
	}

	return next, err
}

// A node the cache misses is discovered before the store lock is taken: the
// lock is never held while discovery runs, and an unreachable node fails the
// update without entering it.
func TestUpdateSandboxNetworkConfig_NodeDiscoveryRunsOutsideTheLock(t *testing.T) {
	t.Parallel()

	sbx := sandbox.Sandbox{
		SandboxID: "sbx-discovery", TeamID: uuid.New(), NodeID: "node-uncached",
		ClusterID: uuid.New(), State: sandbox.StateRunning,
		StartTime: time.Now(), EndTime: time.Now().Add(time.Hour), MaxInstanceLength: time.Hour,
	}
	storage := &lockCountingStorage{sbx: sbx}
	o := &Orchestrator{
		sandboxStore: sandbox.NewStore(storage, nil, sandbox.Callbacks{}),
		nodes:        smap.New[*nodemanager.Node](),
		clusters:     clusters.NewTestPool(),
	}

	apiErr := o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID, []string{"example.com"}, nil, nil, nil, nil, nil)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusInternalServerError, apiErr.Code)
	assert.Zero(t, storage.updates, "the store lock is not taken while the node is discovered")
}

// nomadDiscovery lists no orchestrator instances, calling onList first so a
// test can register the node that discovery would connect.
func nomadDiscovery(t *testing.T, onList func()) servicediscovery.Discoverer {
	t.Helper()

	client := newNomadMock(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/service/orchestrator" {
			http.NotFound(w, r)

			return
		}
		if onList != nil {
			onList()
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	})

	return nomad.NewServices(client, []string{"orchestrator"})
}

// A config that needs a newer node, sent to a sandbox whose node the cache
// has not seen yet, discovers the node and is checked against it, instead of
// being refused as unavailable.
func TestUpdateSandboxNetworkConfig_VersionCheckFollowsDiscovery(t *testing.T) {
	t.Parallel()

	sbx := sandbox.Sandbox{
		SandboxID: "sbx-undiscovered", TeamID: uuid.New(), NodeID: "node-undiscovered",
		ClusterID: consts.LocalClusterID, State: sandbox.StateRunning,
		StartTime: time.Now(), EndTime: time.Now().Add(time.Hour), MaxInstanceLength: time.Hour,
	}
	storage := &hookedStorage{sbx: sbx}
	o := &Orchestrator{
		sandboxStore: sandbox.NewStore(storage, nil, sandbox.Callbacks{}),
		nodes:        smap.New[*nodemanager.Node](),
	}
	client := &capturingSandboxUpdateClient{}
	node := nodemanager.NewTestNode(sbx.NodeID, api.NodeStatusReady, 0, 2, nodemanager.WithOrchestratorVersion(buildWithEgressProxyTLS))
	node.ClusterID = sbx.ClusterID
	node.SetSandboxClient(client)
	var listed atomic.Int32
	o.nodeDiscovery = nomadDiscovery(t, func() {
		listed.Add(1)
		o.registerNode(node)
	})

	apiErr := o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID, nil, nil, nil, nil, nil, tlsProxy())
	require.Nil(t, apiErr, "the discovered node supports TLS")
	assert.Equal(t, int32(1), listed.Load(), "the node is discovered once")
	require.NotNil(t, client.req, "the discovered node receives the update")
	require.NotNil(t, storage.sbx.Network)
	assert.True(t, storage.sbx.Network.HasEgressProxyTLS())
}

// hookedStorage runs hook once the record's lock is held, before the update
// callback, as the Redis storage would between Obtain and the callback.
type hookedStorage struct {
	sandbox.Storage

	sbx  sandbox.Sandbox
	hook func()
}

func (s *hookedStorage) Get(context.Context, uuid.UUID, string) (sandbox.Sandbox, error) {
	return s.sbx, nil
}

func (s *hookedStorage) Update(_ context.Context, _ uuid.UUID, _ string, update func(sandbox.Sandbox) (sandbox.Sandbox, error)) (sandbox.Sandbox, error) {
	if s.hook != nil {
		s.hook()
	}
	next, err := update(s.sbx)
	if err == nil {
		s.sbx = next
	}

	return next, err
}

// closedSandboxUpdateClient is the client of a node that was closed.
type closedSandboxUpdateClient struct {
	orchestratorgrpc.SandboxServiceClient
}

func (closedSandboxUpdateClient) Update(context.Context, *orchestratorgrpc.SandboxUpdateRequest, ...grpc.CallOption) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unavailable, "grpc: the client connection is closing")
}

// A node replaced under the same ids while the update waited for the lock is
// the one the update calls: the callback reads the cache again instead of
// using the node found before the lock.
func TestUpdateSandboxNetworkConfig_UsesTheNodeRegisteredUnderTheLock(t *testing.T) {
	t.Parallel()

	sbx := sandbox.Sandbox{
		SandboxID: "sbx-replaced", TeamID: uuid.New(), NodeID: "node-replaced",
		ClusterID: uuid.Nil, State: sandbox.StateRunning,
		StartTime: time.Now(), EndTime: time.Now().Add(time.Hour), MaxInstanceLength: time.Hour,
	}
	storage := &hookedStorage{sbx: sbx}
	o := &Orchestrator{
		sandboxStore: sandbox.NewStore(storage, nil, sandbox.Callbacks{}),
		nodes:        smap.New[*nodemanager.Node](),
	}
	old := nodemanager.NewTestNode(sbx.NodeID, api.NodeStatusReady, 0, 2)
	old.ClusterID = sbx.ClusterID
	old.SetSandboxClient(closedSandboxUpdateClient{})
	o.registerNode(old)

	replacementClient := &capturingSandboxUpdateClient{}
	storage.hook = func() {
		o.deregisterNode(old)
		replacement := nodemanager.NewTestNode(sbx.NodeID, api.NodeStatusReady, 0, 2)
		replacement.ClusterID = sbx.ClusterID
		replacement.SetSandboxClient(replacementClient)
		o.registerNode(replacement)
	}

	apiErr := o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID, []string{"example.com"}, nil, nil, nil, nil, nil)
	require.Nil(t, apiErr)
	require.NotNil(t, replacementClient.req, "the replacement node receives the update")
	require.NotNil(t, storage.sbx.Network)
	assert.Equal(t, []string{"example.com"}, storage.sbx.Network.Egress.AllowedAddresses)
}

// unwritableStorage runs the update callback, so the node applies the config,
// and then fails the write, as a store whose writes keep failing would.
type unwritableStorage struct {
	sandbox.Storage

	sbx sandbox.Sandbox
}

func (s *unwritableStorage) Get(context.Context, uuid.UUID, string) (sandbox.Sandbox, error) {
	return s.sbx, nil
}

func (s *unwritableStorage) Update(_ context.Context, _ uuid.UUID, _ string, update func(sandbox.Sandbox) (sandbox.Sandbox, error)) (sandbox.Sandbox, error) {
	if _, err := update(s.sbx); err != nil {
		return sandbox.Sandbox{}, err
	}

	return sandbox.Sandbox{}, errors.New("failed to store sandbox in Redis: write failed")
}

// A config the node applied but the store could not record answers 500 with
// a request to send it again, and is logged and counted, so the mismatch
// between the live egress and the record is visible until the next update.
//
//nolint:paralleltest // replaces the global logger
func TestUpdateSandboxNetworkConfig_AppliedButUnstoredIsLoggedAndCounted(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	t.Cleanup(logger.ReplaceGlobals(t.Context(), logger.NewTracedLoggerFromCore(core)))

	sbx := sandbox.Sandbox{
		SandboxID: "sbx-unstored", TeamID: uuid.New(), NodeID: "node-unstored",
		ClusterID: uuid.Nil, State: sandbox.StateRunning,
		StartTime: time.Now(), EndTime: time.Now().Add(time.Hour), MaxInstanceLength: time.Hour,
	}
	reader := sdkmetric.NewManualReader()
	counter, err := telemetry.GetCounter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator"), telemetry.ApiOrchestratorNetworkUpdateUnstored)
	require.NoError(t, err)
	o := &Orchestrator{
		sandboxStore:                 sandbox.NewStore(&unwritableStorage{sbx: sbx}, nil, sandbox.Callbacks{}),
		nodes:                        smap.New[*nodemanager.Node](),
		networkUpdateUnstoredCounter: counter,
	}
	client := &capturingSandboxUpdateClient{}
	node := nodemanager.NewTestNode(sbx.NodeID, api.NodeStatusReady, 0, 2)
	node.ClusterID = sbx.ClusterID
	node.SetSandboxClient(client)
	o.registerNode(node)

	apiErr := o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID, []string{"example.com"}, nil, nil, nil, nil, nil)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusInternalServerError, apiErr.Code)
	assert.Contains(t, apiErr.ClientMsg, "send the update again")
	require.NotNil(t, client.req, "the node applied the config")

	entries := logs.FilterMessage("network update applied on the node but not stored").All()
	require.Len(t, entries, 1)
	assert.Equal(t, "sbx-unstored", entries[0].ContextMap()["sandbox.id"])

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))
	var total int64
	for _, sm := range collected.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.ApiOrchestratorNetworkUpdateUnstored) {
				continue
			}
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				total += dp.Value
			}
		}
	}
	assert.Equal(t, int64(1), total)
}

// isRecordWrite reports whether cmd is the store's fenced record write, a
// two-key script whose second key is not a lock key.
func isRecordWrite(cmd goredis.Cmder) bool {
	name := strings.ToLower(cmd.Name())
	args := cmd.Args()
	if (name != "evalsha" && name != "eval") || len(args) < 5 {
		return false
	}
	if n, ok := args[2].(int); !ok || n != 2 {
		return false
	}
	key, ok := args[4].(string)

	return ok && !strings.Contains(key, "lock:")
}

// failFirstSets fails the first n record writes the client sends.
type failFirstSets struct {
	n atomic.Int32
}

func (h *failFirstSets) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (h *failFirstSets) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}

func (h *failFirstSets) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		if isRecordWrite(cmd) && h.n.Add(-1) >= 0 {
			err := errors.New("injected SET failure")
			cmd.SetErr(err)

			return err
		}

		return next(ctx, cmd)
	}
}

// Against the Redis store, a network update whose record write fails twice
// and then lands succeeds, and the stored config is the one the node applied.
func TestUpdateSandboxNetworkConfig_TransientWriteFailureIsRetried(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage, err := sandboxredis.NewStorage(client, noop.NewMeterProvider())
	require.NoError(t, err)
	go storage.Start(t.Context())
	t.Cleanup(func() { storage.Close(context.WithoutCancel(t.Context())) })
	o := &Orchestrator{
		sandboxStore: sandbox.NewStore(storage, nil, sandbox.Callbacks{}),
		nodes:        smap.New[*nodemanager.Node](),
	}
	sbx := addSandboxInState(t, o, sandbox.StateRunning, "node-retry")
	nodeClient := &capturingSandboxUpdateClient{}
	node := nodemanager.NewTestNode(sbx.NodeID, api.NodeStatusReady, 0, 2)
	node.ClusterID = sbx.ClusterID
	node.SetSandboxClient(nodeClient)
	o.registerNode(node)

	hook := &failFirstSets{}
	hook.n.Store(2)
	client.AddHook(hook)

	apiErr := o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID, []string{"example.com"}, nil, nil, nil, nil, nil)
	require.Nil(t, apiErr)
	require.NotNil(t, nodeClient.req)

	stored, err := o.sandboxStore.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	require.NotNil(t, stored.Network)
	assert.Equal(t, []string{"example.com"}, stored.Network.Egress.AllowedAddresses)
}
