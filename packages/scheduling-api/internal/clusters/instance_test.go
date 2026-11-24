package clusters

import (
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	infogrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/servicetest"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func fullServiceInfo() *infogrpc.ServiceInfoResponse {
	return &infogrpc.ServiceInfoResponse{
		NodeId:                 "reported-node",
		ServiceId:              "service-1",
		ServiceVersion:         "1.2.3",
		ServiceCommit:          "abc123",
		ServiceStatus:          infogrpc.ServiceInfoStatus_Draining,
		ServiceRoles:           []infogrpc.ServiceInfoRole{infogrpc.ServiceInfoRole_Orchestrator, infogrpc.ServiceInfoRole_TemplateBuilder},
		ServiceStartup:         timestamppb.New(time.Unix(1_700_000_000, 0)),
		MachineInfo:            &infogrpc.MachineInfo{CpuArchitecture: "amd64", CpuFlags: []string{"avx2"}},
		Labels:                 []string{"pool-a"},
		ServiceStatusChangedAt: timestamppb.New(time.Unix(1_700_000_100, 0)),
		StatusOwner:            "operator",
		OutstandingWork:        3,
		MaxSandboxes:           40,
		Capabilities:           []string{"hugepages"},
		MetricSandboxesRunning: 7,
		MetricCpuCount:         16,
		MetricMemoryTotalBytes: 64 << 30,
	}
}

func newTestInstance(t *testing.T, response *infogrpc.ServiceInfoResponse) (*Instance, *fakeInfoServer, servicediscovery.Instance) {
	t.Helper()

	fake, address := startInfoServer(t, response)
	host, port, err := net.SplitHostPort(address)
	require.NoError(t, err)
	portNumber, err := strconv.ParseUint(port, 10, 16)
	require.NoError(t, err)
	discovered := servicediscovery.Instance{
		WorkloadID: "workload-1",
		NodeID:     "discovery-node",
		IPAddress:  host,
		Port:       uint16(portNumber),
		Backend:    servicediscovery.BackendKubernetes,
	}

	client, err := NewNodeClient(telemetry.NewNoopClient(), discovered.Address(), servicetest.NewCredentials())
	require.NoError(t, err)
	instance := NewInstance(uuid.New(), discovered, client)
	t.Cleanup(func() { _ = instance.Close() })

	return instance, fake, discovered
}

func TestInstanceBeforeFirstSyncHasOnlyDiscoveryFields(t *testing.T) {
	t.Parallel()

	instance, _, discovered := newTestInstance(t, fullServiceInfo())

	info := instance.Info()

	assert.Equal(t, discovered.NodeID, info.DiscoveryNodeID)
	assert.Empty(t, info.ReportedNodeID)
	assert.Nil(t, info.ServiceInfo)
	assert.True(t, info.SyncedAt.IsZero())
	require.NoError(t, info.SyncErr)
	assert.Equal(t, connectivity.Idle, info.State)
}

func TestInstanceFirstSyncFillsTheSnapshot(t *testing.T) {
	t.Parallel()

	want := fullServiceInfo()
	instance, _, discovered := newTestInstance(t, want)

	before := time.Now()
	require.NoError(t, instance.Sync(callContext(t)))
	info := instance.Info()

	assert.Equal(t, instance.clusterID, info.ClusterID)
	assert.Equal(t, "workload-1", info.WorkloadID)
	assert.Equal(t, "discovery-node", info.DiscoveryNodeID)
	assert.Equal(t, "reported-node", info.ReportedNodeID)
	assert.Equal(t, discovered.Address(), info.Address)
	assert.Equal(t, servicediscovery.BackendKubernetes, info.Backend)
	assert.True(t, proto.Equal(want, info.ServiceInfo), "got %v", info.ServiceInfo)
	assert.False(t, info.SyncedAt.Before(before))
	require.NoError(t, info.SyncErr)
	assert.Equal(t, connectivity.Ready, info.State)
}

func TestInstanceFailedSyncKeepsTheLastGoodInfo(t *testing.T) {
	t.Parallel()

	want := fullServiceInfo()
	instance, fake, _ := newTestInstance(t, want)
	require.NoError(t, instance.Sync(callContext(t)))
	good := instance.Info()

	fake.set(nil, status.Error(codes.Unavailable, "node is restarting"))
	err := instance.Sync(callContext(t))
	require.Error(t, err)
	failed := instance.Info()

	assert.Equal(t, codes.Unavailable, status.Code(failed.SyncErr))
	assert.Equal(t, good.SyncedAt, failed.SyncedAt)
	assert.Equal(t, "reported-node", failed.ReportedNodeID)
	assert.True(t, proto.Equal(want, failed.ServiceInfo), "got %v", failed.ServiceInfo)

	fake.set(want, nil)
	require.NoError(t, instance.Sync(callContext(t)))
	recovered := instance.Info()

	require.NoError(t, recovered.SyncErr)
	assert.True(t, recovered.SyncedAt.After(good.SyncedAt))
}

func TestInstanceSnapshotsDoNotAlias(t *testing.T) {
	t.Parallel()

	want := fullServiceInfo()
	instance, _, _ := newTestInstance(t, want)
	require.NoError(t, instance.Sync(callContext(t)))

	first := instance.Info()
	first.ServiceInfo.NodeId = "mutated"
	first.ServiceInfo.Labels[0] = "mutated"
	first.ServiceInfo.MachineInfo.CpuFlags[0] = "mutated"

	assert.True(t, proto.Equal(want, instance.Info().ServiceInfo))
}

func TestInstanceConcurrentSyncAndInfo(t *testing.T) {
	t.Parallel()

	instance, _, _ := newTestInstance(t, fullServiceInfo())
	ctx := callContext(t)

	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 20 {
				assert.NoError(t, instance.Sync(ctx))
			}
		})
		wg.Go(func() {
			for range 20 {
				info := instance.Info()
				if info.ServiceInfo != nil {
					info.ServiceInfo.Labels = nil
				}
			}
		})
	}
	wg.Wait()

	assert.Equal(t, "reported-node", instance.Info().ReportedNodeID)
}

func TestInstanceCloseShutsDownTheConnection(t *testing.T) {
	t.Parallel()

	instance, _, _ := newTestInstance(t, fullServiceInfo())
	require.NoError(t, instance.Sync(callContext(t)))

	require.NoError(t, instance.Close())

	assert.Equal(t, connectivity.Shutdown, instance.Info().State)
	assert.Equal(t, codes.Canceled, status.Code(instance.Sync(callContext(t))))
}
