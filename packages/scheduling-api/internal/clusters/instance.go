package clusters

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/protobuf/proto"

	infogrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery"
)

// Instance is one discovered node and the last ServiceInfo it reported.
type Instance struct {
	clusterID  uuid.UUID
	discovered servicediscovery.Instance
	client     *NodeClient

	mu       sync.RWMutex
	info     *infogrpc.ServiceInfoResponse
	syncedAt time.Time
	syncErr  error
}

// InstanceInfo is a snapshot of an Instance. It shares no memory with the
// Instance, so callers may keep or modify it.
type InstanceInfo struct {
	ClusterID uuid.UUID

	WorkloadID      string
	DiscoveryNodeID string
	Address         string
	Backend         string

	// ReportedNodeID is the node_id the node reports about itself. It can
	// differ from DiscoveryNodeID and is empty until the first successful sync.
	ReportedNodeID string
	// ServiceInfo is nil until the first successful sync.
	ServiceInfo *infogrpc.ServiceInfoResponse
	// SyncedAt is the time of the last successful sync; zero if none.
	SyncedAt time.Time
	// SyncErr is the error of the last sync; nil after a successful one.
	SyncErr error

	State connectivity.State
}

// NewInstance takes ownership of client: Close closes it.
func NewInstance(clusterID uuid.UUID, discovered servicediscovery.Instance, client *NodeClient) *Instance {
	return &Instance{
		clusterID:  clusterID,
		discovered: discovered,
		client:     client,
	}
}

// Sync asks the node for its ServiceInfo. A failed sync keeps the last
// good info and its time and records the error.
func (i *Instance) Sync(ctx context.Context) error {
	info, err := i.client.ServiceInfo(ctx)

	i.mu.Lock()
	defer i.mu.Unlock()

	i.syncErr = err
	if err != nil {
		return err
	}

	// gRPC decodes each response into a new message, so storing it as is
	// shares it with no one; Info clones it on the way out.
	i.info = info
	i.syncedAt = time.Now()

	return nil
}

func (i *Instance) Info() InstanceInfo {
	i.mu.RLock()
	defer i.mu.RUnlock()

	var info *infogrpc.ServiceInfoResponse
	if i.info != nil {
		info = proto.CloneOf(i.info)
	}

	return InstanceInfo{
		ClusterID:       i.clusterID,
		WorkloadID:      i.discovered.WorkloadID,
		DiscoveryNodeID: i.discovered.NodeID,
		Address:         i.discovered.Address(),
		Backend:         i.discovered.Backend,
		ReportedNodeID:  info.GetNodeId(),
		ServiceInfo:     info,
		SyncedAt:        i.syncedAt,
		SyncErr:         i.syncErr,
		State:           i.client.State(),
	}
}

func (i *Instance) Close() error {
	return i.client.Close()
}
