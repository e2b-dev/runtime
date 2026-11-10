package clusters

import (
	"strconv"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	infogrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/machineinfo"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

// NewTestPool builds a Pool pre-populated with the given clusters, for use in
// tests that need cluster lookups (e.g. GetClusterById) without spinning up a
// full synchronization loop.
func NewTestPool(clusters ...*Cluster) *Pool {
	m := smap.New[*Cluster]()
	for _, c := range clusters {
		m.Insert(c.ID.String(), c)
	}

	return &Pool{clusters: m, localInstances: make(chan struct{}, 1)}
}

// NewTestCluster builds a minimal Cluster carrying just an ID and sandbox
// domain, for tests. Other collaborators (instances, synchronization,
// resources) are left nil.
func NewTestCluster(id uuid.UUID, sandboxDomain *string) *Cluster {
	return &Cluster{
		ID:            id,
		SandboxDomain: sandboxDomain,
	}
}

// NewTestSharedGRPCClient builds a client that issues its calls on conn the
// way a remote cluster's instances do, for tests: closing it retires the
// client and leaves conn open.
func NewTestSharedGRPCClient(conn *grpc.ClientConn) *GRPCClient {
	return newSharedGRPCClient(conn, "test", func() {})
}

// TestBuilder describes a template builder for NewTestBuilderCluster. An empty
// NodeID defaults to the builder's index; the zero Status is healthy.
type TestBuilder struct {
	Version string
	Machine machineinfo.MachineInfo
	Client  *GRPCClient
	NodeID  string
	Status  infogrpc.ServiceInfoStatus
}

// NewTestBuilderCluster builds a Cluster whose pool holds the given builders,
// for tests of builder selection.
func NewTestBuilderCluster(id uuid.UUID, builders ...TestBuilder) *Cluster {
	pool := smap.New[*Instance]()
	for i, b := range builders {
		key := b.NodeID
		if key == "" {
			key = strconv.Itoa(i)
		}
		pool.Insert(key, &Instance{
			workloadID:     key,
			ClusterID:      id,
			NodeID:         key,
			serviceVersion: b.Version,
			machine:        b.Machine,
			client:         b.Client,
			isBuilder:      true,
			status:         b.Status,
		})
	}

	return &Cluster{ID: id, instances: pool}
}
