package clusters

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
	"github.com/e2b-dev/infra/packages/shared/pkg/synchronization"
)

// DialFunc opens a client to a discovered node.
type DialFunc func(servicediscovery.Instance) (*NodeClient, error)

// Nodes is the set of nodes of one cluster, kept in step with a discovery
// source. It is keyed by WorkloadID.
type Nodes struct {
	clusterID   uuid.UUID
	discovery   servicediscovery.Discoverer
	dial        DialFunc
	syncTimeout time.Duration
	grace       time.Duration
	now         func() time.Time

	nodes *smap.Map[*node]
	sync  *synchronization.Synchronize[servicediscovery.Instance, *node]

	// mu makes each insert atomic with its checks, against Close and
	// against concurrent inserts of the same round.
	mu     sync.Mutex
	closed bool
}

type node struct {
	*Instance

	// missingSince is only read and written inside a sync round, and the
	// rounds are serialized by the synchronization loop.
	missingSince time.Time
}

// NewNodes keeps a node that is missing from discovery for at most grace,
// and only while its last sync succeeded. Each node sync runs with syncTimeout.
func NewNodes(clusterID uuid.UUID, discovery servicediscovery.Discoverer, dial DialFunc, syncTimeout, grace time.Duration) *Nodes {
	n := &Nodes{
		clusterID:   clusterID,
		discovery:   discovery,
		dial:        dial,
		syncTimeout: syncTimeout,
		grace:       grace,
		now:         time.Now,
		nodes:       smap.New[*node](),
	}
	n.sync = synchronization.NewSynchronize("cluster-nodes", "Cluster nodes", nodesStore{n})

	return n
}

// Start runs a sync round now and then every interval until Close.
func (n *Nodes) Start(ctx context.Context, interval time.Duration) {
	go n.sync.Start(ctx, interval, interval, true)
}

func (n *Nodes) Sync(ctx context.Context) error {
	return n.sync.Sync(ctx)
}

func (n *Nodes) Close(ctx context.Context) {
	n.sync.Close()

	n.mu.Lock()
	n.closed = true
	n.mu.Unlock()

	for key, nd := range n.nodes.Items() {
		n.closeNode(ctx, nd)
		n.nodes.Remove(key)
	}
}

func (n *Nodes) List() []InstanceInfo {
	items := n.nodes.Items()
	infos := make([]InstanceInfo, 0, len(items))
	for _, nd := range items {
		infos = append(infos, nd.Info())
	}

	return infos
}

func (n *Nodes) Get(workloadID string) (InstanceInfo, bool) {
	nd, ok := n.nodes.Get(workloadID)
	if !ok {
		return InstanceInfo{}, false
	}

	return nd.Info(), true
}

// syncNode does not inherit the round deadline: a node that hangs for the
// whole round must not leave the nodes synced after it with no time.
func (n *Nodes) syncNode(ctx context.Context, nd *node) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), n.syncTimeout)
	defer cancel()

	return nd.Sync(ctx)
}

func (n *Nodes) addressHeld(address string) bool {
	for _, nd := range n.nodes.Items() {
		if nd.discovered.Address() == address {
			return true
		}
	}

	return false
}

func (n *Nodes) closeNode(ctx context.Context, nd *node) {
	if err := nd.Close(); err != nil {
		logger.L().Error(ctx, "Failed to close cluster node", zap.Error(err),
			logger.WithClusterID(n.clusterID), logger.WithNodeID(nd.discovered.NodeID))
	}
}

type nodesStore struct {
	*Nodes
}

func (s nodesStore) SourceList(ctx context.Context) ([]servicediscovery.Instance, error) {
	items, err := s.discovery.ListInstances(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list cluster nodes from service discovery: %w", err)
	}

	return items, nil
}

// SourceExists treats a node whose address changed as gone, so the round
// removes it and then inserts the new address under the same key. A missing
// node whose address another discovered node now uses is gone too: its syncs
// would be answered by that node.
func (s nodesStore) SourceExists(_ context.Context, source []servicediscovery.Instance, nd *node) bool {
	addressTaken := false
	for _, item := range source {
		if item.WorkloadID == nd.discovered.WorkloadID {
			nd.missingSince = time.Time{}

			return item.Address() == nd.discovered.Address()
		}
		if item.Address() == nd.discovered.Address() {
			addressTaken = true
		}
	}
	if addressTaken {
		return false
	}

	now := s.now()
	if nd.missingSince.IsZero() {
		nd.missingSince = now
	}

	return now.Sub(nd.missingSince) < s.grace && nd.Info().SyncErr == nil
}

func (s nodesStore) PoolList(_ context.Context) []*node {
	items := s.nodes.Items()
	nodes := make([]*node, 0, len(items))
	for _, nd := range items {
		nodes = append(nodes, nd)
	}

	return nodes
}

// PoolExists also reports an item whose address a pool node holds: an address
// belongs to one node, and the item is added once that node is gone.
func (s nodesStore) PoolExists(_ context.Context, item servicediscovery.Instance) bool {
	_, found := s.nodes.Get(item.WorkloadID)

	return found || s.addressHeld(item.Address())
}

// PoolInsert adds a node only after its first sync succeeds; a node that
// does not answer is retried on the next round. Of two new items at the same
// address in one round, the first to finish its sync is added.
func (s nodesStore) PoolInsert(ctx context.Context, item servicediscovery.Instance) {
	fields := []zap.Field{logger.WithClusterID(s.clusterID), logger.WithNodeID(item.NodeID), zap.String("address", item.Address())}

	client, err := s.dial(item)
	if err != nil {
		logger.L().Error(ctx, "Failed to dial cluster node", append(fields, zap.Error(err))...)

		return
	}

	nd := &node{Instance: NewInstance(s.clusterID, item, client)}
	if err := s.syncNode(ctx, nd); err != nil {
		logger.L().Error(ctx, "Failed to sync new cluster node", append(fields, zap.Error(err))...)
		s.closeNode(ctx, nd)

		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || s.addressHeld(item.Address()) {
		s.closeNode(ctx, nd)

		return
	}

	logger.L().Info(ctx, "Adding node into cluster pool", fields...)
	s.nodes.Insert(item.WorkloadID, nd)
}

func (s nodesStore) PoolUpdate(ctx context.Context, nd *node) {
	if err := s.syncNode(ctx, nd); err != nil {
		logger.L().Error(ctx, "Failed to sync cluster node", zap.Error(err),
			logger.WithClusterID(s.clusterID), logger.WithNodeID(nd.discovered.NodeID))
	}
}

func (s nodesStore) PoolRemove(ctx context.Context, nd *node) {
	logger.L().Info(ctx, "Removing node from cluster pool",
		logger.WithClusterID(s.clusterID), logger.WithNodeID(nd.discovered.NodeID), zap.String("address", nd.discovered.Address()))

	s.nodes.Remove(nd.discovered.WorkloadID)
	s.closeNode(ctx, nd)
}
