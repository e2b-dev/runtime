package clusters

import (
	"context"
	"errors"
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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	infogrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const testGrace = time.Minute

type fakeDiscoverer struct {
	servicediscovery.NoSync

	mu        sync.Mutex
	instances []servicediscovery.Instance
	err       error
}

func (d *fakeDiscoverer) ListInstances(context.Context) ([]servicediscovery.Instance, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	return append([]servicediscovery.Instance(nil), d.instances...), d.err
}

func (d *fakeDiscoverer) set(instances ...servicediscovery.Instance) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.instances, d.err = instances, nil
}

func (d *fakeDiscoverer) fail(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.err = err
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

type nodesHarness struct {
	nodes     *Nodes
	discovery *fakeDiscoverer
	clock     *fakeClock

	mu      sync.Mutex
	clients map[string][]*NodeClient
}

func newNodesHarness(t *testing.T) *nodesHarness {
	t.Helper()

	h := &nodesHarness{
		discovery: &fakeDiscoverer{},
		clock:     &fakeClock{now: time.Unix(1_700_000_000, 0)},
		clients:   map[string][]*NodeClient{},
	}
	dial := func(item servicediscovery.Instance) (*NodeClient, error) {
		client, err := NewNodeClient(telemetry.NewNoopClient(), item.Address(), insecure.NewCredentials())
		if err != nil {
			return nil, err
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		h.clients[item.Address()] = append(h.clients[item.Address()], client)

		return client, nil
	}
	h.nodes = NewNodes(uuid.New(), h.discovery, dial, 5*time.Second, testGrace)
	h.nodes.now = h.clock.Now
	t.Cleanup(func() { h.nodes.Close(t.Context()) })

	return h
}

func (h *nodesHarness) sync(t *testing.T) {
	t.Helper()

	require.NoError(t, h.nodes.Sync(callContext(t)))
}

func (h *nodesHarness) clientsFor(address string) []*NodeClient {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([]*NodeClient(nil), h.clients[address]...)
}

func (h *nodesHarness) assertAllClosed(t *testing.T, address string) {
	t.Helper()

	clients := h.clientsFor(address)
	require.NotEmpty(t, clients)
	for _, client := range clients {
		assert.Equal(t, connectivity.Shutdown, client.State())
	}
}

func discoveredNode(t *testing.T, workloadID string, response *infogrpc.ServiceInfoResponse) (servicediscovery.Instance, *fakeInfoServer) {
	t.Helper()

	fake, address := startInfoServer(t, response)

	return discoveredAt(t, workloadID, address), fake
}

func discoveredAt(t *testing.T, workloadID, address string) servicediscovery.Instance {
	t.Helper()

	host, port, err := net.SplitHostPort(address)
	require.NoError(t, err)
	portNumber, err := strconv.ParseUint(port, 10, 16)
	require.NoError(t, err)

	return servicediscovery.Instance{
		WorkloadID: workloadID,
		NodeID:     "machine-" + workloadID,
		IPAddress:  host,
		Port:       uint16(portNumber),
		Backend:    servicediscovery.BackendKubernetes,
	}
}

func unavailable() error {
	return status.Error(codes.Unavailable, "node is down")
}

func TestNodesAddsAndSyncsDiscoveredNodes(t *testing.T) {
	t.Parallel()

	h := newNodesHarness(t)
	a, _ := discoveredNode(t, "a", &infogrpc.ServiceInfoResponse{NodeId: "node-a"})
	b, _ := discoveredNode(t, "b", &infogrpc.ServiceInfoResponse{NodeId: "node-b"})
	h.discovery.set(a, b)

	h.sync(t)

	reported := map[string]string{}
	for _, info := range h.nodes.List() {
		reported[info.WorkloadID] = info.ReportedNodeID
		assert.False(t, info.SyncedAt.IsZero())
	}
	assert.Equal(t, map[string]string{"a": "node-a", "b": "node-b"}, reported)

	info, ok := h.nodes.Get("a")
	require.True(t, ok)
	assert.Equal(t, a.Address(), info.Address)
	_, ok = h.nodes.Get("missing")
	assert.False(t, ok)
}

func TestNodesSkipsANodeUntilItsFirstSyncSucceeds(t *testing.T) {
	t.Parallel()

	h := newNodesHarness(t)
	a, fake := discoveredNode(t, "a", nil)
	fake.set(nil, unavailable())
	h.discovery.set(a)

	h.sync(t)

	assert.Empty(t, h.nodes.List())
	h.assertAllClosed(t, a.Address())

	fake.set(&infogrpc.ServiceInfoResponse{NodeId: "node-a"}, nil)
	h.sync(t)

	info, ok := h.nodes.Get("a")
	require.True(t, ok)
	assert.Equal(t, "node-a", info.ReportedNodeID)
}

func TestNodesReplacesANodeWhoseAddressChanged(t *testing.T) {
	t.Parallel()

	h := newNodesHarness(t)
	old, _ := discoveredNode(t, "a", &infogrpc.ServiceInfoResponse{NodeId: "old"})
	h.discovery.set(old)
	h.sync(t)

	moved, _ := discoveredNode(t, "a", &infogrpc.ServiceInfoResponse{NodeId: "new"})
	h.discovery.set(moved)
	h.sync(t)

	info, ok := h.nodes.Get("a")
	require.True(t, ok)
	assert.Equal(t, moved.Address(), info.Address)
	assert.Equal(t, "new", info.ReportedNodeID)
	assert.Len(t, h.nodes.List(), 1)
	h.assertAllClosed(t, old.Address())
}

func TestNodesKeepsAMissingNodeThatAnswersWithinGrace(t *testing.T) {
	t.Parallel()

	h := newNodesHarness(t)
	a, _ := discoveredNode(t, "a", &infogrpc.ServiceInfoResponse{NodeId: "node-a"})
	h.discovery.set(a)
	h.sync(t)

	h.discovery.set()
	h.sync(t)
	h.clock.advance(testGrace - time.Second)
	h.sync(t)

	info, ok := h.nodes.Get("a")
	require.True(t, ok)
	require.NoError(t, info.SyncErr)
	assert.Equal(t, connectivity.Ready, info.State)
}

func TestNodesRemovesAMissingNodeWhoseAddressAnotherNodeTook(t *testing.T) {
	t.Parallel()

	h := newNodesHarness(t)
	old, _ := discoveredNode(t, "old-pod", &infogrpc.ServiceInfoResponse{NodeId: "node-a"})
	h.discovery.set(old)
	h.sync(t)

	h.discovery.set(discoveredAt(t, "new-pod", old.Address()))
	h.sync(t)

	_, ok := h.nodes.Get("old-pod")
	assert.False(t, ok)
	_, ok = h.nodes.Get("new-pod")
	assert.True(t, ok)
	assert.Len(t, h.nodes.List(), 1)
}

func TestNodesRemovesAMissingNodeAfterGrace(t *testing.T) {
	t.Parallel()

	h := newNodesHarness(t)
	a, _ := discoveredNode(t, "a", &infogrpc.ServiceInfoResponse{NodeId: "node-a"})
	h.discovery.set(a)
	h.sync(t)

	h.discovery.set()
	h.sync(t)
	h.clock.advance(testGrace)
	h.sync(t)

	_, ok := h.nodes.Get("a")
	assert.False(t, ok)
	h.assertAllClosed(t, a.Address())
}

func TestNodesRemovesAMissingNodeThatStoppedAnswering(t *testing.T) {
	t.Parallel()

	h := newNodesHarness(t)
	a, fake := discoveredNode(t, "a", &infogrpc.ServiceInfoResponse{NodeId: "node-a"})
	h.discovery.set(a)
	h.sync(t)

	fake.set(nil, unavailable())
	h.sync(t)
	_, ok := h.nodes.Get("a")
	require.True(t, ok)

	h.discovery.set()
	h.sync(t)

	_, ok = h.nodes.Get("a")
	assert.False(t, ok)
	h.assertAllClosed(t, a.Address())
}

func TestNodesRemovesAMissingNodeOnTheRoundAfterItStopsAnswering(t *testing.T) {
	t.Parallel()

	h := newNodesHarness(t)
	a, fake := discoveredNode(t, "a", &infogrpc.ServiceInfoResponse{NodeId: "node-a"})
	h.discovery.set(a)
	h.sync(t)

	h.discovery.set()
	fake.set(nil, unavailable())
	h.sync(t)
	info, ok := h.nodes.Get("a")
	require.True(t, ok)
	require.Error(t, info.SyncErr)

	h.sync(t)

	_, ok = h.nodes.Get("a")
	assert.False(t, ok)
}

func TestNodesReappearingNodeResetsItsMissingTime(t *testing.T) {
	t.Parallel()

	h := newNodesHarness(t)
	a, _ := discoveredNode(t, "a", &infogrpc.ServiceInfoResponse{NodeId: "node-a"})
	h.discovery.set(a)
	h.sync(t)

	h.discovery.set()
	h.sync(t)
	h.clock.advance(testGrace - time.Second)
	h.discovery.set(a)
	h.sync(t)

	h.discovery.set()
	h.sync(t)
	h.clock.advance(testGrace - time.Second)
	h.sync(t)

	_, ok := h.nodes.Get("a")
	assert.True(t, ok)
	assert.Len(t, h.clientsFor(a.Address()), 1)
}

func TestNodesDiscoveryErrorRemovesNothing(t *testing.T) {
	t.Parallel()

	h := newNodesHarness(t)
	a, fake := discoveredNode(t, "a", &infogrpc.ServiceInfoResponse{NodeId: "node-a"})
	h.discovery.set(a)
	h.sync(t)

	h.discovery.fail(errors.New("kubernetes is down"))
	fake.set(nil, unavailable())
	require.Error(t, h.nodes.Sync(callContext(t)))
	h.clock.advance(10 * testGrace)
	require.Error(t, h.nodes.Sync(callContext(t)))

	info, ok := h.nodes.Get("a")
	require.True(t, ok)
	assert.Equal(t, "node-a", info.ReportedNodeID)
	assert.NotEqual(t, connectivity.Shutdown, info.State)
}

func TestNodesCloseClosesAllNodes(t *testing.T) {
	t.Parallel()

	h := newNodesHarness(t)
	a, _ := discoveredNode(t, "a", &infogrpc.ServiceInfoResponse{NodeId: "node-a"})
	b, _ := discoveredNode(t, "b", &infogrpc.ServiceInfoResponse{NodeId: "node-b"})
	h.discovery.set(a, b)
	h.sync(t)

	h.nodes.Close(t.Context())

	assert.Empty(t, h.nodes.List())
	h.assertAllClosed(t, a.Address())
	h.assertAllClosed(t, b.Address())
}

func TestNodesCloseDuringARoundAddsNoNode(t *testing.T) {
	t.Parallel()

	h := newNodesHarness(t)
	a, fake := discoveredNode(t, "a", &infogrpc.ServiceInfoResponse{NodeId: "node-a"})
	arrived, release := fake.hold()
	t.Cleanup(release)
	h.discovery.set(a)

	ctx := callContext(t)
	done := make(chan error, 1)
	go func() { done <- h.nodes.Sync(ctx) }()

	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		require.Fail(t, "the node was never synced")
	}

	h.nodes.Close(t.Context())
	release()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.Fail(t, "the round did not end")
	}

	assert.Empty(t, h.nodes.List())
	h.assertAllClosed(t, a.Address())
}

func TestNodesAddsANewNodeWhileAnotherHangsForTheWholeRound(t *testing.T) {
	t.Parallel()

	h := newNodesHarness(t)
	h.nodes.syncTimeout = time.Second
	hung, fake := discoveredNode(t, "hung", &infogrpc.ServiceInfoResponse{NodeId: "node-hung"})
	h.discovery.set(hung)
	h.sync(t)

	_, release := fake.hold()
	t.Cleanup(release)
	fresh, _ := discoveredNode(t, "fresh", &infogrpc.ServiceInfoResponse{NodeId: "node-fresh"})
	h.discovery.set(hung, fresh)

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	require.NoError(t, h.nodes.Sync(ctx))

	info, ok := h.nodes.Get("fresh")
	require.True(t, ok)
	assert.Equal(t, "node-fresh", info.ReportedNodeID)
}

func TestNodesAddsAReplacementOnlyAfterTheOldNodeLeavesItsAddress(t *testing.T) {
	t.Parallel()

	h := newNodesHarness(t)
	old, _ := discoveredNode(t, "old-pod", &infogrpc.ServiceInfoResponse{NodeId: "node-a"})
	replacement := discoveredAt(t, "new-pod", old.Address())
	h.discovery.set(old)
	h.sync(t)

	h.discovery.set(old, replacement)
	h.sync(t)

	nodes := h.nodes.List()
	require.Len(t, nodes, 1)
	assert.Equal(t, "old-pod", nodes[0].WorkloadID)

	h.discovery.set(replacement)
	h.sync(t)

	nodes = h.nodes.List()
	require.Len(t, nodes, 1)
	assert.Equal(t, "new-pod", nodes[0].WorkloadID)
	clients := h.clientsFor(old.Address())
	require.Len(t, clients, 2)
	assert.Equal(t, connectivity.Shutdown, clients[0].State())
}

func TestNodesAddsOneNodeForTwoNewItemsAtOneAddress(t *testing.T) {
	t.Parallel()

	h := newNodesHarness(t)
	a, _ := discoveredNode(t, "a", &infogrpc.ServiceInfoResponse{NodeId: "node-a"})
	h.discovery.set(a, discoveredAt(t, "b", a.Address()))

	h.sync(t)

	require.Len(t, h.nodes.List(), 1)
	clients := h.clientsFor(a.Address())
	require.Len(t, clients, 2)
	shutdown := 0
	for _, client := range clients {
		if client.State() == connectivity.Shutdown {
			shutdown++
		}
	}
	assert.Equal(t, 1, shutdown)
}
