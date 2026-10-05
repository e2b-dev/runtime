package nodemanager

import (
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

func TestMetricsOutstandingWork(t *testing.T) {
	t.Parallel()

	n := &Node{}
	for _, work := range []uint64{0, 7, math.MaxUint64, 0} {
		n.UpdateMetricsFromServiceInfoResponse(&orchestratorinfo.ServiceInfoResponse{OutstandingWork: work})
		require.Equal(t, work, n.Metrics().OutstandingWork)
	}
}

func TestMetricsMaxSandboxesSnapshotIsolation(t *testing.T) {
	t.Parallel()

	n := &Node{}
	info := &orchestratorinfo.ServiceInfoResponse{MaxSandboxes: 200}
	n.UpdateMetricsFromServiceInfoResponse(info)
	info.MaxSandboxes = 300
	require.Equal(t, int64(200), n.Metrics().MaxSandboxes)

	snapshot := n.Metrics()
	n.UpdateMetricsFromServiceInfoResponse(info)
	require.Equal(t, int64(200), snapshot.MaxSandboxes)
	require.Equal(t, int64(300), n.Metrics().MaxSandboxes)

	n.UpdateMetricsFromServiceInfoResponse(&orchestratorinfo.ServiceInfoResponse{})
	require.Zero(t, n.Metrics().MaxSandboxes)
	require.Equal(t, int64(200), snapshot.MaxSandboxes)
}

func TestMetricsOutstandingWorkSnapshotIsolation(t *testing.T) {
	t.Parallel()

	n := &Node{}
	info := &orchestratorinfo.ServiceInfoResponse{OutstandingWork: 7}
	n.UpdateMetricsFromServiceInfoResponse(info)
	info.OutstandingWork = 8
	require.Equal(t, uint64(7), n.Metrics().OutstandingWork)

	snapshot := n.Metrics()
	n.UpdateMetricsFromServiceInfoResponse(info)
	require.Equal(t, uint64(7), snapshot.OutstandingWork)
	require.Equal(t, uint64(8), n.Metrics().OutstandingWork)

	n.UpdateMetricsFromServiceInfoResponse(&orchestratorinfo.ServiceInfoResponse{})
	require.Zero(t, n.Metrics().OutstandingWork)
	require.Equal(t, uint64(7), snapshot.OutstandingWork)
}

func TestMetricsOutstandingWorkConcurrentAccess(t *testing.T) {
	t.Parallel()

	n := &Node{}
	n.UpdateMetricsFromServiceInfoResponse(&orchestratorinfo.ServiceInfoResponse{OutstandingWork: 7})

	var wg sync.WaitGroup
	wg.Go(func() {
		for work := range uint64(100) {
			n.UpdateMetricsFromServiceInfoResponse(&orchestratorinfo.ServiceInfoResponse{OutstandingWork: 1000 + work})
		}
	})
	defer wg.Wait()

	// The written counts sit in a band disjoint from the seed, so a torn read
	// lands outside both. The race detector covers unsynchronized access.
	for range 100 {
		work := n.Metrics().OutstandingWork
		if work != 7 {
			require.GreaterOrEqual(t, work, uint64(1000))
			require.Less(t, work, uint64(1100))
		}
	}
}
