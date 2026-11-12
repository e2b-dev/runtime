package nodemanager

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

// A refresh applies what the node reports, so a status change is seen before
// the next sync.
func TestRefreshInfo_AppliesTheReportedStatusAndMetrics(t *testing.T) {
	t.Parallel()

	n := NewTestNode("test-node", api.NodeStatusReady, 0, 1,
		WithServiceInfoResponse(&orchestratorinfo.ServiceInfoResponse{
			ServiceStatus:          orchestratorinfo.ServiceInfoStatus_Standby,
			StatusOwner:            "x",
			MetricSandboxesRunning: 7,
		}))

	n.RefreshInfo(t.Context())

	assert.Equal(t, api.NodeStatusStandby, n.Status())
	assert.Equal(t, "x", n.StatusInfo().Owner)
	assert.Equal(t, uint32(7), n.Metrics().SandboxCount)
}

// A refresh leaves to the sync what only the sync can judge: it does not mark
// a node on a failed call, apply a restarted process's answer, or put back a
// node the sync marked.
func TestRefreshInfo_LeavesTheRestToTheSync(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		node func(t *testing.T) *TestNode
		want api.NodeStatus
	}{
		{
			name: "the call fails",
			node: func(*testing.T) *TestNode {
				return NewTestNode("test-node", api.NodeStatusReady, 0, 1, WithSilentInfoClient())
			},
			want: api.NodeStatusReady,
		},
		{
			name: "the process restarted",
			node: func(*testing.T) *TestNode {
				return NewTestNode("test-node", api.NodeStatusReady, 0, 1,
					WithServiceInstanceID("process-1"),
					WithServiceInfoResponse(&orchestratorinfo.ServiceInfoResponse{
						ServiceId:     "process-2",
						ServiceStatus: orchestratorinfo.ServiceInfoStatus_Standby,
					}))
			},
			want: api.NodeStatusReady,
		},
		{
			// The node answers Healthy, but its sandbox list fails every sync.
			name: "the sync marked the node unhealthy",
			node: func(t *testing.T) *TestNode {
				t.Helper()

				n := NewTestNode("test-node", api.NodeStatusReady, 0, 1, WithFailingSandboxList())
				require.NoError(t, n.Sync(t.Context(), nil))

				return n
			},
			want: api.NodeStatusUnhealthy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			n := tt.node(t)

			n.RefreshInfo(t.Context())

			assert.Equal(t, tt.want, n.status.Status)
			_, unreachable := n.UnreachableSince()
			assert.False(t, unreachable)
		})
	}
}

// placingInfoClient records a placement on the node before it answers, as a
// create that completes between a refresh's request and its answer does.
type placingInfoClient struct {
	orchestratorinfo.InfoServiceClient

	node *Node
}

func (c *placingInfoClient) ServiceInfo(context.Context, *emptypb.Empty, ...grpc.CallOption) (*orchestratorinfo.ServiceInfoResponse, error) {
	time.Sleep(time.Nanosecond)
	c.node.OptimisticAdd(SandboxResources{MiBMemory: 4, HugePages: true})

	return &orchestratorinfo.ServiceInfoResponse{MetricHugepageSizeBytes: 2 << 20}, nil
}

// The node's answer can predate a placement recorded after the refresh asked,
// so that placement's hugepages stay in the view. The clock is synctest's, so
// the placement is always later than the request.
func TestRefreshInfo_KeepsAPlacementRecordedAfterItAsked(t *testing.T) {
	t.Parallel()

	// Built outside the bubble: the test node's gRPC connection runs goroutines of its own.
	n := NewTestNode("test-node", api.NodeStatusReady, 0, 1, WithHugePages(100, 0, 0, 2<<20))
	n.SetInfoClient(&placingInfoClient{node: n})

	synctest.Test(t, func(t *testing.T) {
		n.RefreshInfo(t.Context())

		assert.Equal(t, uint64(2), n.Metrics().HugePagesReserved)
	})
}
