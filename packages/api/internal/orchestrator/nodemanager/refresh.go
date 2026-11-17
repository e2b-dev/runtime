package nodemanager

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
)

// RefreshInfo applies the status and metrics the node reports, between syncs.
// It leaves to the next sync a node the sync marked Unhealthy and a process
// that restarted, whose labels and version only a sync reads. A failed call
// changes nothing.
func (n *Node) RefreshInfo(ctx context.Context) {
	if n.Status() == api.NodeStatusUnhealthy {
		return
	}

	client, ctx := n.GetClient(ctx)
	requestedAt := time.Now()
	info, err := client.Info.ServiceInfo(ctx, &emptypb.Empty{})
	if err != nil || info.GetServiceId() != n.Metadata().ServiceInstanceID {
		return
	}

	reported := reportedStatus(ctx, n.ID, info)
	n.setStatus(ctx, reported.Status, reported.Owner, reported.ChangedAt)
	n.UpdateMetricsFromSync(info, requestedAt)
}
