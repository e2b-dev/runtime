package orchestrator

import (
	"cmp"
	"context"
	"slices"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/api"
)

func (o *Orchestrator) TeamRunningSandboxCounts(ctx context.Context) (map[uuid.UUID]int64, error) {
	return o.sandboxStore.TeamsWithSandboxes(ctx)
}

func (o *Orchestrator) AdminNodes(clusterID uuid.UUID) ([]*api.Node, error) {
	var result []*api.Node

	for _, n := range o.nodes.Items() {
		if n.ClusterID != clusterID {
			continue
		}

		meta := n.Metadata()
		metrics := n.GetAPIMetric()
		nodeMetrics := n.Metrics()
		machineInfo := n.MachineInfo()
		statusInfo := n.StatusInfo()
		result = append(result, &api.Node{
			Id:                n.ID,
			ServiceInstanceID: meta.ServiceInstanceID,
			ClusterID:         n.ClusterID.String(),
			MachineInfo: api.MachineInfo{
				CpuArchitecture: machineInfo.CPUArchitecture,
				CpuFamily:       machineInfo.CPUFamily,
				CpuModel:        machineInfo.CPUModel,
				CpuModelName:    machineInfo.CPUModelName,
			},
			Status:               statusInfo.Status,
			StatusChangedAt:      statusInfo.ChangedAt,
			CreateSuccesses:      n.PlacementMetrics.SuccessCount(),
			CreateFails:          n.PlacementMetrics.FailsCount(),
			SandboxStartingCount: int(n.PlacementMetrics.InProgressCount()),
			SandboxCount:         nodeMetrics.SandboxCount,
			MaxSandboxes:         nodeMetrics.MaxSandboxes,
			OutstandingWork:      nodeMetrics.OutstandingWork,
			Version:              meta.Version,
			Commit:               meta.Commit,
			Metrics:              metrics,
		})
	}

	slices.SortFunc(result, func(i, j *api.Node) int {
		return cmp.Compare(i.Id, j.Id)
	})

	return result, nil
}

func (o *Orchestrator) AdminNodeDetail(clusterID uuid.UUID, nodeID string) (*api.NodeDetail, error) {
	n := o.GetNode(clusterID, nodeID)
	if n == nil {
		return nil, ErrNodeNotFound
	}

	meta := n.Metadata()
	metrics := n.GetAPIMetric()
	nodeMetrics := n.Metrics()
	machineInfo := n.MachineInfo()
	statusInfo := n.StatusInfo()

	node := &api.NodeDetail{
		Id:                n.ID,
		ClusterID:         n.ClusterID.String(),
		ServiceInstanceID: meta.ServiceInstanceID,
		MachineInfo: api.MachineInfo{
			CpuArchitecture: machineInfo.CPUArchitecture,
			CpuFamily:       machineInfo.CPUFamily,
			CpuModel:        machineInfo.CPUModel,
			CpuModelName:    machineInfo.CPUModelName,
		},
		Status:          statusInfo.Status,
		StatusChangedAt: statusInfo.ChangedAt,
		CreateSuccesses: n.PlacementMetrics.SuccessCount(),
		CreateFails:     n.PlacementMetrics.FailsCount(),
		SandboxCount:    nodeMetrics.SandboxCount,
		MaxSandboxes:    nodeMetrics.MaxSandboxes,
		OutstandingWork: nodeMetrics.OutstandingWork,
		Version:         meta.Version,
		Commit:          meta.Commit,
		Metrics:         metrics,
	}

	return node, nil
}
