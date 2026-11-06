package orchestrator

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
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

// AdminNodeSandboxes lists the sandboxes the node runs that the store
// records as running on it with the same execution: a guest the node runs
// for an execution recorded elsewhere, such as the source's copy during a
// move, or not recorded at all, is left out, as is one the node reports
// ended. A node or store read that fails fails the list, so a worker outage
// never reads as an empty node.
func (o *Orchestrator) AdminNodeSandboxes(ctx context.Context, clusterID uuid.UUID, nodeID string) (*api.NodeSandboxes, error) {
	node := o.GetNode(clusterID, nodeID)
	if node == nil {
		return nil, ErrNodeNotFound
	}
	observed, err := node.GetOrphanCandidates(ctx)
	if err != nil {
		return nil, fmt.Errorf("list the node's sandboxes: %w", err)
	}
	now := time.Now()
	items := make([]api.NodeSandbox, 0, len(observed))
	for _, onNode := range observed {
		if onNode.Ended {
			continue
		}
		sbx, err := o.sandboxStore.Get(ctx, onNode.TeamID, onNode.SandboxID)
		if errors.Is(err, sandbox.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read sandbox %q: %w", onNode.SandboxID, err)
		}
		if sbx.ClusterID != clusterID || sbx.State != sandbox.StateRunning || sbx.NodeID != node.ID || sbx.ExecutionID != onNode.ExecutionID {
			continue
		}
		executionID, err := uuid.Parse(sbx.ExecutionID)
		if err != nil {
			return nil, fmt.Errorf("sandbox %q execution ID: %w", sbx.SandboxID, err)
		}
		item := api.NodeSandbox{TeamID: sbx.TeamID, SandboxID: sbx.SandboxID, ExecutionID: executionID}
		if sbx.Held(now) {
			until := time.UnixMilli(sbx.LifecycleHoldUntil)
			item.LifecycleHoldUntil = &until
		}
		items = append(items, item)
	}

	return &api.NodeSandboxes{NodeID: node.ID, ObservedAt: now, Items: items}, nil
}
