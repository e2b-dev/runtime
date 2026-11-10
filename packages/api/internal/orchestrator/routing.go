package orchestrator

import (
	"errors"
	"fmt"
	"math"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/env"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	catalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
)

const localSandboxIPAddress = "127.0.0.1"

func routeNodeIPAddress(node *nodemanager.Node, local bool) string {
	if node.IPAddress != "" {
		return node.IPAddress
	}

	if local && node.ClusterID == consts.LocalClusterID {
		return localSandboxIPAddress
	}

	return ""
}

// StartedSandboxResumeResponse answers the auto-resume RPC for a sandbox this
// request started or joined. It resolves the orchestrator address from the
// node cache only.
func (o *Orchestrator) StartedSandboxResumeResponse(sbx sandbox.Sandbox) (*proxygrpc.SandboxResumeResponse, error) {
	return resumeResponse(sbx, func() (string, error) {
		nodeIP := o.GetNodeRouteIPAddress(sbx.ClusterID, sbx.NodeID)
		if nodeIP == "" {
			return "", errors.New("sandbox resumed but orchestrator IP is not available yet")
		}

		return nodeIP, nil
	})
}

// resumeResponse answers the auto-resume RPC with the sandbox's saved
// placement. nodeAddress resolves the orchestrator address and is called
// only for modes whose ingress ends on the orchestrator, so a full-router
// sandbox never depends on an orchestrator being reachable.
func resumeResponse(sbx sandbox.Sandbox, nodeAddress func() (string, error)) (*proxygrpc.SandboxResumeResponse, error) {
	placement := sbx.NetworkPlacement
	if placement == nil {
		// A sandbox started without a placement uses the legacy per-worker
		// network, and is answered as it always was: an absent placement
		// means legacy to every reader, as it does in the create response
		// and the sandbox record.
		nodeIP, err := nodeAddress()
		if err != nil {
			return nil, err
		}

		return &proxygrpc.SandboxResumeResponse{OrchestratorIp: nodeIP}, nil
	}

	// A placement left from an earlier execution would send traffic to an
	// ingress that no longer serves this sandbox.
	if placement.GetExecutionId() != sbx.ExecutionID {
		return nil, fmt.Errorf("sandbox network placement belongs to execution %q, not %q", placement.GetExecutionId(), sbx.ExecutionID)
	}

	switch catalog.NetworkMode(placement.GetNetworkMode()) {
	case catalog.NetworkModeFullRouter:
		if placement.GetRouterId() == "" || placement.GetIngressAddress() == "" || placement.GetIngressPort() == 0 || placement.GetIngressPort() > math.MaxUint16 {
			return nil, errors.New("full-router sandbox network placement has no usable router ingress")
		}

		return &proxygrpc.SandboxResumeResponse{NetworkPlacement: placement}, nil
	case catalog.NetworkModeEgressRouter:
		// Ingress still ends on the orchestrator, which older callers reach
		// through orchestrator_ip.
		nodeIP, err := nodeAddress()
		if err != nil {
			return nil, err
		}

		return &proxygrpc.SandboxResumeResponse{OrchestratorIp: nodeIP, NetworkPlacement: placement}, nil
	default:
		return nil, fmt.Errorf("sandbox network placement has unsupported mode %q", placement.GetNetworkMode())
	}
}

func (o *Orchestrator) GetNodeRouteIPAddress(clusterID uuid.UUID, nodeID string) string {
	node := o.GetNode(clusterID, nodeID)
	if node == nil {
		return ""
	}

	return routeNodeIPAddress(node, env.IsLocal())
}
