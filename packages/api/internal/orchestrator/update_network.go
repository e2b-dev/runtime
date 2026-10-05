package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	orchestratorgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	sandbox_network "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-network"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (o *Orchestrator) UpdateSandboxNetworkConfig(
	ctx context.Context,
	teamID uuid.UUID,
	sandboxID string,
	allowedEntries []string,
	deniedEntries []string,
	rules map[string][]types.SandboxNetworkRule,
	allowInternetAccess *bool,
	egressProxy *sandbox_network.EgressProxyConfig,
) *api.APIError {
	// PUT is full-replace: omitting egressProxy clears BYOP.
	egressConfig := &types.SandboxNetworkEgressConfig{
		AllowedAddresses: allowedEntries,
		DeniedAddresses:  deniedEntries,
		Rules:            rules,
	}
	ApplyValidatedEgressProxy(egressConfig, egressProxy)
	network := &types.SandboxNetworkConfig{Egress: egressConfig}
	orchNetwork := buildNetworkConfig(network, allowInternetAccess, nil)
	egress := orchNetwork.GetEgress()
	features := placement.EgressUpdateFeatures(egress)

	updateFunc := func(sbx sandbox.Sandbox) (sandbox.Sandbox, error) {
		if sbx.State != sandbox.StateRunning {
			return sbx, &sandbox.NotRunningError{SandboxID: sandboxID, State: sbx.State}
		}

		// Checked against the record being written, under its lock: a config
		// the node cannot apply would otherwise be stored, and read back, as
		// if it were in effect.
		if apiErr := o.checkNodeSupportsEgress(sbx, features); apiErr != nil {
			return sbx, apiErr
		}

		if sbx.Network == nil {
			sbx.Network = &types.SandboxNetworkConfig{}
		}

		sbx.Network.Egress = egressConfig

		if allowInternetAccess != nil {
			sbx.AllowInternetAccess = allowInternetAccess
		}

		return sbx, nil
	}

	var sbxNotRunningErr *sandbox.NotRunningError
	var apiErr *api.APIError

	sbx, err := o.sandboxStore.Update(ctx, teamID, sandboxID, updateFunc)
	if err != nil {
		switch {
		case errors.As(err, &apiErr):
			return apiErr
		case errors.As(err, &sbxNotRunningErr):
			return &api.APIError{Code: http.StatusConflict, ClientMsg: utils.SandboxChangingStateMsg(sandboxID, sbxNotRunningErr.State), Err: err}
		case errors.Is(err, sandbox.ErrNotFound):
			return &api.APIError{Code: http.StatusNotFound, ClientMsg: utils.SandboxNotFoundMsg(sandboxID), Err: err}
		default:
			return &api.APIError{Code: http.StatusInternalServerError, ClientMsg: "Error updating sandbox network config", Err: err}
		}
	}

	// Apply the network update on the orchestrator node.
	return o.updateSandboxNetworkOnNode(ctx, sbx, egress)
}

// checkNodeSupportsEgress refuses an egress config that needs a newer
// orchestrator than the node running the sandbox. It runs under the store
// lock, so it reads only the in-memory node cache.
func (o *Orchestrator) checkNodeSupportsEgress(sbx sandbox.Sandbox, features placement.FeatureRequirement) *api.APIError {
	if features.MinVersion() == "" {
		return nil
	}

	node := o.GetNode(sbx.ClusterID, sbx.NodeID)
	if node == nil {
		return &api.APIError{
			Code:      http.StatusServiceUnavailable,
			ClientMsg: fmt.Sprintf("Node hosting sandbox '%s' is not available, try again later", sbx.SandboxID),
			Err:       fmt.Errorf("node '%s' not in cache for cluster '%s'", sbx.NodeID, sbx.ClusterID),
		}
	}

	if !placement.NodeSatisfiesFeatures(node, features) {
		return &api.APIError{
			Code:      http.StatusServiceUnavailable,
			ErrorCode: errCodeFeatureUnsupported,
			ClientMsg: fmt.Sprintf("Sandbox '%s' runs on an orchestrator older than version %s, which this network config requires", sbx.SandboxID, features.MinVersion()),
			Err:       fmt.Errorf("node '%s' at version %q does not support %v", sbx.NodeID, node.Metadata().Version, features.FeatureNames()),
		}
	}

	return nil
}

func (o *Orchestrator) updateSandboxNetworkOnNode(
	ctx context.Context,
	sbx sandbox.Sandbox,
	egress *orchestratorgrpc.SandboxNetworkEgressConfig,
) *api.APIError {
	ctx, span := tracer.Start(ctx, "update-sandbox-network-on-node",
		trace.WithAttributes(
			attribute.String("instance.id", sbx.SandboxID),
		),
	)
	defer span.End()

	node := o.getOrConnectNode(ctx, sbx.ClusterID, sbx.NodeID)
	if node == nil {
		return &api.APIError{
			Code:      http.StatusInternalServerError,
			ClientMsg: fmt.Sprintf("Node hosting sandbox '%s' not found", sbx.SandboxID),
			Err:       fmt.Errorf("node '%s' not found for cluster '%s'", sbx.NodeID, sbx.ClusterID),
		}
	}

	client, ctx := node.GetClient(ctx)
	_, err := client.Sandbox.Update(ctx, &orchestratorgrpc.SandboxUpdateRequest{
		SandboxId: sbx.SandboxID,
		Egress:    egress,
	})
	if err != nil {
		grpcErr, ok := status.FromError(err)
		if ok && grpcErr.Code() == codes.NotFound {
			return &api.APIError{Code: http.StatusNotFound, ClientMsg: utils.SandboxNotFoundMsg(sbx.SandboxID), Err: err}
		}

		err = utils.UnwrapGRPCError(err)
		telemetry.ReportCriticalError(ctx, "failed to update sandbox network on node", err)

		return &api.APIError{
			Code:      http.StatusInternalServerError,
			ClientMsg: "Error applying network config to sandbox",
			Err:       fmt.Errorf("failed to update sandbox network on node: %w", err),
		}
	}

	telemetry.ReportEvent(ctx, "Updated sandbox network on node")

	return nil
}
