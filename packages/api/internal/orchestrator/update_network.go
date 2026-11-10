package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	orchestratorgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
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
	dnsServers *[]string,
	egressProxy *sandbox_network.EgressProxyConfig,
) *api.APIError {
	// PUT is full-replace: omitting egressProxy clears BYOP.
	egressConfig := &types.SandboxNetworkEgressConfig{
		AllowedAddresses: allowedEntries,
		DeniedAddresses:  deniedEntries,
		Rules:            rules,
		DnsServers:       dnsServers,
	}
	ApplyValidatedEgressProxy(egressConfig, egressProxy)
	network := &types.SandboxNetworkConfig{Egress: egressConfig}
	orchNetwork := buildNetworkConfig(network, allowInternetAccess, nil)
	egress := orchNetwork.GetEgress()
	features := placement.EgressUpdateFeatures(egress)

	// The node is found, and connected if the cache misses it, before the
	// store lock is taken: discovery can take its own budget, which the lock
	// must not wait for.
	current, err := o.sandboxStore.Get(ctx, teamID, sandboxID)
	if err != nil {
		if errors.Is(err, sandbox.ErrNotFound) {
			return &api.APIError{Code: http.StatusNotFound, ClientMsg: utils.SandboxNotFoundMsg(sandboxID), Err: err}
		}

		return &api.APIError{Code: http.StatusInternalServerError, ClientMsg: "Error updating sandbox network config", Err: err}
	}
	// The state refusal is answered from the record as read, before any
	// discovery, and the version refusal after it, so a node the cache has not
	// seen yet is discovered rather than refused. The locked update repeats
	// both against the record it writes.
	if current.State != sandbox.StateRunning {
		return &api.APIError{Code: http.StatusConflict, ClientMsg: utils.SandboxChangingStateMsg(sandboxID, current.State), Err: &sandbox.NotRunningError{SandboxID: sandboxID, State: current.State}}
	}
	discovered := o.getOrConnectNode(ctx, current.ClusterID, current.NodeID)
	if discovered == nil {
		return &api.APIError{
			Code:      http.StatusInternalServerError,
			ClientMsg: fmt.Sprintf("Node hosting sandbox '%s' not found", sandboxID),
			Err:       fmt.Errorf("node '%s' not found for cluster '%s'", current.NodeID, current.ClusterID),
		}
	}
	if apiErr := nodeSupportsEgress(discovered, current, features); apiErr != nil {
		return apiErr
	}

	applied := false
	updateFunc := func(sbx sandbox.Sandbox) (sandbox.Sandbox, error) {
		applied = false
		if sbx.State != sandbox.StateRunning {
			return sbx, &sandbox.NotRunningError{SandboxID: sandboxID, State: sbx.State}
		}

		// Checked against the record being written, under its lock: a config
		// the node cannot apply would otherwise be stored, and read back, as
		// if it were in effect.
		if sbx.NetworkPlacement != nil && egress.GetEgressProxyAddress() != "" {
			return sbx, &api.APIError{
				Code:      http.StatusBadRequest,
				ClientMsg: "Egress proxy is not supported on this sandbox's network",
				Err:       errors.New("egress proxy is not supported on a placed network"),
			}
		}
		// The node is read from the cache again under the lock, so the
		// capability check and the call use the node registered now, not one
		// replaced while the lock was awaited. Discovery stays outside the lock.
		node := o.GetNode(sbx.ClusterID, sbx.NodeID)
		if node == nil {
			return sbx, &api.APIError{
				Code:      http.StatusServiceUnavailable,
				ClientMsg: fmt.Sprintf("Node hosting sandbox '%s' is not available, try again later", sbx.SandboxID),
				Err:       fmt.Errorf("node '%s' not in cache for cluster '%s'", sbx.NodeID, sbx.ClusterID),
			}
		}
		if apiErr := nodeSupportsEgress(node, sbx, features); apiErr != nil {
			return sbx, apiErr
		}

		// The node applies the config before it is stored, under the same
		// lock: a config the node refuses is never stored, and concurrent
		// updates reach the node in the order they are stored.
		nodeCtx, cancel := context.WithTimeout(ctx, networkUpdateNodeTimeout)
		defer cancel()
		if apiErr := o.updateSandboxNetworkOnNode(nodeCtx, node, sbx, egress); apiErr != nil {
			return sbx, apiErr
		}
		applied = true

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

	_, err = o.sandboxStore.Update(ctx, teamID, sandboxID, updateFunc)
	if err != nil && applied {
		// The node runs the new config and the store keeps the old one, after
		// the store's own write retries. The next update replaces both, since
		// a PUT carries the whole config, and the sandbox's end removes the
		// record; until then the mismatch is logged and counted.
		logger.L().Error(ctx, "network update applied on the node but not stored", zap.Error(err), logger.WithSandboxID(sandboxID))
		if o.networkUpdateUnstoredCounter != nil {
			o.networkUpdateUnstoredCounter.Add(ctx, 1)
		}

		return &api.APIError{
			Code:      http.StatusInternalServerError,
			ClientMsg: "The network config was applied but could not be saved; send the update again",
			Err:       fmt.Errorf("network update applied on the node but not stored: %w", err),
		}
	}
	if err != nil {
		switch {
		case errors.As(err, &apiErr):
			return apiErr
		case errors.As(err, &sbxNotRunningErr):
			return &api.APIError{Code: http.StatusConflict, ClientMsg: utils.SandboxChangingStateMsg(sandboxID, sbxNotRunningErr.State), Err: err}
		case errors.Is(err, sandbox.ErrLifecycleHeld):
			return &api.APIError{Code: http.StatusConflict, ClientMsg: utils.SandboxLifecycleHeldMsg(sandboxID), Err: err}
		case errors.Is(err, sandbox.ErrNotFound):
			return &api.APIError{Code: http.StatusNotFound, ClientMsg: utils.SandboxNotFoundMsg(sandboxID), Err: err}
		default:
			return &api.APIError{Code: http.StatusInternalServerError, ClientMsg: "Error updating sandbox network config", Err: err}
		}
	}

	return nil
}

// networkUpdateNodeTimeout bounds the node call made under the sandbox's store
// lock, which delays a concurrent update, pause or kill of the sandbox. A node
// applies an update in well under a second; the lock expires after a minute.
const networkUpdateNodeTimeout = 10 * time.Second

// nodeSupportsEgress refuses an egress config that needs a newer orchestrator
// than node.
func nodeSupportsEgress(node *nodemanager.Node, sbx sandbox.Sandbox, features placement.FeatureRequirement) *api.APIError {
	if features.MinVersion() == "" {
		return nil
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
	node *nodemanager.Node,
	sbx sandbox.Sandbox,
	egress *orchestratorgrpc.SandboxNetworkEgressConfig,
) *api.APIError {
	ctx, span := tracer.Start(ctx, "update-sandbox-network-on-node",
		trace.WithAttributes(
			attribute.String("instance.id", sbx.SandboxID),
		),
	)
	defer span.End()

	client, ctx := node.GetSandboxClient(ctx, sbx.TeamID.String())
	_, err := client.Sandbox.Update(ctx, &orchestratorgrpc.SandboxUpdateRequest{
		SandboxId: sbx.SandboxID,
		Egress:    egress,
	})
	if err != nil {
		grpcErr, ok := status.FromError(err)
		if ok && grpcErr.Code() == codes.NotFound {
			return &api.APIError{Code: http.StatusNotFound, ClientMsg: utils.SandboxNotFoundMsg(sbx.SandboxID), Err: err}
		}
		// The node refused the config before applying it.
		if ok && grpcErr.Code() == codes.InvalidArgument {
			return &api.APIError{Code: http.StatusBadRequest, ClientMsg: grpcErr.Message(), Err: err}
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
