package handlers

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func toVolumeKey(volume queries.Volume) *orchestrator.VolumeInfo {
	return &orchestrator.VolumeInfo{
		VolumeId:   volume.ID.String(),
		VolumeType: volume.VolumeType,
		TeamId:     volume.TeamID.String(),
	}
}

func (a *APIStore) getVolume(c *gin.Context, volumeID string) (queries.Volume, *types.Team, bool) {
	ctx := c.Request.Context()

	team, apiErr := a.GetTeam(ctx, c, nil)
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportCriticalError(ctx, "error when getting team", apiErr.Err)

		return queries.Volume{}, team, false
	}

	telemetry.SetAttributes(ctx,
		telemetry.WithTeamID(team.ID.String()),
	)

	volumeIDuuid, err := uuid.Parse(volumeID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid volume ID")
		telemetry.ReportCriticalError(ctx, "error when parsing volume ID", err)

		return queries.Volume{}, team, false
	}

	volume, err := a.sqlcDB.GetVolume(ctx, queries.GetVolumeParams{
		VolumeID: volumeIDuuid,
		TeamID:   team.ID,
	})

	switch {
	case dberrors.IsNotFoundError(err):
		a.sendAPIStoreError(c, http.StatusNotFound, "Volume not found")
		telemetry.ReportError(ctx, "volume not found", err)

		return volume, team, false
	case err != nil:
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when getting volume")
		telemetry.ReportCriticalError(ctx, "error when getting volume", err)

		return volume, team, false
	default:
		return volume, team, true
	}
}

var ErrClusterNotFound = errors.New("cluster not found")

// volumeContentDomain resolves the domain the SDK should use as the destination
// for volume content requests. Teams connected to a custom (BYOC) cluster get
// that cluster's domain, which the SDK uses in place of the default
// api.<E2B_DOMAIN> host. Teams on the default cluster get nil, leaving the SDK
// on its configured default.
func (a *APIStore) volumeContentDomain(team *types.Team) (*string, error) {
	if team.ClusterID == nil {
		return nil, nil
	}

	cluster, ok := a.clusters.GetClusterById(*team.ClusterID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, team.ClusterID.String())
	}

	return cluster.SandboxDomain, nil
}

// volumeTokenAudience builds the audience claim for a volume content token: the
// origin the SDK targets for content requests, i.e. `https://api.<domain>`. The
// domain is the team's BYOC cluster domain when set (as resolved by
// volumeContentDomain) and the deployment default otherwise, matching the host
// the token is actually presented to.
func (a *APIStore) volumeTokenAudience(domain *string) string {
	host := a.config.DomainName
	if domain != nil && *domain != "" {
		host = *domain
	}

	return fmt.Sprintf("https://api.%s", host)
}

var ErrNoHealthyOrchestratorFound = errors.New("no healthy orchestrator found")

var ErrUnknownVolumeType = errors.New("unknown volume type")

func (a *APIStore) executeOnOrchestratorByClusterID(
	ctx context.Context,
	clusterID uuid.UUID,
	volume queries.Volume,
	fn func(context.Context, *clusters.GRPCClient) error,
) error {
	nodes := a.orchestrator.GetClusterNodes(clusterID)
	if len(nodes) == 0 {
		return ErrClusterNotFound
	}

	// Try nodes with a matching volume label first, then fall back to the rest
	// of the cluster. A node that fails with a retryable error simply moves on
	// to the next one, so we only reach the unlabeled nodes once every labeled
	// node has failed with a retryable error.
	volumeLabel := internal.MakeVolumeTypeLabel(volume.VolumeType)
	labeledNodes, otherNodes := findNodesByVolumeLabel(nodes, volumeLabel)
	rand.Shuffle(len(labeledNodes), func(i, j int) { labeledNodes[i], labeledNodes[j] = labeledNodes[j], labeledNodes[i] })
	rand.Shuffle(len(otherNodes), func(i, j int) { otherNodes[i], otherNodes[j] = otherNodes[j], otherNodes[i] })

	// Falling back to unlabeled nodes is only useful during the volume-label
	// migration; once every node is labeled, unlabeled nodes fail 100% of the
	// time. The flag lets us turn the fallback off (and eventually remove it).
	fallbackToUnmatched := a.featureFlags.BoolFlag(ctx,
		featureflags.VolumeFallbackToUnmatchedNodesFlag,
		featureflags.TeamContext(volume.TeamID.String()),
		featureflags.ClusterContext(clusterID),
		featureflags.VolumeContext(volume.ID.String()),
	)

	nodes = labeledNodes
	if fallbackToUnmatched {
		nodes = append(nodes, otherNodes...)
	}

	var (
		receivedUnknownVolumeTypeErrors int
		unknownVolumeType               string
		skippedNodeCount                int
	)
	defer func() {
		if receivedUnknownVolumeTypeErrors != 0 {
			logger.L().Warn(ctx, "received unknown volume type errors",
				zap.String("volume_type", unknownVolumeType),
				zap.Int("total_nodes", len(nodes)),
				zap.Int("unknown_type_errors", receivedUnknownVolumeTypeErrors),
			)
		}
	}()

	for _, node := range nodes {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context error: %w", err)
		}

		// Only nodes that can take new requests serve volume content
		if !node.CanAcceptNewRequests() {
			skippedNodeCount++

			continue
		}

		c, clientCtx := node.GetClient(ctx)

		err := fn(clientCtx, c)
		if err == nil { // inverted guard clause, the rest of the function is chunky
			return nil
		}

		// the rest is all error handling

		if ctxErr := ctx.Err(); ctxErr != nil {
			// original context is canceled, bail
			return errors.Join(
				fmt.Errorf("orchestrator error: %w", err),
				fmt.Errorf("request error: %w", ctxErr),
			)
		}

		// we want to retry these, but we don't want to flood the logs with reports
		if volumeType, ok := isUnknownVolumeTypeError(err); ok {
			unknownVolumeType = volumeType
			receivedUnknownVolumeTypeErrors++

			continue
		}

		if isRetryableError(err) {
			logger.L().Warn(clientCtx, "failed to make orchestrator call, retrying ... ", zap.Error(err))

			continue
		}

		return err
	}

	if receivedUnknownVolumeTypeErrors == len(nodes)-skippedNodeCount && receivedUnknownVolumeTypeErrors > 0 {
		return fmt.Errorf("%w: %s", ErrUnknownVolumeType, unknownVolumeType)
	}

	return ErrNoHealthyOrchestratorFound
}

// findNodesByVolumeLabel splits nodes into those that advertise the
// volume's type label and those that don't, preserving the input order within
// each group.
func findNodesByVolumeLabel(nodes []*nodemanager.Node, expectedLabel string) (matched, unmatched []*nodemanager.Node) {
	for _, node := range nodes {
		labels := node.Labels()
		if _, ok := labels[expectedLabel]; ok {
			matched = append(matched, node)

			continue
		}

		unmatched = append(unmatched, node)
	}

	return matched, unmatched
}

func isUnknownVolumeTypeError(err error) (string, bool) {
	grpcStatus, ok := status.FromError(err)
	if ok {
		for _, actual := range grpcStatus.Details() {
			if vterr, ok := actual.(*orchestrator.UnknownVolumeTypeError); ok {
				return vterr.GetVolumeType(), true // maybe there's another orchestrator that knows about it?
			}
		}
	}

	return "", false
}

func isRetryableError(err error) bool {
	if errors.Is(err, net.ErrClosed) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}

	st, ok := status.FromError(err)
	if ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
			return true
		}
	}

	return false
}
